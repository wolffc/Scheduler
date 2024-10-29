<?php

namespace Ttree\Scheduler\Service;

/*                                                                        *
 * This script belongs to the Neos Flow package "Ttree.Scheduler".       *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        */

use Assert\Assertion;
use DateTimeInterface;
use Ttree\Scheduler\Annotations\Schedule;
use Ttree\Scheduler\Domain\Model\Task;
use Ttree\Scheduler\Domain\Repository\TaskRepository;
use Ttree\Scheduler\Task\TaskDescriptor;
use Ttree\Scheduler\Task\TaskInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Cache\Frontend\VariableFrontend;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Flow\Reflection\ReflectionService;
use Neos\Flow\Utility\Now;
use Ttree\Scheduler\Annotations;
use Ttree\Scheduler\Task\TaskTypeEnum;

class TaskService
{
    const TASK_INTERFACE = 'Ttree\Scheduler\Task\TaskInterface';

    /**
     * @Flow\Inject
     */
    protected VariableFrontend $dynamicTaskLastExecutionCache;

    /**
     * @Flow\Inject
     */
    protected TaskRepository $taskRepository;

    /**
     * @Flow\Inject
     */
    protected PersistenceManagerInterface $persistenceManager;

    /**
     * @Flow\Inject
     */
    protected ReflectionService $reflexionService;

    /**
     * @Flow\Inject
     */
    protected ObjectManagerInterface $objectManager;

    /**
     * @param ObjectManagerInterface $objectManager
     * @return list<class-string>
     * @Flow\CompileStatic
     */
    public static function getAllTaskImplementations(ObjectManagerInterface $objectManager): array
    {
        $reflectionService = $objectManager->get(ReflectionService::class);
        return $reflectionService->getAllImplementationClassNamesForInterface(self::TASK_INTERFACE);
    }

    /**
     * @param ObjectManagerInterface $objectManager
     * @return array<string,array{implementation:string, expression:string, description:string}>
     * @Flow\CompileStatic
     */
    public static function getAllDynamicTaskImplementations(ObjectManagerInterface $objectManager): array
    {
        $tasks = [];
        $reflectionService = $objectManager->get(ReflectionService::class);

        foreach (self::getAllTaskImplementations($objectManager) as $className) {
            if (!$reflectionService->isClassAnnotatedWith($className, Annotations\Schedule::class)) {
                continue;
            }
            /** @var Schedule $scheduleAnnotation */
            $scheduleAnnotation = $reflectionService->getClassAnnotation($className, Annotations\Schedule::class);
            $tasks[$className] = [
                'implementation' => $className,
                'expression' => $scheduleAnnotation->expression,
                'description' => ''
            ];

            if ($reflectionService->isClassAnnotatedWith($className, Annotations\Meta::class)) {
                /** @var Annotations\Meta $metaAnnotation */
                $metaAnnotation = $reflectionService->getClassAnnotation($className, Annotations\Meta::class);
                $tasks[$className]['description'] = $metaAnnotation->description;
            }
        }

        return $tasks;
    }

    /**
     * @return array<TaskDescriptor>
     */
    public function getDueTasks(): array
    {
        $tasks = array_merge($this->getDuePersistedTasks(), $this->getDynamicTasks(true));

        return $this->sortTaskList($tasks);
    }

    /**
     * @return array<TaskDescriptor>
     */
    public function getDuePersistedTasks(): array
    {
        $tasks = [];
        foreach ($this->taskRepository->findDueTasks() as $task) {
            $tasks[] = TaskDescriptor::fromPersistedTask(
                $this->persistenceManager->getIdentifierByObject($task),
                $task
            );
        }
        return $tasks;
    }

    /**
     * @return array<TaskDescriptor>
     */
    public function getTasks(): array
    {
        $tasks = array_merge($this->getPersistedTasks(), $this->getDynamicTasks());

        return $this->sortTaskList($tasks);
    }

    /**
     * @return array<TaskDescriptor>
     */
    public function getPersistedTasks(): array
    {
        $tasks = [];
        foreach ($this->taskRepository->findAll() as $task) {
            assert($task instanceof Task);
            $identifier = $this->persistenceManager->getIdentifierByObject($task);
            $tasks[$identifier] = TaskDescriptor::fromPersistedTask(
                $identifier,
                $task
            );
        }
        return $tasks;
    }

    /**
     * @return array<TaskDescriptor>
     */
    public function getDynamicTasks(bool $dueOnly = false): array
    {
        $tasks = [];
        $now = new Now();

        foreach (self::getAllDynamicTaskImplementations($this->objectManager) as $dynamicTask) {
            $task = new Task($dynamicTask['expression'], $dynamicTask['implementation'], [], $dynamicTask['description']);
            $cacheKey = md5($dynamicTask['implementation']);
            $lastExecution = $this->dynamicTaskLastExecutionCache->get($cacheKey);
            if (!$lastExecution instanceof \DateTime) {
                $lastExecution = null;
            }
            if ($dueOnly && ($lastExecution instanceof \DateTime && $now < $task->getNextExecution($lastExecution))) {
                continue;
            }
            $task->enable();
            $taskDescriptor = TaskDescriptor::fromDynamicTask($task, $lastExecution);
            $tasks[$taskDescriptor->identifier] = $taskDescriptor;
        }
        return $tasks;
    }


    /**
     * @param array<mixed> $arguments
     */
    public function create(string $expression, string $task, array $arguments, string $description): Task
    {
        $task = new Task($expression, $task, $arguments, $description);
        $this->assertValidTask($task);
        $this->taskRepository->add($task);
        return $task;
    }

    public function remove(Task $task): void
    {
        $this->taskRepository->remove($task);
    }

    public function update(Task $task, TaskTypeEnum $type): void
    {
        switch ($type) {
            case TaskTypeEnum::TYPE_DYNAMIC:
                $cacheKey = md5($task->getImplementation());
                $this->dynamicTaskLastExecutionCache->set($cacheKey, $task->getLastExecution());
                break;
            case TaskTypeEnum::TYPE_PERSISTED:
                $this->taskRepository->update($task);
                break;
        }
    }

    /**
     * @param array<TaskDescriptor> $tasks
     * @return array<TaskDescriptor>
     */
    protected function sortTaskList(array $tasks): array
    {
        uasort(
            $tasks,
            function (TaskDescriptor $a, TaskDescriptor $b) {
                return ($a->task->getNextExecution() < $b->task->getNextExecution()) ? -1 : 1;
            }
        );
        return $tasks;
    }

    protected function assertValidTask(Task $task): void
    {
        if (!class_exists($task->getImplementation())) {
            throw new \InvalidArgumentException(
                sprintf('Task implementation "%s" must exist', $task->getImplementation()),
                1419296545
            );
        }
        if (!$this->reflexionService->isClassImplementationOf($task->getImplementation(), self::TASK_INTERFACE)) {
            throw new \InvalidArgumentException('Task must implement TaskInterface', 1419296485);
        }
    }
}
