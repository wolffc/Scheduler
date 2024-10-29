<?php

namespace Ttree\Scheduler\Command;

/*                                                                        *
 * This script belongs to the Neos Flow package "Ttree.Scheduler".       *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        */

use Assert\Assertion;
use DateTimeInterface;
use Neos\Flow\Persistence\PersistenceManagerInterface;
use Neos\Flow\Utility\Environment;
use Neos\Utility\Files;
use Neos\Utility\Lock\LockManager;
use Ttree\Scheduler\Domain\Model\Task;
use Ttree\Scheduler\Service\TaskService;
use Ttree\Scheduler\Task\TaskInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Utility\Lock\Lock;
use Neos\Utility\Lock\LockNotAcquiredException;
use Ttree\Scheduler\Task\TaskTypeEnum;

/**
 * Task Command Controller
 */
class TaskCommandController extends CommandController
{
    /**
     * @Flow\Inject
     */
    protected TaskService $taskService;

    /**
     * @Flow\Inject(lazy=false)
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * @Flow\Inject
     */
    protected PersistenceManagerInterface $persistenceManager;

    /**
     * @Flow\Inject
     */
    protected Environment $environment;

    /**
     * @Flow\InjectConfiguration(package="Ttree.Scheduler", path="allowParallelExecution")
     */
    protected bool $allowParallelExecution = true;

    /**
     * @Flow\InjectConfiguration(package="Ttree.Scheduler", path="lockStrategyClassName")
     */
    protected string $lockStrategyClassName = '';

    /**
     * @var Lock
     */
    protected $parallelExecutionLock;

    /**
     * @throws \Neos\Flow\Utility\Exception
     * @throws \Neos\Utility\Exception\FilesException
     */
    public function initializeObject(): void
    {
        $lockManager = new LockManager($this->lockStrategyClassName, [
            'lockDirectory' => Files::concatenatePaths([
                $this->environment->getPathToTemporaryDirectory(),
                'Lock'
            ])
        ]);
        Lock::setLockManager($lockManager);
    }

    /**
     * Run all pending task
     *
     * @param boolean $dryRun do not execute tasks
     */
    public function runCommand(bool $dryRun = false): void
    {
        if ($this->allowParallelExecution !== true) {
            try {
                $this->parallelExecutionLock = new Lock('Ttree.Scheduler.ParallelExecutionLock');
            } catch (LockNotAcquiredException $exception) {
                $this->tellStatus('The scheduler is already running and parallel execution is disabled.');
                $this->sendAndExit(0);
            }
        }

        foreach ($this->taskService->getDueTasks() as $taskDescriptor) {
            $task = $taskDescriptor->task;
            $arguments = [$task->getImplementation(), $taskDescriptor->identifier];

            $this->markTaskAsRun($task, $taskDescriptor->type);
            try {
                if (!$dryRun) {
                    $task->execute($this->objectManager);
                    $this->tellStatus('[Success] Run "%s" (%s)', $arguments);
                } else {
                    $this->tellStatus('[Skipped, dry run] Skipped "%s" (%s)', $arguments);
                }
            } catch (\Exception $exception) {
                $this->tellStatus('[Error] Task "%s" (%s) throw an exception, check your log', $arguments);
            }
        }

        if ($this->parallelExecutionLock instanceof Lock) {
            $this->parallelExecutionLock->release();
        }
    }

    /**
     * List all tasks
     */
    public function listCommand(): void
    {
        $tasks = [];
        foreach ($this->taskService->getTasks() as $taskDescriptor) {
            $tasks[] = [
                $taskDescriptor->type->value,
                $taskDescriptor->getEnabledLabel(),
                $taskDescriptor->identifier,
                $taskDescriptor->task->getImplementation(),
                $taskDescriptor->task->getNextExecution()->format(DateTimeInterface::ATOM),
                $taskDescriptor->task->getLastExecution()?->format(DateTimeInterface::ATOM),
                $taskDescriptor->task->getDescription()
            ];
        }
        if (count($tasks)) {
            $this->output->outputTable($tasks, [
                'Type',
                'Status',
                'Identifier',
                'Interval',
                'Implementation',
                'Next Execution Date',
                'Last Execution Date',
                'Description'
            ]);
        } else {
            $this->outputLine('Empty task list ...');
        }
    }

    /**
     * Run a single persisted task ignoring status and schedule.
     *
     * @param string $taskIdentifier
     */
    public function runSingleCommand(string $taskIdentifier): void
    {
        $taskDescriptors = $this->taskService->getTasks();
        Assertion::keyExists(
            $taskDescriptors,
            $taskIdentifier,
            sprintf('Task with identifier %s does not exist.', $taskIdentifier)
        );

        $taskDescriptor = $taskDescriptors[$taskIdentifier];

        $arguments = [$taskDescriptor->task->getImplementation(), $taskDescriptor->identifier];

        $this->markTaskAsRun($taskDescriptor->task, $taskDescriptor->type);
        try {
            $taskDescriptor->task->execute($this->objectManager);
            $this->tellStatus('[Success] Run "%s" (%s)', $arguments);
        } catch (\Exception $exception) {
            $this->tellStatus('[Error] Task "%s" (%s) throw an exception, check your log', $arguments);
        }
    }

    /**
     * @param Task $task
     */
    public function removeCommand(Task $task): void
    {
        $this->taskService->remove($task);
    }

    /**
     * Enable the given persistent class
     *
     * @param Task $task persistent task identifier, see task:list
     */
    public function enableCommand(Task $task): void
    {
        $task->enable();
        $this->taskService->update($task, TaskTypeEnum::TYPE_PERSISTED);
    }

    /**
     * Disable the given persistent class
     *
     * @param Task $task persistent task identifier, see task:list
     */
    public function disableCommand(Task $task): void
    {
        $task->disable();
        $this->taskService->update($task, TaskTypeEnum::TYPE_PERSISTED);
    }

    /**
     * Register a persistent task
     *
     * @param string $expression cron expression for the task scheduling
     * @param string $task task class implementation
     * @param ?string $arguments task arguments, can be a valid JSON array
     * @param string $description task description
     */
    public function registerCommand(string $expression, string $task, ?string $arguments = null, string $description = ''): void
    {

        if (!empty($arguments)) {
            $arguments = json_decode($arguments, true, flags: JSON_THROW_ON_ERROR);
            assert(is_array($arguments));
        }

        $this->taskService->create($expression, $task, $arguments ?: [], $description);
    }

    /**
     * @param string $message
     * @param array<bool|float|int|string|null> $arguments
     */
    protected function tellStatus(string $message, array $arguments = []): void
    {
        $message = vsprintf($message, $arguments);
        $this->outputLine('%s: %s', [date(DateTimeInterface::ATOM), $message]);
    }

    protected function markTaskAsRun(Task $task, TaskTypeEnum $taskType): void
    {
        $task->markAsRun();
        $this->taskService->update($task, $taskType);
        if ($taskType === TaskTypeEnum::TYPE_PERSISTED) {
            $this->persistenceManager->allowObject($task);
            $this->persistenceManager->persistAll(true);
        }
    }
}
