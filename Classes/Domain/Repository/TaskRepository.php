<?php

namespace Ttree\Scheduler\Domain\Repository;

/*                                                                        *
 * This script belongs to the Neos Flow package "Ttree.Scheduler".       *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        */

use Ttree\Scheduler\Domain\Model\Task;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\QueryInterface;
use Neos\Flow\Persistence\Repository;
use Neos\Flow\Utility\Now;

/**
 * Schedule Task
 *
 * @Flow\Scope("singleton")
 */
class TaskRepository extends Repository
{
    /**
     * @var array<string,string>
     */
    protected $defaultOrderings = [
        'status' => QueryInterface::ORDER_ASCENDING,
        'nextExecution' => QueryInterface::ORDER_ASCENDING
    ];

    /**
     * @param string $identifier
     * @return Task|null
     */
    public function findByIdentifier($identifier): ?Task
    {
        $object =  parent::findByIdentifier($identifier);
        assert(is_null($object) || $object instanceof Task);
        return $object;
    }

    /**
     * @return \Neos\Flow\Persistence\QueryResultInterface<Task>
     */
    public function findDueTasks()
    {
        $query = $this->createQuery();

        $now = new Now();

        $query->matching($query->logicalAnd(
            $query->equals('status', Task::STATUS_ENABLED),
            $query->lessThanOrEqual('nextExecution', $now)
        ));

        return $query->execute();
    }

    /**
     * @param boolean $showDisabled
     * @return \Neos\Flow\Persistence\QueryResultInterface
     */
    public function findAllTasks($showDisabled = false)
    {
        $query = $this->createQuery();

        if (!$showDisabled) {
            $query->matching($query->equals('status', Task::STATUS_ENABLED));
        }

        return $query->execute();
    }

    /**
     * @param string $implementation
     * @param array<mixed> $arguments
     * @return Task
     */
    public function findOneByImplementationAndArguments(string $implementation, array $arguments)
    {
        $argumentsHash = sha1(serialize($arguments));
        $query = $this->createQuery();

        $query->matching($query->logicalAnd(
            $query->equals('implementation', $implementation),
            $query->equals('argumentsHash', $argumentsHash)
        ));

        $task = $query->execute()->getFirst();
        assert($task instanceof Task);
        return $task;
    }
}
