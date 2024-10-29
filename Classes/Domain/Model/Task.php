<?php

namespace Ttree\Scheduler\Domain\Model;

/*                                                                        *
 * This script belongs to the Neos Flow package "Ttree.Scheduler".       *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        */

use Cron\CronExpression;
use Doctrine\ORM\Mapping as ORM;
use Ttree\Scheduler\Task\TaskInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\Utility\Now;

/**
 * Schedule Task
 *
 * @Flow\Entity
 */
class Task
{
    const STATUS_DISABLED = 0;
    const STATUS_ENABLED = 1;

    /**
     * @var integer
     */
    protected $status;

    /**
     * @var string
     * @Flow\Identity
     */
    protected $expression;

    /**
     * @var string
     * @Flow\Identity
     */
    protected $implementation;

    /**
     * @var array<mixed>
     */
    protected $arguments;

    /**
     * @var string
     * @Flow\Identity
     */
    protected $argumentsHash;

    /**
     * @var \DateTime
     */
    protected $creationDate;

    /**
     * @var ?\DateTime
     * @ORM\Column(nullable=true)
     */
    protected $lastExecution;

    /**
     * @var \DateTime
     * @ORM\Column(nullable=true)
     */
    protected $nextExecution;

    /**
     * @var CronExpression|null
     * @Flow\Transient
     */
    protected ?CronExpression $cronExpression;

    /**
     * @var string
     * @ORM\Column(type="text")
     */
    protected string $description;


    /**
     * @param array<mixed> $arguments
     */
    public function __construct(string $expression, string $implementation, array $arguments = [], string $description = '')
    {
        $this->disable();
        $this->setExpression($expression);
        $this->setImplementation($implementation);
        $this->setArguments($arguments);
        $this->setDescription($description);
        $this->creationDate = new \DateTime('now');
        $this->initializeNextExecution();
    }

    public function getCronExpression(): CronExpression
    {
        if ($this->cronExpression === null) {
            $this->cronExpression = CronExpression::factory($this->expression);
        }
        return $this->cronExpression;
    }


    public function isDue(): bool
    {
        $now = new Now();
        return $this->nextExecution <= $now;
    }


    public function getPreviousRunDate(): \DateTime
    {
        return $this->getCronExpression()->getPreviousRunDate();
    }


    public function isDisabled(): bool
    {
        return $this->status === self::STATUS_DISABLED;
    }


    public function isEnabled(): bool
    {
        return $this->status === self::STATUS_ENABLED;
    }

    /**
     * @return void
     */
    public function enable(): void
    {
        $this->status = self::STATUS_ENABLED;
    }

    /**
     * @return void
     */
    public function disable(): void
    {
        $this->status = self::STATUS_DISABLED;
    }

    public function getExpression(): string
    {
        return $this->expression;
    }

    public function setExpression(string $expression): void
    {
        /* Slashes in annotaion expressions of dynamic tasks have to be double-escaped due to proxy classes.
        For the cron expression, the remaining backslash needs to be removed here. */
        $this->expression = str_replace('\\', '', $expression);
        $this->initializeNextExecution();
    }

    public function getImplementation(): string
    {
        return $this->implementation;
    }

    public function setImplementation(string $implementation): void
    {
        $this->implementation = $implementation;
    }

    /**
     * @return array<mixed>
     */
    public function getArguments(): array
    {
        return $this->arguments;
    }

    /**
     * @param array<mixed> $arguments
     */
    public function setArguments($arguments): void
    {
        $this->arguments = $arguments;
        $this->argumentsHash = sha1(serialize($arguments));
    }

    public function execute(ObjectManagerInterface $objectManager): void
    {
        $task = $objectManager->get($this->implementation, $this);
        assert($task instanceof TaskInterface);
        $task->execute($this->arguments);
    }


    public function initializeNextExecution(): void
    {
        $this->nextExecution = $this->getCronExpression()->getNextRunDate();
    }

    public function getCreationDate(): \DateTime
    {
        return clone $this->creationDate;
    }

    public function getLastExecution(): ?\DateTime
    {
        return $this->lastExecution ? clone $this->lastExecution : null;
    }

    /**
     * @param \DateTime|string|null $currentTime
     * @return \DateTime
     */
    public function getNextExecution($currentTime = null): \DateTime
    {
        if ($currentTime) {
            return $this->getCronExpression()->getNextRunDate($currentTime);
        } else {
            return clone $this->nextExecution;
        }
    }

    public function getDescription(): string
    {
        return $this->description;
    }

    public function setDescription(string $description): void
    {
        $this->description = $description;
    }

    public function markAsRun(): void
    {
        $this->lastExecution = new \DateTime('now');
        $this->initializeNextExecution();
    }
}
