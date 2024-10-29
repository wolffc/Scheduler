<?php

declare(strict_types=1);

namespace Ttree\Scheduler\Task;

use Ttree\Scheduler\Domain\Model\Task;

class TaskDescriptor
{
    private function __construct(
        public readonly TaskTypeEnum $type,
        public readonly string $identifier,
        public readonly Task $task,
        private readonly ?\DateTime $lastExecution
    ) {
    }

    public static function fromPersistedTask(mixed $identifier, Task $task): self
    {
        if (!is_scalar($identifier)) {
            throw new \Exception('scalar Identifier expected', 1730112944422);
        }
        return new self(
            TaskTypeEnum::TYPE_PERSISTED,
            (string)$identifier,
            $task,
            $task->getLastExecution()
        );
    }

    public static function fromDynamicTask(Task $task, ?\DateTime $lastExecution): self
    {
        return new self(
            TaskTypeEnum::TYPE_DYNAMIC,
            md5($task->getImplementation()),
            $task,
            $lastExecution
        );
    }



    public function getLastExecution(): \DateTime|null
    {
        return $this->lastExecution ? clone $this->lastExecution : null;
    }

    public function getEnabledLabel(): string
    {
        return $this->task->isEnabled() ? 'On' : 'Off';
    }
}
