<?php

namespace Ttree\Scheduler\Task;

enum TaskTypeEnum: string
{
    case TYPE_PERSISTED = 'Persisted';
    case TYPE_DYNAMIC = 'Dynamic';
}
