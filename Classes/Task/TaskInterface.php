<?php

namespace Ttree\Scheduler\Task;

/*                                                                        *
 * This script belongs to the Neos Flow package "Ttree.Scheduler".       *
 *                                                                        *
 * It is free software; you can redistribute it and/or modify it under    *
 * the terms of the GNU General Public License, either version 3 of the   *
 * License, or (at your option) any later version.                        *
 *                                                                        */

use Neos\Flow\Annotations as Flow;

/**
 * Schedule Task
 */
interface TaskInterface
{
    /**
     * @param array<mixed> $arguments
     * @return mixed
     */
    public function execute(array $arguments = []);
}
