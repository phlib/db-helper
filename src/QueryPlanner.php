<?php

declare(strict_types=1);

namespace Phlib\DbHelper;

use Phlib\Db\Adapter;

/**
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class QueryPlanner
{
    public function __construct(
        private Adapter $adapter,
        private string $select,
        private array $bind = [],
    ) {
    }

    public function getPlan(): array
    {
        return $this->adapter
            ->query("EXPLAIN {$this->select}", $this->bind)
            ->fetchAll(\PDO::FETCH_ASSOC);
    }

    public function getNumberOfRowsInspected(): int
    {
        $inspectedRows = 1;
        foreach ($this->getPlan() as $analysis) {
            $inspectedRows *= (int)$analysis['rows'];

            // when exceeding PHPs integer max, it becomes a float
            if (is_float($inspectedRows)) {
                $inspectedRows = PHP_INT_MAX;
                break;
            }
        }

        return $inspectedRows;
    }
}
