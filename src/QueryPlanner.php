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
    /**
     * @var Adapter
     */
    private $adapter;

    /**
     * @var string
     */
    private $select;

    /**
     * @var array
     */
    private $bind;

    public function __construct(Adapter $adapter, string $select, array $bind = [])
    {
        $this->adapter = $adapter;
        $this->select = $select;
        $this->bind = $bind;
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
