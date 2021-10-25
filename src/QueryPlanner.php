<?php

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
    protected $adapter;

    /**
     * @var string
     */
    protected $select;

    /**
     * @var array
     */
    protected $bind;

    /**
     * @param string $select
     */
    public function __construct(Adapter $adapter, $select, array $bind = [])
    {
        $this->adapter = $adapter;
        $this->select = $select;
        $this->bind = $bind;
    }

    /**
     * @return array
     */
    public function getPlan()
    {
        return $this->adapter
            ->query("EXPLAIN {$this->select}", $this->bind)
            ->fetchAll(\PDO::FETCH_ASSOC);
    }

    /**
     * @return int
     */
    public function getNumberOfRowsInspected()
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
