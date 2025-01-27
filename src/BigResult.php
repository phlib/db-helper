<?php

declare(strict_types=1);

namespace Phlib\DbHelper;

use Phlib\Db\Adapter;
use Phlib\DbHelper\Exception\InvalidArgumentException;

/**
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BigResult
{
    private \Closure $queryPlannerFactory;

    /**
     * @internal @param \Closure $queryPlannerFactory Used for DI in tests; not expected to be used in production. Not part of BC promise.
     */
    public function __construct(
        private Adapter $adapter,
        private int $longQueryTime = 7200,
        private int $netWriteTimeout = 7200,
        \Closure $queryPlannerFactory = null,
    ) {
        if ($queryPlannerFactory === null) {
            $queryPlannerFactory = function (Adapter $adapter, string $select, array $bind = []): QueryPlanner {
                return new QueryPlanner($adapter, $select, $bind);
            };
        }
        $this->queryPlannerFactory = $queryPlannerFactory;
    }

    public static function execute(
        Adapter $adapter,
        string $select,
        array $bind = [],
        int $rowLimit = null,
    ): \PDOStatement {
        return (new static($adapter))
            ->query($select, $bind, $rowLimit);
    }

    /**
     * Execute query and return the unbuffered statement.
     */
    public function query(string $select, array $bind = [], int $inspectedRowLimit = null): \PDOStatement
    {
        if ($inspectedRowLimit !== null) {
            $inspectedRows = $this->getInspectedRows($select, $bind);
            if ($inspectedRows > $inspectedRowLimit) {
                throw new InvalidArgumentException("Number of rows inspected exceeds '{$inspectedRowLimit}'");
            }
        }

        $adapter = clone $this->adapter;
        $adapter->query("SET @@long_query_time={$this->longQueryTime}, @@net_write_timeout={$this->netWriteTimeout}");
        $adapter->disableBuffering();

        $stmt = $adapter->prepare($select);
        $stmt->execute($bind);

        return $stmt;
    }

    private function getInspectedRows(string $select, array $bind): int
    {
        return ($this->queryPlannerFactory)($this->adapter, $select, $bind)
            ->getNumberOfRowsInspected();
    }
}
