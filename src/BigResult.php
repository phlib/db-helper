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
    private array $options;

    private \Closure $queryPlannerFactory;

    /**
     * @param array{
     *     long_query_time?: int, // Default 7200
     *     net_write_timeout?: int, // Default 7200
     * } $options
     * @internal @param \Closure $queryPlannerFactory Used for DI in tests; not expected to be used in production. Not part of BC promise.
     */
    public function __construct(
        private Adapter $adapter,
        array $options = [],
        \Closure $queryPlannerFactory = null
    ) {
        $this->options = $options + [
            'long_query_time' => 7200,
            'net_write_timeout' => 7200,
        ];

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
        int $rowLimit = null
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

        $longQueryTime = $this->options['long_query_time'];
        $netWriteTimeout = $this->options['net_write_timeout'];

        $adapter = clone $this->adapter;
        $adapter->query("SET @@long_query_time={$longQueryTime}, @@net_write_timeout={$netWriteTimeout}");
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
