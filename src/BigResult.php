<?php

namespace Phlib\DbHelper;

use Phlib\Db\Adapter;
use Phlib\DbHelper\Exception\InvalidArgumentException;

/**
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BigResult
{
    /**
     * @var Adapter
     */
    protected $adapter;

    /**
     * @var array
     */
    protected $options;

    /**
     * @var \Closure
     */
    private $queryPlannerFactory;

    /**
     * @param array $options {
     *     @var int $long_query_time   Default 7200
     *     @var int $net_write_timeout Default 7200
     * }
     * @internal @param \Closure Used for DI in tests; not expected to be used in production. Not part of BC promise.
     */
    public function __construct(Adapter $adapter, array $options = [], \Closure $queryPlannerFactory = null)
    {
        $this->adapter = $adapter;
        $this->options = $options + [
            'long_query_time' => 7200,
            'net_write_timeout' => 7200,
        ];

        if ($queryPlannerFactory === null) {
            $queryPlannerFactory = function (Adapter $adapter, $select, array $bind = []) {
                return new QueryPlanner($adapter, $select, $bind);
            };
        }
        $this->queryPlannerFactory = $queryPlannerFactory;
    }

    /**
     * @param string $select
     * @param null $rowLimit
     * @return \PDOStatement
     */
    public static function execute(Adapter $adapter, $select, array $bind = [], $rowLimit = null)
    {
        return (new static($adapter))->query($select, $bind, $rowLimit);
    }

    /**
     * Execute query and return the unbuffered statement.
     *
     * @param string $select
     * @param int $inspectedRowLimit
     * @return \PDOStatement
     */
    public function query($select, array $bind = [], $inspectedRowLimit = null)
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

    /**
     * @param string $select
     * @return int
     */
    protected function getInspectedRows($select, array $bind)
    {
        return ($this->queryPlannerFactory)($this->adapter, $select, $bind)
            ->getNumberOfRowsInspected();
    }
}
