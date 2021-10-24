<?php

namespace Phlib\DbHelper\Tests;

use Phlib\Db\Adapter;
use Phlib\DbHelper\BigResult;
use Phlib\DbHelper\Exception\InvalidArgumentException;
use Phlib\DbHelper\QueryPlanner;

/**
 * BigResult Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BigResultTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var \Phlib\Db\Adapter|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $adapter;

    protected function setUp()
    {
        $this->adapter = $this->createMock(Adapter::class);

        parent::setUp();
    }

    public function testQueryIsSetupForLongQueryTime()
    {
        $queryTime = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("long_query_time={$queryTime}"));

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult($this->adapter, [
            'long_query_time' => $queryTime,
        ]))
            ->query('SELECT');
    }

    public function testQueryIsSetupForWriteTimeout()
    {
        $writeTimeout = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("net_write_timeout={$writeTimeout}"));

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult($this->adapter, [
            'net_write_timeout' => $writeTimeout,
        ]))
            ->query('SELECT');
    }

    public function testQueryDisablesBuffering()
    {
        $this->adapter->expects(static::once())
            ->method('disableBuffering');

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult($this->adapter))->query('SELECT');
    }

    public function testQueryReturnsStatement()
    {
        $bigResult = (new BigResult($this->adapter));

        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($pdoStatement);

        static::assertSame($pdoStatement, $bigResult->query('SELECT'));
    }

    public function testCheckForInspectedRowLimitOnSuccess()
    {
        $select = 'SELECT ' . rand();
        $bind = [
            sha1(uniqid()),
        ];

        $queryPlanner = $this->createMock(QueryPlanner::class);
        $queryPlanner->expects($this->once())
            ->method('getNumberOfRowsInspected')
            ->will($this->returnValue(5));

        $queryPlannerFactory = function (
            Adapter $adapterPass,
            $selectPass,
            array $bindPass = []
        ) use (
            $queryPlanner,
            $select,
            $bind
        ) {
            $this->assertSame($this->adapter, $adapterPass);
            $this->assertSame($select, $selectPass);
            $this->assertSame($bind, $bindPass);
            return $queryPlanner;
        };

        $bigResult = new BigResult($this->adapter, [], $queryPlannerFactory);
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringStartsWith('SET @@long_query_time='));

        $pdoStatement = $this->createMock(\PDOStatement::class);
        $this->adapter->expects(static::once())
            ->method('prepare')
            ->with($select)
            ->willReturn($pdoStatement);

        $pdoStatement->expects(static::once())
            ->method('execute')
            ->with($bind);

        $bigResult->query($select, $bind, 10);
    }

    /**
     * @expectedException InvalidArgumentException
     */
    public function testCheckForInspectedRowLimitOnFailure()
    {
        $select = 'SELECT ' . rand();
        $bind = [
            sha1(uniqid()),
        ];

        $queryPlanner = $this->createMock(QueryPlanner::class);
        $queryPlanner->expects($this->once())
            ->method('getNumberOfRowsInspected')
            ->will($this->returnValue(10));

        $queryPlannerFactory = function (
            Adapter $adapterPass,
            $selectPass,
            array $bindPass = []
        ) use (
            $queryPlanner,
            $select,
            $bind
        ) {
            $this->assertSame($this->adapter, $adapterPass);
            $this->assertSame($select, $selectPass);
            $this->assertSame($bind, $bindPass);
            return $queryPlanner;
        };

        $bigResult = new BigResult($this->adapter, [], $queryPlannerFactory);
        $this->adapter->expects(static::never())
            ->method('query');

        $this->adapter->expects(static::never())
            ->method('prepare');

        $bigResult->query($select, $bind, 5);
    }

    public function testStaticExecuteReturnsStatement()
    {
        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($pdoStatement);

        static::assertSame($pdoStatement, BigResult::execute($this->adapter, 'SELECT'));
    }
}
