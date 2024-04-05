<?php

declare(strict_types=1);

namespace Phlib\DbHelper\Tests;

use Phlib\Db\Adapter;
use Phlib\DbHelper\BigResult;
use Phlib\DbHelper\Exception\InvalidArgumentException;
use Phlib\DbHelper\QueryPlanner;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * BigResult Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BigResultTest extends TestCase
{
    /**
     * @var Adapter|MockObject
     */
    private MockObject $adapter;

    protected function setUp(): void
    {
        $this->adapter = $this->createMock(Adapter::class);

        parent::setUp();
    }

    public function testQueryIsSetupForLongQueryTime(): void
    {
        $queryTime = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("long_query_time={$queryTime}"));

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult(
            $this->adapter,
            longQueryTime: $queryTime,
        ))
            ->query('SELECT');
    }

    public function testQueryIsSetupForWriteTimeout(): void
    {
        $writeTimeout = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("net_write_timeout={$writeTimeout}"));

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult(
            $this->adapter,
            netWriteTimeout: $writeTimeout,
        ))
            ->query('SELECT');
    }

    public function testQueryDisablesBuffering(): void
    {
        $this->adapter->expects(static::once())
            ->method('disableBuffering');

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($this->createMock(\PDOStatement::class));

        (new BigResult($this->adapter))->query('SELECT');
    }

    public function testQueryReturnsStatement(): void
    {
        $bigResult = (new BigResult($this->adapter));

        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($pdoStatement);

        static::assertSame($pdoStatement, $bigResult->query('SELECT'));
    }

    public function testCheckForInspectedRowLimitOnSuccess(): void
    {
        $select = 'SELECT ' . rand();
        $bind = [
            sha1(uniqid()),
        ];

        $queryPlanner = $this->createMock(QueryPlanner::class);
        $queryPlanner->expects(static::once())
            ->method('getNumberOfRowsInspected')
            ->willReturn(5);

        $queryPlannerFactory = function (
            Adapter $adapterPass,
            string $selectPass,
            array $bindPass = []
        ) use (
            $queryPlanner,
            $select,
            $bind
        ): QueryPlanner {
            static::assertSame($this->adapter, $adapterPass);
            static::assertSame($select, $selectPass);
            static::assertSame($bind, $bindPass);
            return $queryPlanner;
        };

        $bigResult = new BigResult($this->adapter, queryPlannerFactory: $queryPlannerFactory);
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

    public function testCheckForInspectedRowLimitOnFailure(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $select = 'SELECT ' . rand();
        $bind = [
            sha1(uniqid()),
        ];

        $queryPlanner = $this->createMock(QueryPlanner::class);
        $queryPlanner->expects(static::once())
            ->method('getNumberOfRowsInspected')
            ->willReturn(10);

        $queryPlannerFactory = function (
            Adapter $adapterPass,
            string $selectPass,
            array $bindPass = []
        ) use (
            $queryPlanner,
            $select,
            $bind
        ): QueryPlanner {
            static::assertSame($this->adapter, $adapterPass);
            static::assertSame($select, $selectPass);
            static::assertSame($bind, $bindPass);
            return $queryPlanner;
        };

        $bigResult = new BigResult($this->adapter, queryPlannerFactory: $queryPlannerFactory);
        $this->adapter->expects(static::never())
            ->method('query');

        $this->adapter->expects(static::never())
            ->method('prepare');

        $bigResult->query($select, $bind, 5);
    }

    public function testStaticExecuteReturnsStatement(): void
    {
        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('prepare')
            ->willReturn($pdoStatement);

        static::assertSame($pdoStatement, BigResult::execute($this->adapter, 'SELECT'));
    }
}
