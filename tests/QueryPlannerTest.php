<?php

declare(strict_types=1);

namespace Phlib\DbHelper\Tests;

use Phlib\Db\Adapter;
use Phlib\DbHelper\QueryPlanner;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * QueryPlanner Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class QueryPlannerTest extends TestCase
{
    /**
     * @var Adapter|MockObject
     */
    protected $adapter;

    protected function setUp(): void
    {
        $this->adapter = $this->createMock(Adapter::class);

        parent::setUp();
    }

    public function testGetPlanDoesExplain(): void
    {
        $pdoStatement = $this->createMock(\PDOStatement::class);
        $pdoStatement->expects(static::once())
            ->method('fetchAll')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn([]);

        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains('EXPLAIN', true))
            ->willReturn($pdoStatement);

        (new QueryPlanner($this->adapter, 'SELECT'))->getPlan();
    }

    public function testGetNumberOfRowsInspected(): void
    {
        $row1 = 1;
        $row2 = 2;
        $row3 = 3;
        $plan = [
            [
                'rows' => $row1,
            ],
            [
                'rows' => $row2,
            ],
            [
                'rows' => $row3,
            ],
        ];

        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('query')
            ->willReturn($pdoStatement);

        $pdoStatement->expects(static::once())
            ->method('fetchAll')
            ->willReturn($plan);

        $planner = new QueryPlanner($this->adapter, 'SELECT');

        $expected = $row1 * $row2 * $row3;
        static::assertEquals($expected, $planner->getNumberOfRowsInspected());
    }

    public function testGetNumberOfRowsInspectedDoesNotExceedMaxInt(): void
    {
        $row1 = PHP_INT_MAX;
        $row2 = 2;
        $plan = [
            [
                'rows' => $row1,
            ],
            [
                'rows' => $row2,
            ],
        ];

        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter->expects(static::once())
            ->method('query')
            ->willReturn($pdoStatement);

        $pdoStatement->expects(static::once())
            ->method('fetchAll')
            ->willReturn($plan);

        $planner = new QueryPlanner($this->adapter, 'SELECT');

        static::assertInternalType('integer', $planner->getNumberOfRowsInspected());
    }
}
