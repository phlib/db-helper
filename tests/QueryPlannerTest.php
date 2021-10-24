<?php

namespace Phlib\DbHelper\Tests;

use Phlib\Db\Adapter;
use Phlib\DbHelper\QueryPlanner;

/**
 * QueryPlanner Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class QueryPlannerTest extends \PHPUnit_Framework_TestCase
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

    public function testGetPlanDoesExplain()
    {
        $pdoStatement = $this->createMock(\PDOStatement::class);
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains('EXPLAIN', true))
            ->willReturn($pdoStatement);

        (new QueryPlanner($this->adapter, 'SELECT'))->getPlan();
    }

    public function testGetNumberOfRowsInspected()
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

    public function testGetNumberOfRowsInspectedDoesNotExceedMaxInt()
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
