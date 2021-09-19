<?php

namespace Phlib\DbHelper\Tests;

use Phlib\DbHelper\QueryPlanner;
use Phlib\Db\Adapter;

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
        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter = $this->createMock(Adapter::class);
        $this->adapter->method('prepare')
            ->willReturn($pdoStatement);
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
            ['rows' => $row1],
            ['rows' => $row2],
            ['rows' => $row3]
        ];

        /** @var QueryPlanner|\PHPUnit_Framework_MockObject_MockObject $planner */
        $planner = $this->getMockBuilder(QueryPlanner::class)
            ->setConstructorArgs([$this->adapter, 'SELECT'])
            ->setMethods(['getPlan'])
            ->getMock();
        $planner->method('getPlan')
            ->willReturn($plan);

        $expected = $row1 * $row2 * $row3;
        static::assertEquals($expected, $planner->getNumberOfRowsInspected());
    }

    public function testGetNumberOfRowsInspectedDoesNotExceedMaxInt()
    {
        $row1 = PHP_INT_MAX;
        $row2 = 2;
        $plan = [
            ['rows' => $row1],
            ['rows' => $row2]
        ];

        /** @var QueryPlanner|\PHPUnit_Framework_MockObject_MockObject $planner */
        $planner = $this->getMockBuilder(QueryPlanner::class)
            ->setConstructorArgs([$this->adapter, 'SELECT'])
            ->setMethods(['getPlan'])
            ->getMock();
        $planner->method('getPlan')
            ->willReturn($plan);

        static::assertInternalType('integer', $planner->getNumberOfRowsInspected());
    }
}
