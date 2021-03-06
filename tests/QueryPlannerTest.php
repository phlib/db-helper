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

        $this->adapter = $this->getMockBuilder(Adapter::class)->getMock();
        $this->adapter->expects($this->any())
            ->method('prepare')
            ->will($this->returnValue($pdoStatement));
        parent::setUp();
    }

    public function tearDown()
    {
        parent::tearDown();
        $this->adapter = null;
    }

    public function testGetPlanDoesExplain()
    {
        $pdoStatement = $this->createMock(\PDOStatement::class);
        $this->adapter->expects($this->once())
            ->method('query')
            ->with($this->stringContains('EXPLAIN', true))
            ->will($this->returnValue($pdoStatement));
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
        $planner->expects($this->any())
            ->method('getPlan')
            ->will($this->returnValue($plan));

        $expected = $row1 * $row2 * $row3;
        $this->assertEquals($expected, $planner->getNumberOfRowsInspected());
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
        $planner->expects($this->any())
            ->method('getPlan')
            ->will($this->returnValue($plan));

        $this->assertInternalType('integer', $planner->getNumberOfRowsInspected());
    }
}
