<?php

namespace Phlib\DbHelper\Tests;

use Phlib\DbHelper\BigResult;
use Phlib\DbHelper\Exception\InvalidArgumentException;
use Phlib\Db\Adapter;

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
        $pdoStatement = $this->createMock(\PDOStatement::class);

        $this->adapter = $this->createMock(Adapter::class);
        $this->adapter->method('prepare')
            ->willReturn($pdoStatement);

        parent::setUp();
    }

    public function testQueryIsSetupForLongQueryTime()
    {
        $queryTime = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("long_query_time=$queryTime"));

        (new BigResult($this->adapter, ['long_query_time' => $queryTime]))
            ->query('SELECT');
    }

    public function testQueryIsSetupForWriteTimeout()
    {
        $writeTimeout = 123;
        $this->adapter->expects(static::once())
            ->method('query')
            ->with(static::stringContains("net_write_timeout=$writeTimeout"));

        (new BigResult($this->adapter, ['net_write_timeout' => $writeTimeout]))
            ->query('SELECT');
    }

    public function testQueryDisablesBuffering()
    {
        $this->adapter->expects(static::once())
            ->method('disableBuffering');

        (new BigResult($this->adapter))->query('SELECT');
    }

    public function testQueryReturnsStatement()
    {
        $bigResult = (new BigResult($this->adapter));

        static::assertInstanceOf(\PDOStatement::class, $bigResult->query('SELECT'));
    }

    public function testCheckForInspectedRowLimitOnSuccess()
    {
        /** @var BigResult|\PHPUnit_Framework_MockObject_MockObject $bigResult */
        $bigResult = $this->getMockBuilder(BigResult::class)
            ->setConstructorArgs([$this->adapter])
            ->setMethods(['getInspectedRows'])
            ->getMock();
        $bigResult->method('getInspectedRows')
            ->willReturn(5);

        $this->adapter->expects(static::once())
            ->method('query');

        $bigResult->query('SELECT', [], 10);
    }

    /**
     * @expectedException InvalidArgumentException
     */
    public function testCheckForInspectedRowLimitOnFailure()
    {
        /** @var BigResult|\PHPUnit_Framework_MockObject_MockObject $bigResult */
        $bigResult = $this->getMockBuilder(BigResult::class)
            ->setConstructorArgs([$this->adapter])
            ->setMethods(['getInspectedRows'])
            ->getMock();
        $bigResult->method('getInspectedRows')
            ->willReturn(10);

        $bigResult->query('SELECT', [], 5);
    }

    public function testStaticExecuteReturnsStatement()
    {
        static::assertInstanceOf(\PDOStatement::class, BigResult::execute($this->adapter, 'SELECT'));
    }
}
