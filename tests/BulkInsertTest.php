<?php

namespace Phlib\DbHelper\Tests;

use Phlib\DbHelper\BulkInsert;
use Phlib\Db\Exception\RuntimeException as DbRuntimeException;
use Phlib\Db\Adapter;

/**
 * BulkInsert Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BulkInsertTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @var Adapter|\PHPUnit_Framework_MockObject_MockObject
     */
    protected $adapter;

    protected function setUp()
    {
        $this->adapter = $this->createMock(Adapter::class);

        $quoteHandler = new Adapter\QuoteHandler(function ($value) {
            return "`$value`";
        });
        $this->adapter->method('quote')
            ->willReturn($quoteHandler);

        parent::setUp();
    }

    /**
     * @dataProvider fetchSqlQuotesDataProvider
     * @param mixed $value
     * @param string $expected
     */
    public function testFetchSqlQuotes($value, $expected)
    {
        $table = 'test_table';
        $insertFields = ['field'];
        $updateFields = ['field'];
        $inserter = new BulkInsert($this->adapter, $table, $insertFields, $updateFields);

        $inserter->add([$value]);
        $actual = $inserter->fetchSql();

        $expectedSql = "VALUES ({$expected})";

        static::assertContains($expectedSql, $actual);
    }

    public function fetchSqlQuotesDataProvider()
    {
        return [
            ['string', '`string`'],
            ['a1', '`a1`'],
            ['1a', '`1a`'],
            [172, '172'],
            ['172', '172'],
            [172.16, '172.16'],
            ['172.16', '172.16'],
            ['172.16.255.255', '`172.16.255.255`'],
            ['2017-03-18 00:00:00', '`2017-03-18 00:00:00`'],
        ];
    }

    /**
     * @dataProvider fetchSqlIgnoreUpdateDataProvider
     * @param bool $ignore
     * @param bool $update
     */
    public function testFetchSqlIgnoreUpdate($ignore, $update)
    {
        $table = 'test_table';
        $insertFields = ['field'];
        $updateFields = [];
        if ($update) {
            $updateFields = ['field'];
        }
        $inserter = new BulkInsert($this->adapter, $table, $insertFields, $updateFields);

        if ($ignore) {
            $inserter->insertIgnoreEnabled();
        } else {
            $inserter->insertIgnoreDisabled();
        }

        $inserter->add(['value']);
        $actual = $inserter->fetchSql();

        $needle = 'INSERT IGNORE INTO';
        if ($ignore && !$update) {
            static::assertContains($needle, $actual);
        } else {
            static::assertNotContains($needle, $actual);
        }

        $needle = 'ON DUPLICATE KEY UPDATE';
        if ($update) {
            static::assertContains($needle, $actual);
        } else {
            static::assertNotContains($needle, $actual);
        }
    }

    public function fetchSqlIgnoreUpdateDataProvider()
    {
        return [
            [true, true],
            [true, false],
            [false, true],
            [false, false]
        ];
    }

    public function testFetchSqlReturnsFalseWhenNoRows()
    {
        static::assertFalse((new BulkInsert($this->adapter, 'table', ['field']))->fetchSql());
    }

    public function testAddCallsWriteWhenExceedsBatchSize()
    {
        $inserter = new BulkInsert($this->adapter, 'table_name', ['field1'], [], ['batchSize' => 1]);

        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter->add(['field1' => 'foo']);
    }

    public function testWriteCallsAdapterExecute()
    {
        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add(['field1' => 'foo', 'field2' => 'bar']);
        $inserter->write();
    }

    public function testWriteReturnsEarlyWhenNoRows()
    {
        $this->adapter->expects(static::never())
            ->method('execute');

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->write();
    }

    public function testWriteDoesNotWriteTheSameRows()
    {
        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add(['field1' => 'foo', 'field2' => 'bar']);
        $inserter->write();
        $inserter->write();
    }

    public function testWriteDetectsDeadlockAndHandlesIt()
    {
        $this->adapter->expects(static::exactly(2))
            ->method('execute')
            ->will(static::onConsecutiveCalls(
                static::throwException(new DbRuntimeException('Deadlock found when trying to get lock')),
                static::returnValue(1)
            ));

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add(['field1' => 'foo', 'field2' => 'bar']);
        $inserter->write();
    }

    /**
     * @expectedException \Phlib\Db\Exception\RuntimeException
     */
    public function testWriteAllowsNonDeadlockErrorsToBubble()
    {
        $this->adapter->method('execute')
            ->will(static::throwException(new DbRuntimeException('Some other foo exception')));

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add(['field1' => 'foo', 'field2' => 'bar']);
        $inserter->write();
    }

    public function testFetchStatsOnInitialConstructWithoutFlush()
    {
        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        $inserter = new BulkInsert($this->adapter, 'table', ['field1']);
        static::assertEquals($expected, $inserter->fetchStats($flush = false));
    }

    public function testFetchStatsOnInitialConstructWithFlush()
    {
        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        $inserter = new BulkInsert($this->adapter, 'table', ['field1']);
        static::assertEquals($expected, $inserter->fetchStats($flush = true));
    }

    /**
     * @param int $expected
     * @param string $statistic
     * @param int $noOfInserts
     * @param int $noOfUpdates
     * @param bool $withFlush
     * @dataProvider fetchStatsIncrementsDataProvider
     */
    public function testFetchStatsIncrements($expected, $statistic, $noOfInserts, $noOfUpdates, $withFlush)
    {
        $affectedRows = ($noOfUpdates * 2) + $noOfInserts;
        $this->adapter->method('execute')
            ->willReturn($affectedRows);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1'], []);
        $totalRows = $noOfInserts + $noOfUpdates;
        for ($i = 0; $i < $totalRows; $i++) {
            $inserter->add(['field1' => 'foo']);
        }

        $stats = $inserter->fetchStats($withFlush);
        static::assertEquals($expected, $stats[$statistic]);
    }

    public function fetchStatsIncrementsDataProvider()
    {
        return [
            // total
            [0, 'total', 1, 0, false],
            [1, 'total', 1, 0, true],
            [0, 'total', 1, 1, false],
            [2, 'total', 1, 1, true],
            [5, 'total', 5, 0, true],
            [10, 'total', 5, 5, true],
            // inserted
            [0, 'inserted', 1, 0, false],
            [1, 'inserted', 1, 0, true],
            [0, 'inserted', 1, 1, false],
            [1, 'inserted', 1, 1, true],
            [0, 'inserted', 0, 5, true],
            [5, 'inserted', 5, 0, true],
            [5, 'inserted', 5, 5, true],
            // updated
            [0, 'updated', 1, 0, false],
            [0, 'updated', 1, 0, true],
            [0, 'updated', 1, 1, false],
            [1, 'updated', 1, 1, true],
            [5, 'updated', 0, 5, true],
            [0, 'updated', 5, 0, true],
            [5, 'updated', 5, 5, true],
            // pending
            [1, 'pending', 1, 0, false],
            [0, 'pending', 1, 0, true],
            [2, 'pending', 1, 1, false],
            [0, 'pending', 1, 1, true],
            [5, 'pending', 5, 0, false],
            [10, 'pending', 5, 5, false],
            [0, 'pending', 5, 0, true],
            [0, 'pending', 5, 5, true],
        ];
    }

    public function testClearStats()
    {
        $this->adapter->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1'], []);
        $inserter->add(['field1' => 'foo']);

        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        static::assertNotEquals($expected, $inserter->fetchStats(true));
        $inserter->clearStats();
        static::assertEquals($expected, $inserter->fetchStats(true));
    }
}
