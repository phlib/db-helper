<?php

namespace Phlib\DbHelper\Tests;

use Phlib\Db\SqlFragment;
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
     * @dataProvider dataSqlQuotes
     * @param mixed $value
     * @param string $expected
     */
    public function testSqlQuotes($value, $expected)
    {
        $table = 'test_table';
        $insertFields = ['field'];
        $updateFields = ['field'];
        $inserter = new BulkInsert($this->adapter, $table, $insertFields, $updateFields);

        $expectedSql = "VALUES ({$expected})";
        $this->adapter->expects(static::once())
            ->method('execute')
            ->with(static::stringContains($expectedSql));

        $inserter
            ->add([$value])
            ->write();
    }

    public function dataSqlQuotes()
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

    public function testSqlHandlesMultipleFieldsAndValues()
    {
        $table = sha1(uniqid());
        $fields = [
            sha1(uniqid()),
            sha1(uniqid()),
            sha1(uniqid()),
        ];
        $values1 = [
            rand(),
            rand(),
            rand(),
        ];
        $values2 = [
            rand(),
            rand(),
            rand(),
        ];

        $duplicate = [];
        foreach ($fields as $field) {
            $duplicate[] = $field . " = VALUES({$field})";
        }
        $expectedSql = "INSERT INTO {$table} (" .
            implode(', ', $fields) . ') VALUES (' .
            implode(', ', $values1) . '), (' .
            implode(', ', $values2) . ') ' .
            'ON DUPLICATE KEY UPDATE ' .
            implode(', ', $duplicate);

        $inserter = new BulkInsert($this->adapter, $table, $fields, $fields);

        $this->adapter->expects(static::once())
            ->method('execute')
            ->with($expectedSql);

        $inserter
            ->add($values1)
            ->add($values2)
            ->write();
    }

    /**
     * @dataProvider dataSqlUpdateExpression
     */
    public function testSqlUpdateExpression($updateValue, $expected)
    {
        $table = sha1(uniqid());
        $fields = [
            sha1(uniqid()),
            sha1(uniqid()),
        ];
        $updateFields = [
            $fields[0],
            $fields[1] => $updateValue,
        ];
        $values = [
            rand(),
            rand(),
        ];

        $expectedSql = "INSERT INTO {$table} (" .
            implode(', ', $fields) . ') VALUES (' .
            implode(', ', $values) . ') ' .
            'ON DUPLICATE KEY UPDATE ' .
            $fields[0] . " = VALUES({$fields[0]}), " .
            $fields[1] . " = {$expected}";

        $inserter = new BulkInsert($this->adapter, $table, $fields, $updateFields);

        $this->adapter->expects(static::once())
            ->method('execute')
            ->with($expectedSql);

        $inserter
            ->add($values)
            ->write();
    }

    public function dataSqlUpdateExpression()
    {
        $int = rand();
        $string = sha1(uniqid());
        $sql = new SqlFragment('Some SQL expression');

        return [
            'int-not-quoted' => [$int, $int],
            'string-quoted' => [$string, "`{$string}`"],
            'sql-as-given' => [$sql, (string)$sql],
        ];
    }

    /**
     * @dataProvider dataSqlIgnoreUpdate
     * @param bool $ignore
     * @param bool $update
     */
    public function testSqlIgnoreUpdate($ignore, $update)
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

        $sqlTest = function ($actual) use ($ignore, $update) {
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

            return true;
        };

        $this->adapter->expects(static::once())
            ->method('execute')
            ->with(static::callback($sqlTest));

        $inserter
            ->add(['value'])
            ->write();
    }

    public function dataSqlIgnoreUpdate()
    {
        return [
            [true, true],
            [true, false],
            [false, true],
            [false, false]
        ];
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
