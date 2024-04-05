<?php

declare(strict_types=1);

namespace Phlib\DbHelper\Tests;

use Phlib\Db\Adapter;
use Phlib\Db\Exception\RuntimeException as DbRuntimeException;
use Phlib\Db\SqlFragment;
use Phlib\DbHelper\BulkInsert;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

/**
 * BulkInsert Test
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BulkInsertTest extends TestCase
{
    /**
     * @var Adapter|MockObject
     */
    private MockObject $adapter;

    protected function setUp(): void
    {
        $this->adapter = $this->createMock(Adapter::class);

        $quoteHandler = new Adapter\QuoteHandler(function ($value): string {
            return "`{$value}`";
        });
        $this->adapter->method('quote')
            ->willReturn($quoteHandler);

        parent::setUp();
    }

    /**
     * @dataProvider dataSqlQuotes
     */
    public function testSqlQuotes(string|int|float $value, string $expected): void
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

    public function dataSqlQuotes(): array
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

    public function testSqlHandlesMultipleFieldsAndValues(): void
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

        $insert = [];
        $duplicate = [];
        foreach ($fields as $field) {
            $insert[] = "`{$field}`";
            $duplicate[] = "`{$field}` = VALUES(`{$field}`)";
        }
        $expectedSql = "INSERT INTO `{$table}` (" .
            implode(', ', $insert) . ') VALUES (' .
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
     * @param mixed $updateValue
     */
    public function testSqlUpdateExpression(string|int|SqlFragment $updateValue, string $expected): void
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

        $insert = [];
        foreach ($fields as $field) {
            $insert[] = "`{$field}`";
        }
        $expectedSql = "INSERT INTO `{$table}` (" .
            implode(', ', $insert) . ') VALUES (' .
            implode(', ', $values) . ') ' .
            'ON DUPLICATE KEY UPDATE ' .
            "`{$fields[0]}` = VALUES(`{$fields[0]}`), " .
            "`{$fields[1]}` = {$expected}";

        $inserter = new BulkInsert($this->adapter, $table, $fields, $updateFields);

        $this->adapter->expects(static::once())
            ->method('execute')
            ->with($expectedSql);

        $inserter
            ->add($values)
            ->write();
    }

    public function dataSqlUpdateExpression(): array
    {
        $int = rand();
        $string = sha1(uniqid());
        $sql = new SqlFragment('Some SQL expression');

        return [
            'int-not-quoted' => [$int, (string)$int],
            'string-quoted' => [$string, "`{$string}`"],
            'sql-as-given' => [$sql, (string)$sql],
        ];
    }

    /**
     * @dataProvider dataSqlIgnoreUpdate
     */
    public function testSqlIgnoreUpdate(bool $ignore, bool $update): void
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

        $sqlTest = function (string $actual) use ($ignore, $update): bool {
            $needle = 'INSERT IGNORE INTO';
            if ($ignore && !$update) {
                static::assertStringContainsString($needle, $actual);
            } else {
                static::assertStringNotContainsString($needle, $actual);
            }

            $needle = 'ON DUPLICATE KEY UPDATE';
            if ($update) {
                static::assertStringContainsString($needle, $actual);
            } else {
                static::assertStringNotContainsString($needle, $actual);
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

    public function dataSqlIgnoreUpdate(): array
    {
        return [
            [true, true],
            [true, false],
            [false, true],
            [false, false],
        ];
    }

    public function testAddCallsWriteWhenExceedsBatchSize(): void
    {
        $inserter = new BulkInsert($this->adapter, 'table_name', ['field1'], [], batchSize: 1);

        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter->add([
            'field1' => 'foo',
        ]);
    }

    public function testWriteCallsAdapterExecute(): void
    {
        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add([
            'field1' => 'foo',
            'field2' => 'bar',
        ]);
        $inserter->write();
    }

    public function testWriteReturnsEarlyWhenNoRows(): void
    {
        $this->adapter->expects(static::never())
            ->method('execute');

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->write();
    }

    public function testWriteDoesNotWriteTheSameRows(): void
    {
        $this->adapter->expects(static::once())
            ->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add([
            'field1' => 'foo',
            'field2' => 'bar',
        ]);
        $inserter->write();
        $inserter->write();
    }

    public function testWriteDetectsDeadlockAndHandlesIt(): void
    {
        $this->adapter->expects(static::exactly(2))
            ->method('execute')
            ->will(static::onConsecutiveCalls(
                static::throwException(new DbRuntimeException('Deadlock found when trying to get lock')),
                static::returnValue(1)
            ));

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add([
            'field1' => 'foo',
            'field2' => 'bar',
        ]);
        $inserter->write();
    }

    public function testWriteAllowsNonDeadlockErrorsToBubble(): void
    {
        $this->expectException(DbRuntimeException::class);

        $this->adapter->method('execute')
            ->willThrowException(new DbRuntimeException('Some other foo exception'));

        $inserter = new BulkInsert($this->adapter, 'table', ['field1', 'field2']);
        $inserter->add([
            'field1' => 'foo',
            'field2' => 'bar',
        ]);
        $inserter->write();
    }

    public function testFetchStatsOnInitialConstructWithoutFlush(): void
    {
        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        $inserter = new BulkInsert($this->adapter, 'table', ['field1']);
        static::assertSame($expected, $inserter->fetchStats($flush = false));
    }

    public function testFetchStatsOnInitialConstructWithFlush(): void
    {
        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        $inserter = new BulkInsert($this->adapter, 'table', ['field1']);
        static::assertSame($expected, $inserter->fetchStats($flush = true));
    }

    /**
     * @dataProvider fetchStatsIncrementsDataProvider
     */
    public function testFetchStatsIncrements(
        int $expected,
        string $statistic,
        int $noOfInserts,
        int $noOfUpdates,
        bool $withFlush
    ): void {
        $affectedRows = ($noOfUpdates * 2) + $noOfInserts;
        $this->adapter->method('execute')
            ->willReturn($affectedRows);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1'], []);
        $totalRows = $noOfInserts + $noOfUpdates;
        for ($i = 0; $i < $totalRows; $i++) {
            $inserter->add([
                'field1' => 'foo',
            ]);
        }

        $stats = $inserter->fetchStats($withFlush);
        static::assertSame($expected, $stats[$statistic]);
    }

    public function fetchStatsIncrementsDataProvider(): array
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

    public function testClearStats(): void
    {
        $this->adapter->method('execute')
            ->willReturn(1);

        $inserter = new BulkInsert($this->adapter, 'table', ['field1'], []);
        $inserter->add([
            'field1' => 'foo',
        ]);

        $expected = [
            'total' => 0,
            'inserted' => 0,
            'updated' => 0,
            'pending' => 0,
        ];
        static::assertNotSame($expected, $inserter->fetchStats(true));
        $inserter->clearStats();
        static::assertSame($expected, $inserter->fetchStats(true));
    }
}
