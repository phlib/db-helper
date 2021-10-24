<?php

namespace Phlib\DbHelper\Tests\Integration;

use Phlib\Db\SqlFragment;
use Phlib\DbHelper\BulkInsert;

/**
 * @group integration
 */
class BulkInsertTest extends IntegrationTestCase
{
    public function testInsert()
    {
        $this->createTestTable();

        $fields = [
            'test_id',
            'char_col',
        ];

        // Integer bounds are used to make sure that keyed results stay in order for assertions
        $values1 = [
            rand(1, 1000),
            sha1(uniqid()),
        ];
        $values2 = [
            rand(2000, 5000),
            sha1(uniqid()),
        ];

        $inserter = new BulkInsert($this->adapter, $this->schemaTable, $fields, $fields);

        $inserter
            ->add($values1)
            ->add($values2)
            ->write();

        $expectedStats = [
            'total' => 2,
            'inserted' => 2,
            'updated' => 0,
            'pending' => 0,
        ];

        static::assertSame($expectedStats, $inserter->fetchStats());

        $expectedRows = [
            [
                'test_id' => (string)$values1[0],
                'char_col' => $values1[1],
            ],
            [
                'test_id' => (string)$values2[0],
                'char_col' => $values2[1],
            ],
        ];

        $selectSql = <<<SQL
SELECT test_id, char_col
FROM {$this->schemaTableQuoted}
SQL;
        $stmt = $this->adapter->query($selectSql);

        static::assertSame($expectedRows, $stmt->fetchAll());
    }

    public function testUpdate()
    {
        $this->createTestTable();

        $id = rand();
        $text1 = sha1(uniqid());
        $text2 = sha1(uniqid());

        $startData = [
            'test_id' => $id,
            'char_col' => $text1,
        ];
        $startExpected = [
            'test_id' => (string)$id,
            'char_col' => $text1,
        ];
        $updateData = [
            $id,
            $text2,
        ];
        $updateExpected = [
            'test_id' => (string)$id,
            'char_col' => $text2,
        ];

        $selectSql = <<<SQL
SELECT test_id, char_col
FROM {$this->schemaTableQuoted}
SQL;

        // Insert and verify starting value
        $this->adapter->insert($this->schemaTable, $startData);

        $startStmt = $this->adapter->query($selectSql);
        static::assertSame($startExpected, $startStmt->fetch());

        // Update the field using BulkInsert
        $fields = [
            'test_id',
            'char_col',
        ];
        $inserter = new BulkInsert($this->adapter, $this->schemaTable, $fields, $fields);

        $inserter
            ->add($updateData)
            ->write();

        $expectedStats = [
            'total' => 1,
            'inserted' => 0,
            'updated' => 1,
            'pending' => 0,
        ];

        static::assertSame($expectedStats, $inserter->fetchStats());

        $stmtAfterUpdate = $this->adapter->query($selectSql);
        static::assertSame($updateExpected, $stmtAfterUpdate->fetch());
    }

    public function testIgnore()
    {
        // Similar to testUpdate, but the value should NOT be changed
        $this->createTestTable();

        $id = rand();
        $text1 = sha1(uniqid());
        $text2 = sha1(uniqid());

        $startData = [
            'test_id' => $id,
            'char_col' => $text1,
        ];
        $startExpected = [
            'test_id' => (string)$id,
            'char_col' => $text1,
        ];
        $updateData = [
            $id,
            $text2,
        ];
        $updateExpected = [
            'test_id' => (string)$id,
            // Value should not be changed from initial value
            'char_col' => $text1,
        ];

        $selectSql = <<<SQL
SELECT test_id, char_col
FROM {$this->schemaTableQuoted}
SQL;

        // Insert and verify starting value
        $this->adapter->insert($this->schemaTable, $startData);

        $startStmt = $this->adapter->query($selectSql);
        static::assertSame($startExpected, $startStmt->fetch());

        // Update the field using BulkInsert
        $fields = [
            'test_id',
            'char_col',
        ];
        $inserter = new BulkInsert($this->adapter, $this->schemaTable, $fields);
        $inserter->insertIgnoreEnabled();

        $inserter
            ->add($updateData)
            ->write();

        // Stats don't look good when using IGNORE
        $expectedStats = [
            'total' => 1,
            'inserted' => 2,
            'updated' => -1,
            'pending' => 0,
        ];

        static::assertSame($expectedStats, $inserter->fetchStats());

        $stmtAfterUpdate = $this->adapter->query($selectSql);
        static::assertSame($updateExpected, $stmtAfterUpdate->fetch());
    }

    public function testUpdateExpressionValue()
    {
        // Similar to testUpdate, but the value is set by the update expression, not to the given row value
        $this->createTestTable();

        $id = rand();
        $text1 = sha1(uniqid());
        $text2 = sha1(uniqid());
        $text3 = sha1(uniqid());

        $startData = [
            'test_id' => $id,
            'char_col' => $text1,
        ];
        $startExpected = [
            'test_id' => (string)$id,
            'char_col' => $text1,
        ];
        $updateData = [
            $id,
            $text2,
        ];
        $updateExpected = [
            'test_id' => (string)$id,
            // Value should be set from the update expression
            'char_col' => $text3,
        ];

        $selectSql = <<<SQL
SELECT test_id, char_col
FROM {$this->schemaTableQuoted}
SQL;

        // Insert and verify starting value
        $this->adapter->insert($this->schemaTable, $startData);

        $startStmt = $this->adapter->query($selectSql);
        static::assertSame($startExpected, $startStmt->fetch());

        // Update the field using BulkInsert
        $fields = [
            'test_id',
            'char_col',
        ];
        $updateFields = [
            'char_col' => $text3,
        ];
        $inserter = new BulkInsert($this->adapter, $this->schemaTable, $fields, $updateFields);

        $inserter
            ->add($updateData)
            ->write();

        // Stats don't look good when using IGNORE
        $expectedStats = [
            'total' => 1,
            'inserted' => 0,
            'updated' => 1,
            'pending' => 0,
        ];

        static::assertSame($expectedStats, $inserter->fetchStats());

        $stmtAfterUpdate = $this->adapter->query($selectSql);
        static::assertSame($updateExpected, $stmtAfterUpdate->fetch());
    }

    public function testUpdateExpressionSql()
    {
        // Similar to testUpdate, but the value is set by the update expression, not to the given row value
        $this->createTestTable();

        $id = rand();
        $text1 = sha1(uniqid());
        $text2 = sha1(uniqid());
        $text3 = sha1(uniqid());

        $startData = [
            'test_id' => $id,
            'char_col' => $text1,
        ];
        $startExpected = [
            'test_id' => (string)$id,
            'char_col' => $text1,
        ];
        $updateData = [
            $id,
            $text2,
        ];
        $updateExpected = [
            'test_id' => (string)$id,
            // Value should be set from the update expression
            'char_col' => $text1 . $text3,
        ];

        $selectSql = <<<SQL
SELECT test_id, char_col
FROM {$this->schemaTableQuoted}
SQL;

        // Insert and verify starting value
        $this->adapter->insert($this->schemaTable, $startData);

        $startStmt = $this->adapter->query($selectSql);
        static::assertSame($startExpected, $startStmt->fetch());

        // Update the field using BulkInsert
        $fields = [
            'test_id',
            'char_col',
        ];
        $updateFields = [
            'char_col' => new SqlFragment("CONCAT(char_col, '{$text3}')"),
        ];
        $inserter = new BulkInsert($this->adapter, $this->schemaTable, $fields, $updateFields);

        $inserter
            ->add($updateData)
            ->write();

        // Stats don't look good when using IGNORE
        $expectedStats = [
            'total' => 1,
            'inserted' => 0,
            'updated' => 1,
            'pending' => 0,
        ];

        static::assertSame($expectedStats, $inserter->fetchStats());

        $stmtAfterUpdate = $this->adapter->query($selectSql);
        static::assertSame($updateExpected, $stmtAfterUpdate->fetch());
    }
}
