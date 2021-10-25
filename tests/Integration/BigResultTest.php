<?php

namespace Phlib\DbHelper\Tests\Integration;

use Phlib\DbHelper\BigResult;
use Phlib\DbHelper\Exception\InvalidArgumentException;

/**
 * @group integration
 */
class BigResultTest extends IntegrationTestCase
{
    /**
     * @return array
     */
    public function dataExecuteAsInstance()
    {
        return [
            'instance' => [true],
            'static' => [false],
        ];
    }

    /**
     * @dataProvider dataExecuteAsInstance
     * @param bool $executeAsInstance
     */
    public function testBasicSelect($executeAsInstance)
    {
        $this->createTestTable();
        $id = rand();
        $text = sha1(uniqid());

        $insertSql = <<<SQL
INSERT INTO {$this->schemaTableQuoted} (
    test_id, char_col
) VALUES (
    {$id}, "{$text}"
)
SQL;
        $insertCount = $this->adapter->execute($insertSql);
        static::assertSame(1, $insertCount);

        $selectSql = <<<SQL
SELECT char_col
FROM {$this->schemaTableQuoted}
WHERE test_id = {$id}
SQL;

        if ($executeAsInstance) {
            $bigResult = new BigResult($this->adapter);
            $stmt = $bigResult->query($selectSql);
        } else {
            $stmt = BigResult::execute($this->adapter, $selectSql);
        }

        static::assertSame($text, $stmt->fetchColumn());

        $deleteSql = <<<SQL
DELETE FROM {$this->schemaTableQuoted}
WHERE test_id = {$id}
SQL;
        $deleteCount = $this->adapter->execute($deleteSql);
        static::assertSame(1, $deleteCount);
    }

    /**
     * @dataProvider dataExecuteAsInstance
     * @param bool $executeAsInstance
     */
    public function testInspectedRowsExceeded($executeAsInstance)
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Number of rows inspected exceeds '5'");

        $inspectedRowLimit = 5;
        $totalNumberOfRows = 10;

        $this->createTestTable();

        $values = [];
        for ($i = 1; $i <= $totalNumberOfRows; $i++) {
            $values[] = '(' . $i . ',"' . sha1(uniqid()) . '")';
        }
        $valuesSql = implode(', ', $values);

        $insertSql = <<<SQL
INSERT INTO {$this->schemaTableQuoted} (
    test_id, char_col
) VALUES {$valuesSql}
SQL;
        $insertCount = $this->adapter->execute($insertSql);
        static::assertSame($totalNumberOfRows, $insertCount);

        $selectSql = <<<SQL
SELECT char_col
FROM {$this->schemaTableQuoted}
SQL;
        try {
            if ($executeAsInstance) {
                $bigResult = new BigResult($this->adapter);
                $bigResult->query($selectSql, [], $inspectedRowLimit);
            } else {
                BigResult::execute($this->adapter, $selectSql, [], $inspectedRowLimit);
            }
        } finally {
            // Clean-up test rows
            $deleteSql = <<<SQL
DELETE FROM {$this->schemaTableQuoted}
SQL;
            $this->adapter->execute($deleteSql);
        }
    }
}
