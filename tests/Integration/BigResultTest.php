<?php

namespace Phlib\DbHelper\Tests\Integration;

use Phlib\Db\Adapter;
use Phlib\DbHelper\BigResult;
use Phlib\DbHelper\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

/**
 * @group integration
 */
class BigResultTest extends TestCase
{
    /**
     * @var Adapter
     */
    private $adapter;

    /**
     * @var string
     */
    private $schemaTable;

    /**
     * @var string
     */
    private $schemaTableQuoted;

    protected function setUp()
    {
        if ((bool)getenv('INTEGRATION_ENABLED') !== true) {
            static::markTestSkipped();
            return;
        }

        parent::setUp();

        $this->adapter = new Adapter([
            'host' => getenv('INTEGRATION_HOST'),
            'port' => getenv('INTEGRATION_PORT'),
            'username' => getenv('INTEGRATION_USERNAME'),
            'password' => getenv('INTEGRATION_PASSWORD'),
        ]);
    }

    protected function tearDown()
    {
        if (isset($this->schemaTableQuoted)) {
            $this->adapter->query("DROP TABLE {$this->schemaTableQuoted}");
        }
    }

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

    private function createTestTable()
    {
        $tableName = 'phlib_dbhelper_test_' . substr(sha1(uniqid()), 0, 10);
        $this->schemaTable = getenv('INTEGRATION_DATABASE') . '.' . $tableName;
        $this->schemaTableQuoted = '`' . getenv('INTEGRATION_DATABASE') . "`.`{$tableName}`";

        $sql = <<<SQL
CREATE TABLE {$this->schemaTableQuoted} (
  `test_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `char_col` varchar(255) DEFAULT NULL,
  `update_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`test_id`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii
SQL;

        $this->adapter->query($sql);
    }
}
