<?php

declare(strict_types=1);

namespace Phlib\DbHelper\Tests\Integration;

use Phlib\Db\Adapter;
use PHPUnit\Framework\TestCase;

abstract class IntegrationTestCase extends TestCase
{
    protected Adapter $adapter;

    protected string $schemaTable;

    protected string $schemaTableQuoted;

    protected function setUp(): void
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
            'attributes' => [
                // @todo php81 Not required for phlib/db v3; can be removed when dropping support for phlib/db v2
                \PDO::ATTR_EMULATE_PREPARES => false,
            ],
        ]);
    }

    protected function tearDown(): void
    {
        if (isset($this->schemaTableQuoted)) {
            $this->adapter->query("DROP TABLE {$this->schemaTableQuoted}");
        }

        parent::tearDown();
    }

    final protected function createTestTable(): void
    {
        $tableName = 'phlib_dbhelper_test_' . substr(sha1(uniqid()), 0, 10);
        $this->schemaTable = getenv('INTEGRATION_DATABASE') . '.' . $tableName;
        $this->schemaTableQuoted = '`' . getenv('INTEGRATION_DATABASE') . "`.`{$tableName}`";

        $sql = <<<SQL
CREATE TABLE {$this->schemaTableQuoted} (
  `test_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `char_col` varchar(255) DEFAULT NULL,
  `int_col` tinyint(128) NOT NULL DEFAULT 0,
  `update_ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`test_id`)
) ENGINE=InnoDB DEFAULT CHARSET=ascii
SQL;

        $this->adapter->query($sql);
    }
}
