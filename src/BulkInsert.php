<?php

declare(strict_types=1);

namespace Phlib\DbHelper;

use Phlib\Db\Adapter;
use Phlib\Db\Exception\RuntimeException as DbRuntimeException;

/**
 * Used to insert large amounts of data into a single table in defined batch
 * sizes.
 *
 * @package Phlib\DbHelper
 * @licence LGPL-3.0
 */
class BulkInsert
{
    private array $insertFields;

    private array $updateFields;

    private bool $insertIgnore = false;

    private array $rows = [];

    private int $batchSize;

    private int $totalRows = 0;

    private int $totalInserted = 0;

    private int $totalUpdated = 0;

    /**
     * @param array{
     *     batchSize?: int, // Default 200
     * } $options
     */
    public function __construct(
        private Adapter $adapter,
        private string $table,
        array $insertFields,
        array $updateFields = [],
        array $options = []
    ) {
        $options = $options + [
            'batchSize' => 200,
        ];
        $this->batchSize = (int)$options['batchSize'];

        $this->setInsertFields($insertFields);
        $this->setUpdateFields($updateFields);
    }

    /**
     * Sets the insert fields for the bulk statement.
     */
    public function setInsertFields(array $fields): self
    {
        $this->insertFields = $fields;
        return $this;
    }

    /**
     * Sets the update fields for the bulk statement.
     */
    public function setUpdateFields(array $fields): self
    {
        $this->updateFields = [];
        if (count($fields) > 0) {
            $values = [];
            foreach ($fields as $key => $value) {
                if (is_int($key)) {
                    $quotedField = $this->adapter->quote()->identifier($value);
                    $values[] = "{$quotedField} = VALUES({$quotedField})";
                } else {
                    $quotedField = $this->adapter->quote()->identifier($key);
                    $values[] = $this->adapter->quote()->into("{$quotedField} = ?", $value);
                }
            }
            $this->updateFields = $values;
        }

        return $this;
    }

    /**
     * Adds a row to the bulk insert. Row should be an indexed array matching
     * the order of the fields given. If the magic number is reached then it'll
     * automatically write the changes to the database.
     */
    public function add(array $row): self
    {
        if (count($row) === count($this->insertFields)) {
            $this->rows[] = $row;
            if (count($this->rows) >= $this->batchSize) {
                $this->write();
            }
        }
        return $this;
    }

    /**
     * Writes the changes so far to the database.
     */
    public function write(): self
    {
        $rowCount = count($this->rows);
        if ($rowCount === 0) {
            return $this;
        }

        $sql = $this->fetchSql();
        do {
            try {
                $affectedRows = $this->adapter->execute($sql);
            } catch (DbRuntimeException $e) {
                if (stripos($e->getMessage(), 'Deadlock') === false) {
                    throw $e;
                }
                $affectedRows = false;
            }
        } while ($affectedRows === false);

        $this->rows = [];

        $updatedRows = $affectedRows - $rowCount;
        $this->totalRows += $rowCount;
        $this->totalInserted += $rowCount - $updatedRows;
        $this->totalUpdated += $updatedRows;

        return $this;
    }

    private function fetchSql(): string
    {
        // No need to check for non-zero row count.
        // This method is only called from write(), which has its own check for zero rows.
        $values = [];
        foreach ($this->rows as $row) {
            $quoted = array_map([$this->adapter->quote(), 'value'], $row);
            $values[] = '(' . implode(', ', $quoted) . ')';
        }
        $values = implode(', ', $values);

        // Build statement structure
        $insert = ['INSERT'];
        $update = '';
        if (!empty($this->updateFields)) {
            $update = 'ON DUPLICATE KEY UPDATE ' . implode(', ', $this->updateFields);
        } elseif ($this->insertIgnore === true) {
            $insert[] = 'IGNORE';
        }
        $insert[] = 'INTO ' . $this->adapter->quote()->identifier($this->table);
        $quotedInsert = array_map([$this->adapter->quote(), 'identifier'], $this->insertFields);
        $insert[] = '(' . implode(', ', $quotedInsert) . ') VALUES';

        return trim(implode(' ', $insert) . " {$values} {$update}");
    }

    /**
     * Gets statistics about bulk insert. If flush is true then it will clear
     * any rows still outstanding before returning the results.
     *
     * Return:
     * array(
     *     'total'    => 100,
     *     'inserted' => 50,
     *     'updated'  => 50,
     *     'pending'  => 0
     * )
     */
    public function fetchStats(bool $flush = true): array
    {
        if ($flush) {
            $this->write();
        }
        return [
            'total' => $this->totalRows,
            'inserted' => $this->totalInserted,
            'updated' => $this->totalUpdated,
            'pending' => count($this->rows),
        ];
    }

    /**
     * Clear the currently recorded statistics.
     */
    public function clearStats(): self
    {
        $this->totalRows = 0;
        $this->totalInserted = 0;
        $this->totalUpdated = 0;
        return $this;
    }

    public function insertIgnoreEnabled(): self
    {
        $this->insertIgnore = true;
        return $this;
    }

    public function insertIgnoreDisabled(): self
    {
        $this->insertIgnore = false;
        return $this;
    }
}
