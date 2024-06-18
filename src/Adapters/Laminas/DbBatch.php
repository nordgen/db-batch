<?php

/**
 * DB-Batch a DB adapter library handling DB connection, queries, import and export.
 * Copyright (C) 2014  Nordic Genetic Resource Center (NordGen).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author    Kjell-Åke Lundblad <kjellake.lundblad@nordgen.org>
 * @copyright 2014- Nordic Genetic Resource Center (NordGen)
 * @license   https://github.com/nordgen/db-batch/blob/master/LICENSE.md AGPL-3.0 Licence
 * @package   nordgen\db-batch
 */

declare(strict_types=1);

namespace nordgen\DbBatch\Adapters\Laminas;

use Exception;
use Laminas\Db\Adapter\Adapter;
use Laminas\Db\Metadata\Object\ColumnObject;
use Laminas\Db\Metadata\Source\Factory;
use nordgen\DbBatch\DbBatchAbstract;
use nordgen\DbBatch\DbBatchInterface;

/**
 *
 * @mixin DbBatchImportTrait|DbBatchQueryTrait|DbBatchTransactionTrait
 */
class DbBatch extends DbBatchAbstract implements DbBatchInterface
{
    use DbBatchTransactionTrait;
    use DbBatchQueryTrait;
    use DbBatchImportTrait;

    const EXPECTED_CONNECTION_TYPE = 'Laminas\\Db\\Adapter\\Adapter';


    // Query Result section

    /**
     * @param string $name
     * @param mixed|null $db
     * @return string
     */
    public static function quoteIdentifierStatic(string $name, mixed $db = null): string
    {
        $ret = $name;
        if ($db instanceof Adapter) {
            $ret = $db->platform->quoteIdentifier($name);
        }
        return $ret;
    }

    /**
     * @param string $name
     * @param mixed|null $connection
     * @return string
     * @throws Exception
     */
    public static function formatParameterNameStatic(string $name, mixed $connection = null): string
    {
        $ret = $name;
        if ($connection instanceof Adapter) {
            $ret = $connection->driver->formatParameterName($name);
        }
        return $ret;
    }



    /**
     * @param string|null $tableName
     * @param string|array|null $fields
     * @return array
     * @throws Exception
     */
    public function getEmptyTableRecord(string $tableName = null, string|array|null $fields = null): array
    {
        $ret = [];
        if (is_string($fields)) {
            $fields = explode(',', $fields);
        }

        $metadata = Factory::createSourceFromAdapter($this->connection);
        $table = $metadata->getTable($tableName);
        foreach ($table->getColumns() as $columnObj) {
            if ($columnObj instanceof ColumnObject) {
                $columnName = $columnObj->getName();
                if (in_array($columnName, $fields)) {
                    $ret[$columnName] = $columnObj->isNullable() ? null :
                        ($columnObj->getDataType() == 'string' ? '' : 0);
                }
            }
        }

        return $ret;
    }

    /**
     * @param string $tableName
     * @param array $record
     * @return array
     * @throws Exception
     */
    public function convertTableRecordValuesToExpectedDataType(string $tableName, array $record): array
    {
        $ret = [];

        $keys = array_keys($record);
        $metadata = Factory::createSourceFromAdapter($this->connection);
        $table = $metadata->getTable($tableName);
        foreach ($table->getColumns() as $columnObj) {
            if ($columnObj instanceof ColumnObject) {
                $columnName = $columnObj->getName();
                if (in_array($columnName, $keys)) {
                    $value = $record[$columnName];
                    $expectedType = $columnObj->getDataType();
                    $newValue = static::convertValueToExpectedType($value, $expectedType);
                    $ret[$columnName] = $columnObj->isNullable() && empty($newValue) ? null : $newValue;
                }
            }
        }

        return $ret;
    }


    /**
     * @param string $tableName
     * @param string $fieldName
     * @param array $record
     * @return bool|float|int|string|null
     * @throws Exception
     */
    public function convertTableRecordValueToExpectedDataTypeByFieldName(string $tableName, string $fieldName, array $record): float|bool|int|string|null
    {
        $retval = false;

        $keys = array_keys($record);
        $metadata = Factory::createSourceFromAdapter($this->connection);
        //$metadata = new Metadata($this->db);

        $columnObj = $metadata->getColumn($fieldName, $tableName);
        $columnName = $fieldName;
        if (in_array($columnName, $keys)) {
            $value = $record[$columnName];
            $expectedType = $columnObj->getDataType();
            $newValue = self::convertValueToExpectedType($value, $expectedType);
            $retval = $columnObj->isNullable() && empty($newValue) ? null : $newValue;
        }

        return $retval;
    }



}