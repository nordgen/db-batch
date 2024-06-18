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

namespace nordgen\DbBatch\Adapters\Adodb;

use Exception;
use nordgen\DbBatch\DbBatchAbstract;
use nordgen\DbBatch\DbBatchInterface;

/**
 *
 * @mixin DbBatchImportTrait|DbBatchQueryTrait|DbBatchTransactionTrait|DbBatchConnectionTrait
 */
class DbBatch extends DbBatchAbstract implements DbBatchInterface
{
    use DbBatchConnectionTrait;
    use DbBatchTransactionTrait;
    use DbBatchQueryTrait;
    use DbBatchImportTrait;

    /**
     *
     */
    const EXPECTED_CONNECTION_TYPE = 'ADODB';


    /**
     * Constructor
     *
     * @param mixed|null $connection
     * @throws Exception
     */
    public function __construct(mixed $connection = null)
    {
        parent::__construct($connection);
    }


    // Query Result section

    /**
     * @param string $name
     * @param mixed|null $db
     * @return string
     */
    public static function quoteIdentifierStatic(string $name, mixed $db = null): string
    {
        return $name;
    }

    /**
     * @param string $name
     * @param mixed|null $connection
     * @return string
     */
    public static function formatParameterNameStatic(string $name, mixed $connection = null): string
    {
        return $name;
    }

    /*
        public function iterateQueryResultWithCallback(callable $callback = null, array &$opt = []): void
        {
            $rs = $this->getQueryResult();
            if (!isset($callback)) {
                $callback = function ($currentrow, $opt = []) {
                };
            }

            if (!$rs) {
                return;
            }

            while (!$rs->EOF) {
                $callback($rs->fields, $opt);
                $rs->MoveNext();
            }
        }
    */

    /**
     *
     * @param string $sql
     * @return array|bool
     * @throws Exception
     */
    public function getQueryFieldNames(string $sql): array|bool
    {
        if (preg_match("/^(.+);\s*$/", $sql, $matches)) {
            $sql = $matches[1];
        }

        $sql = "$sql Limit 1;";

        $rs = $this->connection->Execute($sql); // Execute the query and get the empty recordset
        if (!$rs) {
            throw new Exception("Adodb error " . $this->connection->ErrorNo() . ": " . $this->connection->ErrorMsg());
        }

        # Get Field Names:
        $aRet = [];
        $lngCountFields = 0;
        if (!$rs->EOF) {
            for ($i = 0; $i < $rs->FieldCount(); $i++) {
                $fld = $rs->FetchField($i);
                $aRet[$lngCountFields] = $fld->name;
                $lngCountFields++;
            }
        }
        return $aRet;
    }

    /**
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResult(): array|bool
    {
        $rs = $this->getQueryResult();
        // Get Field Names:
        $aRet = [];
        $lngCountFields = 0;
        if (!$rs->EOF) {
            for ($i = 0; $i < $rs->FieldCount(); $i++) {
                $fld = $rs->FetchField($i);
                $aRet[$lngCountFields] = $fld->name;
                $lngCountFields++;
            }
        }
        return $aRet;
    }

    /**
     * @return array|false
     */
    public function getAllFromResult(): bool|array
    {
        try {
            return $this->getQueryResult()->toArray(); // Execute the query
        } catch (Exception) {
            return false;
        }
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

        $metaColumns = $this->connection->metaColumns($tableName, false);
        $metaColumnsFiltered = array_filter($metaColumns, function ($k) use ($fields) {
            return array_key_exists($k, $fields);
        });

        foreach ($metaColumnsFiltered as $key => $value) {
            $ret[$key] = array_key_exists('not_null', $value) && !$value['not_null'] ? null :
                (array_key_exists('type', $value) && in_array($this->connection->metaType($value['type']), ['C', 'C2', 'X', 'X2', 'XL']) ? '' : null);
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


        $metaColumns = $this->connection->metaColumns($tableName, false);
        $metaColumnsFiltered = array_filter($metaColumns, function ($k) use ($record) {
            return array_key_exists($k, $record);
        });

        foreach ($metaColumnsFiltered as $key => $value) {
            $typeRaw = array_key_exists('type', $value) && $this->connection->metaType($value['type']) !== null
                ? $this->connection->metaType($value['type'])
                : null;

            $ret[$key] = self::convertValueToExpectedType($value, self::convertAdodbRawTypeToExpectedType($typeRaw));

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
        $metaColumns = $this->connection->metaColumns($tableName, false);
        $metaColumn = $metaColumns[$fieldName];
        $typeRaw = array_key_exists('type', $metaColumn) && $this->connection->metaType($metaColumn['type']) !== null
            ? $this->connection->metaType($metaColumn['type'])
            : null;

        return self::convertValueToExpectedType($metaColumn, self::convertAdodbRawTypeToExpectedType($typeRaw));
    }



    /**
     * @param string|null $typeRaw
     * @return string
     */
    protected static function convertAdodbRawTypeToExpectedType(?string $typeRaw): string
    {
        return match ($typeRaw) {
            'C', 'C2', 'X', 'X2', 'XL' => 'string',
            'B' => 'blob',
            'D' => 'date',
            'T' => 'timestamp',
            'L' => 'boolean',
            'I' => 'integer',
            'N' => 'double',
            default => 'unknown type',
        };
    }


}