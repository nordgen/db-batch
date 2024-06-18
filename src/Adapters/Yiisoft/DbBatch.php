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

namespace nordgen\DbBatch\Adapters\Yiisoft;

use ArrayObject;
use Exception;
use nordgen\DbBatch\DbBatchAbstract;
use nordgen\DbBatch\DbBatchInterface;
use Throwable;
use Yiisoft\Db\Query\BatchQueryResultInterface;
use Yiisoft\Db\Query\Data\DataReader;


/**
 *
 * @mixin DbBatchImportTrait|DbBatchQueryTrait|DbBatchTransactionTrait
 */
class DbBatch extends DbBatchAbstract implements DbBatchInterface
{
    use DbBatchTransactionTrait;
    use DbBatchQueryTrait;
    use DbBatchImportTrait;

    const EXPECTED_CONNECTION_TYPE = 'Yiisoft\\Db\\Connection';


    // Query Result section

    public static function quoteIdentifierStatic(string $name, mixed $db = null): string
    {
        $ret = $name;
        // TODO: Implement...
        return $ret;
    }

    /**
     * @param string $name
     * @param mixed|null $connection
     * @return string
     */
    public static function formatParameterNameStatic(string $name, mixed $connection = null): string
    {
        $ret = $name;
        // TODO: Implement...

        return $ret;
    }

    /**
     * @return array|false
     * @throws Throwable
     */
    public function getAllFromResult(): bool|array
    {
        $rs = $this->getQueryResult();
        if ($rs instanceof BatchQueryResultInterface) {
            return $rs->getQuery()->all();
        }
        return false;
    }

    /**
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResult(): array|bool
    {
        $rs = $this->getQueryResult();
        $rsArr = [];
        if (isset($rs) && $rs instanceof DataReader && $rs->valid()) {
            // Get Field Names:
            //$rs->rewind();
            $r = $rs->current();
            $rsArr = array_keys($r instanceof ArrayObject ? $r->getArrayCopy() : $r);
        }
        return $rsArr;
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

        // TODO: Implement...

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

        // TODO: Implement...

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

        // TODO: Implement...

        return $retval;
    }


}