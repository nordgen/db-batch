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

namespace nordgen\DbBatch;


use ArrayObject;
use Box\Spout\Common\Exception\IOException;
use Box\Spout\Common\Exception\UnsupportedTypeException;
use Box\Spout\Reader\Exception\ReaderNotOpenedException;
use Box\Spout\Writer\Exception\WriterNotOpenedException;
use Box\Spout\Writer\WriterInterface;
use Closure;
use Exception;
use Iterator;
use Throwable;
use Traversable;
use Yiisoft\Db\Exception\InvalidCallException;
use Yiisoft\Db\Exception\InvalidConfigException;

/**
 *
 * @author Kjell-Åke Lundblad <kjellake.lundblad@nordgen.org>
 *
 */
interface DbBatchInterface
{
    /**
     *
     * @param mixed|null $connection
     * @return string
     */
    public function getConnectionType(mixed $connection = null): string;

    /**
     *
     * @param mixed|null $connection
     * @return string
     */
    public static function getConnectionTypeStatic(mixed $connection = null): string;

    /**
     *
     * @param string|null $connectionType
     * @return bool
     */
    public function validateConnectionType(string $connectionType = null): bool;

    /**
     *
     * @param string|null $connectionType
     * @return bool
     */
    public static function validateConnectionTypeStatic(string $connectionType = null): bool;


    /**
     * @return mixed
     */
    public function getConnection(): mixed;


    /**
     * @param mixed|null $connection
     * @return mixed
     */
    public static function convertConnection(mixed $connection = null): mixed;

    /**
     */
    public function startTrans(): void;

    /**
     * @return void
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function completeTrans(): void;

    /**
     * @return void
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     * @throws InvalidConfigException
     */
    public function rollbackTrans(): void;

    /**
     * @return void
     * @throws InvalidConfigException
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function failTrans(): void;





    // DB query section


    /**
     *
     * @param string $sql
     * @param array|null $parameters
     * @return false|int|null
     * @throws Exception
     * @throws Throwable
     */
    public function execute(string $sql, array $parameters = null): bool|int|null;

    /**
     *
     * @param string $sql
     * @param array $parameters
     * @return self
     * @throws Exception
     */
    public function query(string $sql, array $parameters = []): static;


    /**
     * Returns first row and first column
     *
     * @param string $sql
     * @param array $parameters
     * @return mixed|null
     */
    public function queryScalar(string $sql, array $parameters = []): mixed;

    /**
     * Returns an array of first column in each rows
     *
     * @param string $sql
     * @param array $parameters
     * @return bool|array|null
     */
    public function queryColumn(string $sql, array $parameters = []): bool|array|null;


    /**
     * Returns first row
     *
     * @param string $sql
     * @param array|null $parameters
     * @return bool|array|null
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function queryOne(string $sql, array $parameters = null): bool|array|null;

    /**
     * Returns all rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array[]|false
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function queryAll(string $sql, array $parameters = []): array|bool;



    // Query Result section

    /**
     *
     * @param string $sql
     * @return array|bool
     * @throws Exception
     */
    public function getQueryFieldNames(string $sql): array|bool;

    /**
     * @deprecated Since version 2.1.0 use only getQueryFieldNamesFromQueryResult() instead.
     *
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResultSet(): array|bool;

    /**
     * @since version 2.1.0 use only this method instead of getQueryFieldNamesFromQueryResultSet().
     *
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResult(): array|bool;

    /**
     * @return mixed|null
     */
    public function getQueryResult(): mixed;

    /**
     * @param callable|null $callback $callback
     * @param array $opt
     */
    public function iterateQueryResultWithCallback(callable $callback = null, array &$opt = []): void;

    /**
     * @param callable $callback
     * @param $extraData
     * @return array|false
     * @throws Exception
     */
    public function mapDbResult(callable $callback, &$extraData): bool|array;

    /**
     * @param null $limit
     * @param int $offset
     * @return array
     */
    public function pageNextDbResult($limit = null, int $offset = 0): array;



    /**
     * @deprecated Since version 2.1.0 use only getAllFromResult() instead.
     *
     * @return array|false
     */
    public function getAllFromResultSet(): bool|array;

    /**
     * @since version 2.1.0 use this method instead of deprecated getAllFromResultSet().
     *
     * @return array|false
     */
    public function getAllFromResult(): bool|array;

    /**
     * @deprecated Since version 2.1.0 use only getQueryResultSet() instead.
     *
     * @return bool|mixed|null
     */
    public function getQueryResultSet(): mixed;

    /**
     * @return int|null
     * @throws Throwable
     */
    public function getQueryRecordCount(): ?int;

    /**
     * @return null|int
     */
    public function getQueryRecordFieldsCount(): ?int;

    /**
     * @return mixed|null
     */
    public function rewindQueryRecord(): mixed;

    /**
     * @return mixed
     */
    public function nextQueryRecord(): mixed;

    /**
     * @return array|ArrayObject|null
     */
    public function currentQueryRecord(): ArrayObject|array|null;

    /**
     * @param string $attributes
     * @return string
     * @throws InvalidCallException
     */
    public function queryRecordSetToHtml(string $attributes = ''): string;

    /**
     * @param string $tableName
     * @param array $fieldsOrRecord
     * @param array|string $where
     * @return string
     */
    public function createParameterizedUpdateSqlString(string $tableName, array $fieldsOrRecord, array|string $where): string;

    /**
     * @param string $tableName
     * @param array $fieldsOrRecord
     * @param array|string $where
     * @param mixed|null $connection
     * @return string
     */
    public static function createParameterizedUpdateSqlStringStatic(
        string $tableName,
        array $fieldsOrRecord,
        array|string $where,
        mixed $connection = null
    ): string;

    /**
     * @param string $name
     * @return string
     */
    public function quoteIdentifier(string $name): string;

    /**
     * @param string $name
     * @return string
     */
    public function formatParameterName(string $name): string;

    /**
     * @param string $tableName
     * @param $fieldsOrRecord
     * @return string
     */
    public function createParameterizedInsertSqlString(string $tableName, $fieldsOrRecord): string;

    /**
     * @param string|null $tableName
     * @param string|array|null $fields
     * @return array
     * @throws Exception
     */
    public function getEmptyTableRecord(string $tableName = null, string|array|null $fields = null): array;

    /**
     * @param string $tableName
     * @param array $record
     * @return array
     * @throws Exception
     */
    public function convertTableRecordValuesToExpectedDataType(string $tableName, array $record): array;

    /**
     * @param string $tableName
     * @param string $fieldName
     * @param array $record
     * @return bool|float|int|string|null
     * @throws Exception
     */
    public function convertTableRecordValueToExpectedDataTypeByFieldName(
        string $tableName,
        string $fieldName,
        array $record
    ): float|bool|int|string|null;


    // DB import section

    /**
     * Populates given table in given database with data from file
     *
     * <p>NB $opt['extraData']['pk'] has to be set if 'id' is not primary key</p>
     *
     * @param string $filepath name to file to populate from
     * @param string $table table name
     * @param callable|null $rowPopulator closure to handle each row
     * @param array $opt
     * @param int|null $preferedSheet
     *
     * @return bool success
     * @throws Exception
     * @throws Throwable
     * @uses ADOConnection|\ADODB_postgres8|\ADODB_postgres9|\ADODB_mysqli|\yii\db\connection $this->db database connector
     *
     */
    public function populate(
        string $filepath,
        string $table = "",
        callable $rowPopulator = null,
        array &$opt = [],
        int $preferedSheet = null
    ): bool;

    /**
     * insertRowIntoTable
     * NB $extraData['pk'] has to be set if 'id' is not primary key
     * @param string $table
     * @param array $row
     * @param $rowNumber
     * @param callable|array $rowPopulator
     * @param array $extraData
     * @return bool
     * @throws Exception
     */
    public function insertRowIntoTable(
        string $table,
        array $row,
        $rowNumber,
        callable|array
        $rowPopulator,
        array &$extraData = []
    ): bool;

    /**
     *
     * @param callable|array $rowPopulator
     * @param array $row
     * @param string|int $rowNumber
     * @param array $extraData
     * @return mixed|Closure
     * @throws Exception
     */
    public function getRowToInsert(
        callable|array $rowPopulator,
        array $row,
        string|int $rowNumber,
        array &$extraData
    ): mixed;

    /**
     *
     * @param callable|array $rowPopulator
     * @param array $row
     * @param string|int $rowNumber
     * @param array $extraData
     * @return mixed|Closure
     * @throws Exception
     */
    public function processClosure(
        callable|array $rowPopulator,
        array $row,
        string|int $rowNumber,
        array &$extraData
    ): mixed;

    /**
     * Updates given table in given database with data from file
     *
     * <p>NB $opt['extraData']['pk'] has to be set if 'id' is not primary key</p>
     *
     * @param string $filepath name to file to update from
     * @param string $table tablename
     * @param callable|null $rowUpdator closure to handle each row
     * @param array $opt
     * @param int|null $preferedSheet
     *
     * @return bool success
     * @throws Exception|Throwable
     * @uses ADODB|Yiisoft\Db\Connection $this->db database connector
     *
     */
    public function update(
        string $filepath,
        string $table = "",
        callable $rowUpdator = null,
        array &$opt = [],
        int $preferedSheet = null
    ): bool;

    /**
     *
     * @param string $table
     * @param array $row
     * @param $rowNumber
     * @param callable|array $rowUpdator
     * @param null $condition
     * @param array $extraData
     * @return bool
     * @throws Exception
     */
    public function updateRowInTable(
        string $table,
        array $row,
        $rowNumber,
        callable|array $rowUpdator,
        $condition = null,
        array &$extraData = []
    ): bool;


    // Db export section

    /**
     * Populates given table in given database with data from file
     *
     * @param string $filepath
     *            name to file to populate from
     * @param string $table
     *            tablename
     * @param callable|null $rowPopulator
     *            clousre to handle each row
     * @param array $opt
     *
     * @throws Exception
     * @uses ADODB|yii\db\connection $this->db database connector
     *
     */
    public function export(
        string $filepath,
        string $table = "",
        callable $rowPopulator = null,
        array &$opt = []
    ): void;

    /**
     *
     * @param WriterInterface $writer
     * @param array $row
     * @param string|int|null $rowNumber
     * @param callable|array|null $rowPopulator
     * @param array $extraData
     * @throws IOException
     * @throws WriterNotOpenedException
     */
    public function insertRowIntoFile(
        WriterInterface $writer,
        array $row,
        string|int $rowNumber = null,
        callable|array $rowPopulator = null,
        array &$extraData = []
    ): void;

    /**
     *
     * @param string $filepath
     * @param callable $rowPopulator
     * @param array $opt
     * @param string|null $preferedSheet
     * @param array|null $result
     * @return array|null
     * @throws IOException
     * @throws ReaderNotOpenedException
     * @throws UnsupportedTypeException
     * @throws Exception
     */
    public function mapReader(
        string $filepath,
        callable $rowPopulator,
        array &$opt = [],
        ?string $preferedSheet = null,
        ?array &$result = null
    ): ?array;


    /**
     * @param callable $callback
     * @param $extraData
     * @throws Exception
     */
    public function walkDbResult(callable $callback, &$extraData): void;


    /**
     * Returns iterator of for a csv file
     * @deprecated since version 2.1.0 use self::getCsvRowIterator() instead.
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     */
    public static function getCsvRowIteratorStatic(string $filepath, array $opt = []): Traversable|Iterator|null;


    /**
     * Returns iterator of for a csv file
     * @since version 2.1.0 instead of self::getCsvRowIteratorStatic().
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     */
    public static function getCsvRowIterator(string $filepath, array $opt = []): Traversable|Iterator|null;



    /**
     * @param array $opt
     * @param array $filterFunctions
     * @param string $recordFieldValueKeyName
     * @return mixed
     * @throws Exception
     */
    public static function parseRecordValueFilters(
        array &$opt,
        array $filterFunctions = [],
        string $recordFieldValueKeyName = 'fieval'
    ): mixed;

}