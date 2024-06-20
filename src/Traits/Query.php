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

namespace nordgen\DbBatch\Traits;

use ArrayObject;
use Exception;
use nordgen\DbBatch\Helpers\ArrayHelper;
use ReflectionException;
use ReflectionFunction;
use ReflectionMethod;
use Throwable;
use nordgen\DbBatch\Models\QueryResult;
use Yiisoft\Db\Query\BatchQueryResultInterface;


/**
 *
 *
 */
trait Query
{

    /**
     * @param $value
     * @param $expectedType
     * @return bool|float|int|string
     */
    protected static function convertValueToExpectedType($value, $expectedType): float|bool|int|string
    {

        $type = gettype($value);
        if ($type === 'string') {
            $value = trim($value);
        }

        switch ($expectedType) {
            case 'boolean':
                if ($type !== 'boolean') {
                    $newValue = match (strtolower("$value")) {
                        '1', 'true', 't', 'on', 'yes', 'y' => true,
                        default => false,
                    };
                } else {
                    $newValue = $value;
                }
                break;
            case 'double':
                $newValue = is_numeric($value) ? floatval("$value") : $value;
                break;
            case 'integer':
                $newValue = is_numeric($value) ? intval("$value") : $value;
                break;

            default:
                $newValue = $value;

        }
        return $newValue;
    }

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
        $this->query($sql);

        return $this->getQueryFieldNamesFromQueryResultSet();
    }

    /**
     * @return null|int
     * @throws Throwable
     */
    public function getQueryRecordFieldsCount(): ?int
    {
        if ($this->queryResult instanceof QueryResult) {
            return $this->queryResult->fieldCount();
        }
        return null;
    }

    /**
     * @param callable|null $callback $callback
     * @param array $opt
     */
    public function iterateQueryResultWithCallback(callable $callback = null, array &$opt = []): void
    {
        $rs = $this->getQueryResult();

        if (!isset($callback)) {
            $callback = function ($currentRow, $opt = []) {
            };
        }

        if (!$rs) {
            return;
        }

        try {
            $noOfParams = self::getNumberOfParams($callback);
        } catch (Exception $e) {
            $noOfParams = 3;
        }

        switch ($noOfParams) {
            case 1:
                foreach ($rs as $currentRow) {
                    $callback($currentRow);
                }
                break;
            case 2:
                foreach ($rs as $currentRow) {
                    $callback($currentRow, $opt);
                }
                break;
            default:
                foreach ($rs as $currentRowNum => $currentRow) {
                    $callback($currentRow, $currentRowNum, $opt);
                }
        }
    }

    /**
     * @return mixed|null
     */
    public function getQueryResult(): mixed
    {
        return $this->queryResult;
    }

    /**
     * @param $callable
     * @return int
     * @throws ReflectionException
     */
    protected static function getNumberOfParams($callable): int
    {
        $CReflection = is_array($callable) ?
            new ReflectionMethod($callable[0], $callable[1]) :
            new ReflectionFunction($callable);
        return $CReflection->getNumberOfParameters();
    }

    /**
     * @since version 2.1.0 use this method instead of deprecated getAllFromResultSet().
     *
     * @return array|false
     * @throws Throwable
     */
    public function getAllFromResultSet(): bool|array
    {
        return $this->getAllFromResult();
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
     * @deprecated Since version 2.1.0 use only getQueryResultSet() instead.
     *
     * @return bool|mixed|null
     */
    public function getQueryResultSet(): mixed
    {
        return $this->getQueryResult();
    }


    /**
     * @since version 2.1.0 use only this method instead of getQueryFieldNamesFromQueryResultSet().
     *
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResultSet(): array|bool
    {
        return $this->getQueryFieldNamesFromQueryResult();
    }

    /**
     * @param callable $callback
     * @param $extraData
     * @return array|false
     * @throws Exception
     */
    public function mapDbResult(callable $callback, &$extraData): bool|array
    {
        // Do nothing if
        if (!isset ($this->queryResult) || !$this->queryResult) {
            return false;
        }
        $arr = [];
        foreach ($this->queryResult as $rownum => $row) {
            $arr [] = $this->processClosure($callback, $row, $rownum, $extraData);
        }
        return $arr;
    }

    /**
     * @param null $limit
     * @param int $offset
     * @return array
     */
    public function pageNextDbResult($limit = null, int $offset = 0): array
    {
        $result = [];
        foreach ($this->queryResult as $row) {
            if ($this->queryResult->key() < $offset) {
                continue;
            }
            if (isset ($limit) && $limit == 0) {
                break;
            }
            $result [] = $row;
            if (isset ($limit)) {
                $limit--;
            }
        }

        return $result;
    }

    /**
     * @return mixed|null
     * @throws Throwable
     */
    public function rewindQueryRecord(): mixed
    {
        if ($this->queryResult instanceof QueryResult) {
            $this->queryResult->rewind();
            return $this->queryResult->current();
        }
        return false;
    }

    /**
     * @return mixed
     * @throws Throwable
     */
    public function nextQueryRecord(): mixed
    {
        if ($this->queryResult instanceof QueryResult) {
            $this->queryResult->next();
            return $this->queryResult->current();
        }
        return false;
    }

    /**
     * @return array|ArrayObject|null
     */
    public function currentQueryRecord(): ArrayObject|array|null
    {
        if ($this->queryResult instanceof QueryResult) {
            return $this->queryResult->current();
        }
        return null;
    }

    /**
     * @param string $attributes
     * @return string
     */
    public function queryRecordSetToHtml(string $attributes = ''): string
    {
        $html = '';
        $attributes = trim($attributes);

        $rs = $this->getQueryResult();

        if (!isset($rs) || $rs->count() === 0) {
            return $html;
        }

        $html .= <<<HTML
<table $attributes>

HTML;

        // Header
        $rs->rewind();
        foreach ($rs as $r) {

            $html .= <<<HTML
    <thead>
        <tr>
HTML;

            foreach ($r as $col => $val) {
                $html .= <<<HTML
            <th>$col</th>
HTML;

            }

            $html .= <<<HTML
        </tr>
    </thead>
HTML;

            break;
        } // Header -- end
        // Body

        $html .= <<<HTML
    <tbody>
HTML;


        $rs->rewind();
        foreach ($rs as $r) {

            $html .= <<<HTML
        <tr>
HTML;

            foreach ($r as $val) {
                $html .= <<<HTML
            <td>$val</td>
HTML;

            }

            $html .= <<<HTML
        </tr>
HTML;

        } // Body -- end
        $html .= <<<HTML
    </tbody>
HTML;

        $html .= <<<HTML
</table>

HTML;

        return $html;
    }

    /**
     * @param string $tableName
     * @param $fieldsOrRecord
     * @param $where
     * @return string
     * @throws Exception
     */
    public function createParameterizedUpdateSqlString(string $tableName, $fieldsOrRecord, $where): string
    {
        return self::createParameterizedUpdateSqlStringStatic($tableName, $fieldsOrRecord, $where, $this->connection);
    }

    /**
     * @param string $tableName
     * @param array $fieldsOrRecord
     * @param array|string $where
     * @param mixed|null $connection
     * @return string
     * @throws Exception
     */
    public static function createParameterizedUpdateSqlStringStatic(
        string $tableName,
        array $fieldsOrRecord,
        array|string $where,
        mixed  $connection = null
    ): string
    {
        $keys = ArrayHelper::isAssoc($fieldsOrRecord) ? array_keys($fieldsOrRecord) : $fieldsOrRecord;
        $whereKeys = is_array($where)
            ? (ArrayHelper::isAssoc($where) ? array_keys($where) : $where)
            : $where;
        return "UPDATE $tableName SET "
            . implode(
                ', ',
                array_map(
                    function ($key) use ($connection) {
                        return self::quoteIdentifierAndFormatParameterNameSetPairStatic($key, $connection);
                    },
                    $keys
                )
            )
            . " WHERE "
            . (
                is_array($whereKeys)
                ? (
                    implode(
                        ' AND ',
                        array_map(
                            function ($key) use ($connection) {
                                return self::quoteIdentifierAndFormatParameterNameSetPairStatic($key, $connection);
                            },
                            $whereKeys
                        )
                    )
                )
                : "$where"
            );
    }

    /**
     * @throws Exception
     */
    public static function quoteIdentifierAndFormatParameterNameSetPairStatic(
        string $key,
        mixed $connection = null
    ): string {
        return static::quoteIdentifierStatic($key, $connection) . " = "
            . static::formatParameterNameStatic($key, $connection);
    }

    /**
     * @param string $name
     * @return string
     */
    public function quoteIdentifier(string $name): string
    {
        return static::quoteIdentifierStatic($name, $this->connection);
    }

    /**
     * @param string $name
     * @return string
     * @throws Exception
     */
    public function formatParameterName(string $name): string
    {
        return static::formatParameterNameStatic($name, $this->connection);
    }

    /**
     * @param string $tableName
     * @param $fieldsOrRecord
     * @return string
     * @throws Exception
     */
    public function createParameterizedInsertSqlString(string $tableName, $fieldsOrRecord): string
    {
        return self::createParameterizedInsertSqlStringStatic($tableName, $fieldsOrRecord, $this->connection);
    }

    /**
     * @param string $tableName
     * @param $fieldsOrRecord
     * @param mixed|null $connection
     * @return string
     * @throws Exception
     */
    public static function createParameterizedInsertSqlStringStatic(string $tableName, $fieldsOrRecord, mixed $connection = null): string
    {
        $keys = ArrayHelper::isAssoc($fieldsOrRecord) ? array_keys($fieldsOrRecord) : $fieldsOrRecord;
        $columns = implode(', ', array_map(
                function ($key) use ($connection) {
                    return static::quoteIdentifierStatic($key, $connection);
                },
                $keys
            )
        );
        $values = implode(', ', array_map(
                function ($key) use ($connection) {
                    return static::formatParameterNameStatic($key, $connection);
                },
                $keys
            )
        );
        return <<<SQL
INSERT INTO $tableName ($columns) VALUES ($values)
SQL;

    }

}