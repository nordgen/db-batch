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

use ArrayObject;
use Exception;
use Laminas\Db\Adapter\Adapter;
use Laminas\Db\Adapter\Driver\ResultInterface;
use Laminas\Db\Adapter\Exception\InvalidQueryException;
use Laminas\Db\Adapter\StatementContainerInterface;
use Laminas\Db\ResultSet\ResultSet;
use nordgen\DbBatch\Models\QueryResult;
use Throwable;

trait DbBatchQueryTrait
{


    // DB query section


    /**
     * Returns first row and first column
     *
     * @param string $sql
     * @param array $parameters
     * @return mixed|null
     * @throws Throwable
     */
    public function queryScalar(string $sql, array $parameters = []): mixed
    {
        $row = $this->queryOne($sql, $parameters);
        return array_shift($row);
        /*
        $statement = $this->connection->createStatement($sql);
        $statement->prepare();
        $result = $statement->execute($parameters);


        if ($result instanceof ResultInterface && $result->isQueryResult() && $result->getAffectedRows() > 0) {
            $resultSet = new ResultSet;
            $resultSet->initialize($result);
            $row = $resultSet->current();
            if (!(isset($row) && $row->count() > 0)) {
                return null;
            }
            $rowArr = $row->getArrayCopy();
            return array_shift($rowArr);
        }
        return null;
        */
    }

    /**
     * Returns first row
     *
     * @param string $sql
     * @param array|null $parameters
     * @return bool|array|null
     * @throws Throwable
     */
    public function queryOne(string $sql, array $parameters = null): bool|array|null
    {
        $statement = $this->connection->createStatement($sql);
        $statement->prepare();
        $parameters = ($parameters === null) ? [] : $parameters;
        $result = $statement->execute($parameters);

        if ($result instanceof ResultInterface && $result->isQueryResult()) {
            $resultSet = new ResultSet;
            $resultSet->initialize($result);

            return $resultSet->current()?->getArrayCopy();

        }
        return false;
    }

    /**
     *
     * @param string $sql
     * @param array|null $parameters
     * @return false|int|null
     * @throws Exception
     * @throws Throwable
     */
    public function execute(string $sql, array $parameters = null): bool|int|null
    {
        /**
         * @var Adapter $this
         */
        $parameters = ($parameters === null) ? Adapter::QUERY_MODE_EXECUTE : $parameters;
        if (is_array($parameters)) {
            try {
                $statement = $this->connection->createStatement($sql);
                if ($statement instanceof StatementContainerInterface) {

                    $statement->prepare();
                    $result = $statement->execute($parameters);

                    $preparedSql = $statement->getSql();
                    $preparedParams = $statement->getParameterContainer();


                    if (!(isset($result) && $result instanceof ResultInterface && (
                            $result->valid()
                            || $result->count() > 0
                            || in_array('getAffectedRows', get_class_methods($result))
                            && $result->getAffectedRows() > 0)
                    )) {
                        throw new Exception ("Db query failed");
                    } else {
                        return in_array('getAffectedRows', get_class_methods($result))
                            ? $result->getAffectedRows() : null;
                    }
                } else {
                    throw new Exception (
                        "Adapters statement is not implementing StatementContainerInterface"
                    );
                }


            } catch (InvalidQueryException $e) {
                throw new Exception ("Db query failed: " . $e->getMessage());
            }

        } elseif ($parameters === Adapter::QUERY_MODE_EXECUTE) {
            try {
                $result = $this->connection->query($sql, $parameters);
                return ($result instanceof ResultInterface)
                    ? $result->getAffectedRows()
                    : null;

            } catch (InvalidQueryException $e) {
                throw new Exception ("Db query failed: " . $e->getMessage());
            }

        }
        return false;
    }

    /**
     *
     * @param string $sql
     * @param array $parameters
     * @return self
     * @throws Exception
     */
    public function query(string $sql, array $parameters = []): static
    {
        $parameters = ($parameters === null) ? Adapter::QUERY_MODE_EXECUTE : $parameters;
        if (is_array($parameters)) {
            $statement = $this->connection->createStatement($sql);
            $statement->prepare();
            $result = $statement->execute($parameters);
            if ($result instanceof ResultInterface && $result->isQueryResult()) {
                $resultSet = new ResultSet;
                $resultSet->initialize($result);
                //$this->queryResult = $result;
                //$this->queryResultSet = $resultSet;
                $this->queryResult = new QueryResult($resultSet);
            } else {
                throw new Exception ("Db query failed");
            }
        } elseif ($parameters === Adapter::QUERY_MODE_EXECUTE) {
            $result = $this->connection->query($sql, $parameters);
            if (!(isset($result) && ($result->valid() || $result->count() > 0 /*|| $result->getAffectedRows()>0*/))) {
                throw new Exception ("Db query failed");
            } else {
                //$this->queryResultSet = $result;
                $this->queryResult = new QueryResult($result);
            }
        }
        return $this;
    }

    /**
     * Returns an array of first column in each rows
     *
     * @param string $sql
     * @param array $parameters
     * @return bool|array|null
     */
    public function queryColumn(string $sql, array $parameters = []): bool|array|null
    {
        $queryResult = $this->query($sql, $parameters)->getQueryResult();
        if (!$queryResult) {
            throw new Exception ("Db query failed");
        }
        codecept_debug($queryResult);
        $firstcolumn = [];
        if ($queryResult instanceof QueryResult && $queryResult->valid()) {
            /** @var ArrayObject $item */
            foreach ($queryResult as $itemObj) {
                $item = $itemObj->getArrayCopy();
                $firstcolumn[] = array_shift($item);
            }
            return $firstcolumn;
        }
        return false;
        /*
        $statement = $this->connection->createStatement($sql);
        $statement->prepare();
        $result = $statement->execute($parameters);

        if ($result instanceof ResultInterface && $result->isQueryResult()) {
            $resultSet = new ResultSet;
            $resultSet->initialize($result);

            $firstcolumn = [];

            if ($resultSet->getReturnType() === 'arrayobject') {
                foreach ($resultSet->toArray() as $item) {
                    $firstcolumn[] = array_shift($item);
                }
                return $firstcolumn;
            }
        }
        return null;
        */
    }

    /**
     * Returns all rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array[]|false
     * @throws Throwable
     */
    public function queryAll(string $sql, array $parameters = []): array|bool
    {
        $statement = $this->connection->createStatement($sql);
        $statement->prepare();
        $result = $statement->execute($parameters);

        if ($result instanceof ResultInterface && $result->isQueryResult()) {
            $resultSet = new ResultSet;
            $resultSet->initialize($result);
            return $resultSet->toArray();
        }
        return false;
    }


    /**
     * @return int|null
     * @throws Throwable
     */
    public function getQueryRecordCount(): ?int
    {
        return $this?->queryResult->count();
    }


    /**
     * @return array|false
     * @throws Throwable
     */
    public function getAllFromResult(): bool|array
    {
        return $this->getQueryResult()->toArray();
    }


    /**
     * @return array|bool
     */
    public function getQueryFieldNamesFromQueryResult(): array|bool
    {
        $rs = $this->getQueryResult();
        $rsArr = [];
        if (isset($rs) && $rs instanceof ResultSet && $rs->valid()) {
            // Get Field Names:
            //$rs->rewind();
            $r = $rs->current();
            $rsArr = array_keys($r instanceof ArrayObject ? $r->getArrayCopy() : $r);
        }
        return $rsArr;
    }


}