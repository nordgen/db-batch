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

use nordgen\DbBatch\Models\QueryResult;
use Throwable;
use Yiisoft\Db\Exception\Exception as YiiException;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Query\BatchQueryResultInterface;

/**
 *
 */
trait DbBatchQueryTrait
{

    // DB query section

    /**
     *
     * @param string $sql
     * @param array|null $parameters
     * @return false|int|null
     * @throws YiiException
     * @throws Throwable
     */
    public function execute(string $sql, array $parameters = null): bool|int|null
    {
        $this->getConnection()->createCommand($sql)->execute();
        return false;
    }

    /**
     *
     * @param string $sql
     * @param array $parameters
     * @return self
     * @throws YiiException
     */
    public function query(string $sql, array $parameters = []): static
    {
        if ($this->connection instanceof ConnectionInterface) {
            $rs = $this->connection->createCommand($sql)->query();
            $this->queryResult = new QueryResult($rs);
        }

        return $this;
    }


    /**
     * Returns first row and first column
     *
     * @param string $sql
     * @param array $parameters
     * @return mixed|null
     */
    public function queryScalar(string $sql, array $parameters = []): mixed
    {
        return $this->connection->createCommand($sql)->queryScalar();
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
        if ($this->connection instanceof ConnectionInterface) {
            return $this->connection->createCommand($sql)->queryColumn();
        }
        return false;
    }


    /**
     * Returns first row
     *
     * @param string $sql
     * @param array|null $parameters
     * @return bool|array|null
     * @throws Throwable
     * @throws YiiException
     */
    public function queryOne(string $sql, array $parameters = null): bool|array|null
    {
        if ($this->connection instanceof ConnectionInterface) {
            return $this->connection->createCommand($sql)->queryOne();
        }
        return false;
    }

    /**
     * Returns all rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array[]|false
     * @throws Throwable
     * @throws YiiException
     */
    public function queryAll(string $sql, array $parameters = []): array|bool
    {
        if ($this->connection instanceof ConnectionInterface) {
            return $this->connection->createCommand($sql)->queryAll();
        }
        return false;
    }


    /**
     * @return int|null
     * @throws Throwable
     */
    public function getQueryRecordCount(): ?int
    {
        $ret = null;
        if ($this->queryResult instanceof BatchQueryResultInterface) {
            $ret = $this->queryResult->getQuery()->count();
        }
        return $ret;
    }

}