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
use nordgen\DbBatch\Models\QueryResult;
use Throwable;

trait DbBatchQueryTrait
{

    // DB query section

    /**
     *
     * @param string $sql
     * @param array $parameters
     * @return self
     * @throws Exception
     */
    public function query(string $sql, array $parameters = []): static
    {
        $rs = $this->connection->Execute($sql); // Execute the query and get the empty recordset
        if (!$rs) {
            throw new Exception ("Adodb error " . $this->connection->ErrorNo() . ": " . $this->connection->ErrorMsg());
        }
        $this->queryResult = new QueryResult($rs);
        return $this;
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
        $rs = $this->connection->Execute($sql); // Execute the query and get the empty recordset
        if (!$rs) {
            throw new Exception ("Adodb error " . $this->connection->ErrorNo() . ": "
                . $this->connection->ErrorMsg());
        }
        return null;
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
        return $this->connection->GetOne($sql); // Execute the query
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
        return $this->connection->GetCol($sql); // Execute the query
    }


    /**
     * Returns first row
     *
     * @param string $sql
     * @param array|null $parameters
     * @return bool|array|null
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function queryOne(string $sql, array $parameters = null): bool|array|null
    {
        return $this->connection->GetRow($sql); // Execute the query
    }

    /**
     * Returns all rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array[]|false
     * @throws Throwable
     * @throws \Yiisoft\Db\Exception\Exception
     */
    public function queryAll(string $sql, array $parameters = []): array|bool
    {
        return $this->connection->GetAll($sql); // Execute the query
    }


    /**
     * @return int|null
     * @throws Throwable
     */
    public function getQueryRecordCount(): ?int
    {
        return $this?->queryResult->RecordCount();
    }

}