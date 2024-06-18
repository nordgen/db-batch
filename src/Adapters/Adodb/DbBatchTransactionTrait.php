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

/**
 *
 */
trait DbBatchTransactionTrait
{

    /**
     */
    public function startTrans(): void
    {
        $this->connection->StartTrans();
    }

    /**
     * @return void
     */
    public function rollbackTrans(): void
    {
        $this->connection->FailTrans();
        $this->connection->CompleteTrans();
    }

    /**
     * @return void
     */
    public function failTrans(): void
    {
        $this->connection->FailTrans();
    }

    /**
     * @return void
     */
    public function completeTrans(): void
    {
        $this->connection->CompleteTrans();
    }

}