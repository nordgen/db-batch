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

use Throwable;
use Yiisoft\Db\Connection\ConnectionInterface;
use Yiisoft\Db\Exception\Exception as YiiException;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Exception\NotSupportedException;
use Yiisoft\Db\Transaction\TransactionInterface;

/**
 *
 */
trait DbBatchTransactionTrait
{
    /**
     * @var TransactionInterface|null
     */
    protected TransactionInterface|null $yiiTransaction = null;

    /**
     * @return void
     * @throws InvalidConfigException
     * @throws Throwable
     * @throws YiiException
     * @throws NotSupportedException
     */
    public function startTrans(): void
    {
        /**
         * @var ConnectionInterface $db
         */
        $db = $this->getConnection();
        $this->yiiTransaction = $db->beginTransaction();
    }

    /**
     * @return void
     * @throws Throwable
     * @throws YiiException
     */
    public function completeTrans(): void
    {
        $this->yiiTransaction->commit();
    }

    /**
     * @return void
     * @throws InvalidConfigException
     * @throws Throwable
     * @throws YiiException
     */
    public function rollbackTrans(): void
    {
        $this->yiiTransaction->rollback();
    }

    /**
     * @return void
     * @throws InvalidConfigException
     * @throws Throwable
     * @throws YiiException
     */
    public function failTrans(): void
    {
        $this->yiiTransaction->rollback();
    }
}