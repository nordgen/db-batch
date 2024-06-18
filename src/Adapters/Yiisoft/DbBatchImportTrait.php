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

use Exception;

/**
 *
 */
trait DbBatchImportTrait
{

    /**
     * insertRowIntoTable
     * NB $extraData['pk'] has to be set if 'id' is not primary key
     * @param string $table
     * @param array $row
     * @param $rowNum
     * @param callable|array $rowPopulator
     * @param array $extraData
     * @return bool
     * @throws Exception
     */
    public function insertRowIntoTable(
        string         $table,
        array          $row, $rowNum,
        callable|array $rowPopulator,
        array          &$extraData = []
    ): bool
    {
        $isThrowExceptionEnabled = isset($extraData ['isThrowExceptionEnabled'])
            && $extraData['isThrowExceptionEnabled'] === true;
        $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rowNum, $extraData);
        // Ignore row if it is false
        if (!!$rowToInsert) {
            $this->connection->createCommand()->insert($table, $rowToInsert);
            if ($isThrowExceptionEnabled) {
                return !!$this->connection->createCommand()->insert($table, $rowToInsert)->execute();
            } else {
                try {
                    return !!$this->connection->createCommand()->insert($table, $rowToInsert)->execute();
                } catch (Exception) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     *
     * @param string $table
     * @param array $row
     * @param $rowNum
     * @param callable|array $rowUpdator
     * @param null $condition
     * @param array $extraData
     * @return bool
     * @throws Exception
     */
    public function updateRowInTable(
        string         $table,
        array          $row,
                       $rowNum,
        callable|array $rowUpdator,
                       $condition = null,
        array          &$extraData = []
    ): bool
    {
        $isThrowExceptionEnabled = isset($extraData ['isThrowExceptionEnabled'])
            && $extraData ['isThrowExceptionEnabled'] === true;

        if ($isThrowExceptionEnabled && !!$condition) {
            throw new Exception("Update without condition.");
        }

        $rowToUpdate = $this->getRowToInsert($rowUpdator, $row, $rowNum, $extraData);
        // Ignore row if it is false
        if (!!$rowToUpdate) {

            if ($isThrowExceptionEnabled) {
                return !!$this->connection->createCommand()->update($table, $rowToUpdate)->execute();
            } else {
                try {
                    return !!$this->connection->createCommand()->update($table, $rowToUpdate)->execute();
                } catch (Exception) {
                    return false;
                }
            }

        }
        return false;
    }


}