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
use nordgen\DbBatch\Helpers\ArrayHelper;
use nordgen\DbBatch\Helpers\StringTemplateHelper;

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

        $pk = isset($extraData ['pk']) ? $extraData ['pk'] : 'id';

        $noInsertOnEmptyRow = isset($extraData ['noInsertOnEmptyRow'])
            && $extraData ['noInsertOnEmptyRow'] === true;

        if ($noInsertOnEmptyRow && empty(array_filter($row))) {
            return true;
        }

        // Create empty recordset
        $sql = "SELECT * FROM $table WHERE $pk = -1";
        $rs = $this->connection->Execute($sql); // Execute the query and get the empty recordset
        $extraData ['rs'] = $rs;
        $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rowNum, $extraData);

        // Ignore row if it is false
        if (!!$rowToInsert) {
            $insertSQL = $this->connection->GetInsertSQL($rs, $rowToInsert);
            $result = $this->connection->Execute($insertSQL); // Insert the record into the database;
            if (!$result && $isThrowExceptionEnabled) {
                throw new Exception($this->connection->ErrorMsg());
            }
            return !!$result;
        } elseif ($noInsertOnEmptyRow) {
            return true;
        } elseif ($isThrowExceptionEnabled) {
            throw new Exception("Could not prepare an insert sql.");
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

        $pk = isset($extraData ['pk']) ? $extraData ['pk'] : 'id';
        // Create empty recordset
        $sql = "SELECT * FROM $table WHERE $pk = -1";
        $rs = $this->connection->Execute($sql); // Execute the query and get the empty recordset

        $extraData ['rs'] = $rs;

        $rowToUpdate = $this->getRowToInsert($rowUpdator, $row, $rowNum, $extraData);

        // parse $condition with $rowToUpdate context
        $templateData = [
            'fileRow' => &$row,
            'updateRow' => &$rowToUpdate,
            'extraData' => &$extraData
        ];

        if (isset($condition) && is_array($condition) && count($condition) > 0) {
            $callback = function ($value, $key) use ($row) {
                return "$key = $value";
            };
            $condition = implode(' and ', ArrayHelper::arrayKeyMap($callback, $condition));
        }

        // i.e. $condition = "accide = #extraData[accide]"; =>
        // $condition = "accide = 345345; =>

        $condition = StringTemplateHelper::template($condition, $templateData);

        // Select recordset to update
        $sql = "SELECT * FROM $table WHERE $condition";
        $rs = $this->connection->Execute($sql); // Execute the query and get selected recordset

        // Ignore row if it is false
        if (!!$rowToUpdate) {

            $updateSQL = $this->connection->GetUpdateSQL($rs, $rowToUpdate);
            $result = $this->connection->Execute($updateSQL); // Update the record in the database;
            if (!$result && $isThrowExceptionEnabled) {
                throw new Exception($this->connection->ErrorMsg());
            }

            return !!$result;

        } elseif ($isThrowExceptionEnabled) {
            throw new Exception("Could not prepare an insert sql.");
        }

        return false;
    }


}