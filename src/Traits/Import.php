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

use Closure;
use Exception;
use Iterator;
use nordgen\DbBatch\Helpers\ArrayHelper;
use nordgen\DbBatch\Traits\CsvParser\CsvParser;
use Throwable;
use Traversable;

/**
 *
 */
trait Import
{

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
     * @param int|null $preferredSheet
     *
     * @return bool success
     * @throws Exception
     * @throws Throwable
     *
     */
    public function populate(
        string   $filepath,
        string   $table = "",
        callable $rowPopulator = null,
        array    &$opt = [],
        int      $preferredSheet = null
    ): bool
    {
        $opt ??= [];
        ArrayHelper::initiateArrayKeyIfNeeded($opt, 'extraData');
        $extraData = &$opt['extraData'];
        $beforeInsert = $this->getCallableByOptKey($opt, 'beforeInsert', function ($row, $rowNum, &$extraData) {
        });
        $afterInsert = $this->getCallableByOptKey($opt, 'afterInsert', function ($row, $rowNum, &$extraData) {
        });
        $ignoreSecondRow = $opt['ignoreSecondRow'] ?? false;
        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);
        $rowNum = 0;
        $successTotal = true;

        $this->startTrans();
        try {
            foreach ($sheetIterator as $sheet) {
                if (isset($preferredSheet) && $preferredSheet != $sheet->getName()) {
                    continue;
                }
                if ($sheet->getName() == $preferredSheet) {
                    $firstRow = true;
                    $secondRow = false;
                    $successTotal = true;
                    $head = [];
                    foreach ($sheet->getRowIterator() as $rawNow) {
                        if ($firstRow) {
                            $firstRow = false;
                            $secondRow = true;
                            $head = $rawNow;
                            $rowNum++;
                            continue;
                        }
                        if ($secondRow && $ignoreSecondRow) {
                            $secondRow = false;
                            $rowNum++;
                            continue;
                        }
                        $rowNum++;
                        $row = array_combine($head, $rawNow) ?: ArrayHelper::headRowArrayCombine($head, $rawNow);

                        if (isset ($beforeInsert) && is_callable($beforeInsert)) {
                            $beforeInsert($row, $rowNum, $extraData);
                        }

                        if (isset($rowPopulator) && is_callable($rowPopulator)) {
                            $success = $this->insertRowIntoTable($table, $row, $rowNum, $rowPopulator, $extraData);
                            $successTotal = $successTotal && !!$success;
                        }
                        if (isset($afterInsert) && is_callable($afterInsert)) {
                            $afterInsert ($row, $rowNum, $extraData);
                        }
                    }
                }
            }
            $this->completeTrans();
        } catch (Exception|Throwable $e) {
            $this->rollbackTrans();
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
            $opt ['extraData'] = $extraData;
        }

        return $successTotal;
    }

    /**
     * @param array $opt
     * @param string $key
     * @param callable $default
     * @return Closure
     */
    protected function getCallableByOptKey(array $opt, string $key, callable $default): Closure
    {
        return ArrayHelper::getArrayKeyCallableValue($opt, $key, $default, $this);
    }

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
    abstract public function insertRowIntoTable(
        string         $table,
        array          $row, $rowNum,
        callable|array $rowPopulator,
        array          &$extraData = []
    ): bool;

    /**
     * Updates given table in given database with data from file
     *
     * <p>NB $opt['extraData']['pk'] has to be set if 'id' is not primary key</p>
     *
     * @param string $filepath name to file to update from
     * @param string $table table name
     * @param callable|null $rowUpdator closure to handle each row
     * @param array $opt
     * @param int|null $preferredSheet
     *
     * @return bool success
     * @throws Exception|Throwable
     *
     */
    public function update(string $filepath, string $table = "", callable $rowUpdator = null, array &$opt = [], int $preferredSheet = null): bool
    {
        $opt ??= [];
        ArrayHelper::initiateArrayKeyIfNeeded($opt, 'extraData');
        $extraData = &$opt['extraData'];
        $beforeUpdate = $this->getCallableByOptKey($opt, 'beforeUpdate', function ($row, $rowNum, $extraData) {
        });
        $afterUpdate = $this->getCallableByOptKey($opt, 'afterUpdate', function ($row, $rowNum, $extraData) {
        });

        $ignoreSecondRow = $opt['ignoreSecondRow'] ?? false;
        $updateWhereCondition = $opt['updateWhereCondition'] ?: null;
        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);

        $rowNum = 0;

        $this->startTrans();

        try {
            $successTotal = false;
            foreach ($sheetIterator as $sheet) {
                if (isset($preferredSheet) && $preferredSheet != $sheet->getName()) {
                    continue;
                }
                $firstRow = true;
                $secondRow = false;
                $successTotal = true;
                $head = [];
                foreach ($sheet->getRowIterator() as $rawRow) {
                    if ($firstRow) {
                        $firstRow = false;
                        $secondRow = true;
                        $head = $rawRow;
                        $rowNum++;
                        continue;
                    }
                    if ($secondRow && $ignoreSecondRow) {
                        $secondRow = false;
                        $rowNum++;
                        continue;
                    }
                    $rowNum++;
                    $row = array_combine($head, $rawRow) ?: ArrayHelper::headRowArrayCombine($head, $rawRow);
                    if (isset ($beforeUpdate) && is_callable($beforeUpdate)) {
                        $beforeUpdate($row, $rowNum, $extraData);
                    }

                    if (isset ($rowUpdator) && is_callable($rowUpdator)) {
                        $success = $this->updateRowInTable($table, $row, $rowNum, $rowUpdator, $updateWhereCondition, $extraData);
                        $successTotal = $successTotal && !!$success;
                    }
                    if (isset ($afterUpdate) && is_callable($afterUpdate)) {
                        $afterUpdate($row, $rowNum, $extraData);
                    }
                }
            }

            $this->completeTrans();
        } catch (Exception $e) {
            $this->rollbackTrans();
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
            $opt ['extraData'] = $extraData;
        }

        return $successTotal;
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
    abstract public function updateRowInTable(
        string         $table,
        array          $row,
                       $rowNum,
        callable|array $rowUpdator,
                       $condition = null,
        array          &$extraData = []
    ): bool;

    /**
     *
     * @param callable|array $rowPopulator
     * @param array $row
     * @param string|int $rowNum
     * @param array $extraData
     * @return mixed|Closure
     * @throws Exception
     */
    public function getRowToInsert(
        callable|array $rowPopulator,
        array          $row,
        string|int     $rowNum,
        array          &$extraData
    ): mixed
    {
        $overrideRowWithKeyVals = $extraData['overideRowWithKeyVals'] ?? [];
        return $overrideRowWithKeyVals + $this->processClosure($rowPopulator, $row, $rowNum, $extraData);
    }

    /**
     *
     * @param callable|array $rowPopulator
     * @param array $row
     * @param string|int $rowNum
     * @param array $extraData
     * @return mixed|Closure
     * @throws Exception
     */
    public function processClosure(
        callable|array $rowPopulator,
        array          $row,
        string|int     $rowNum,
        array          &$extraData
    ): mixed
    {
        // TODO: Check if it is ok to remove $overrideRowWithKeyVals
        switch (gettype($rowPopulator)) {
            case 'object' :
                if (is_callable($rowPopulator)) {
                    if (method_exists($rowPopulator, 'bindTo')) {
                        $rowPopulator = $rowPopulator->bindTo($this);
                    }

                    return $rowPopulator($row, $rowNum, $extraData);
                }
                if ($rowPopulator instanceof Closure) {
                    $rowPopulator = $rowPopulator->bindTo($this);
                    return call_user_func_array($rowPopulator, [
                        $row,
                        $rowNum,
                        &$extraData
                    ]);
                }

                break;
            case 'array' :
            case 'string' :
                if (is_callable($rowPopulator)) {
                    return call_user_func_array($rowPopulator, [
                        $row,
                        $rowNum,
                        &$extraData
                    ]);
                } elseif (is_array($rowPopulator)) {
                    return $rowPopulator;
                }
                break;

            case 'NULL' :
                //return $overrideRowWithKeyVals + $row;
                return $row;

            default :
                throw new Exception ("RowPopulator was neither callable or an array. Row number: " . ($rowNum ?: "unknown") . ".");
        }
        throw new Exception("RowPopulator was neither callable or an array. Row number: " . ($rowNum ?: "unknown") . ".");
    }


    /**
     * Returns iterator of for a csv file
     * @deprecated since version 2.1.0 use self::getCsvRowIterator() instead.
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     */
    public static function getCsvRowIteratorStatic(string $filepath, array $opt = []): Traversable|Iterator|null
    {
        return self::getCsvRowIterator($filepath, $opt);
    }

    /**
     * Returns iterator of for a csv file
     * @since version 2.1.0 instead of self::getCsvRowIteratorStatic().
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     */
    public static function getCsvRowIterator(string $filepath, array $opt = []): Traversable|Iterator|null
    {
        $delimiter = (isset ($opt) && array_key_exists('fieldDelimiter', $opt)) ? $opt ['fieldDelimiter'] : ",";
        $enclosure = (isset ($opt) && array_key_exists('fieldEnclosure', $opt)) ? $opt ['fieldEnclosure'] : '"';
        $iterator = CsvParser::fromFile(realpath($filepath), [
            'encoding' => 'UTF8',
            'delimiter' => $delimiter,
            'enclosure' => $enclosure,
            'header' => false,
            'filepath' => $filepath
        ]);
        return $iterator?->getIterator();
    }
}