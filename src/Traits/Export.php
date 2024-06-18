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

use Box\Spout\Common\Exception\IOException;
use Box\Spout\Common\Exception\UnsupportedTypeException;
use Box\Spout\Common\Helper\GlobalFunctionsHelper;
use Box\Spout\Common\Type;
use Box\Spout\Reader\Exception\ReaderNotOpenedException;
use Box\Spout\Writer\Exception\WriterNotOpenedException;
use Box\Spout\Writer\WriterFactory;
use Box\Spout\Writer\WriterInterface;
use Exception;
use nordgen\DbBatch\Models\CsvParserWrapper\Reader;
use nordgen\DbBatch\Helpers\ArrayHelper;

/**
 *
 */
trait Export
{


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
    public function export(string $filepath, string $table = "", callable $rowPopulator = null, array &$opt = []): void
    {
        $opt ??= [];
        ArrayHelper::initiateArrayKeyIfNeeded($opt, 'extraData');
        $extraData = &$opt['extraData'];


        $this->fileWriter = $this->getFileWriterObject($filepath, $opt);

        if (array_key_exists('head', $opt)) {
            $extraData['head'] = $opt['head'];
        }

        if (!isset($rowPopulator)) {
            $rowPopulator = function ($row, $rowNum, $extraData) {
                return $row;
            };
        }

        $sql = <<<SQL
SELECT Col.Column_Name from 
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
WHERE 
    Col.Constraint_Name = Tab.Constraint_Name
    AND Col.Table_Name = Tab.Table_Name
    AND Constraint_Type = 'PRIMARY KEY'
    AND Col.Table_Name = '$table'
SQL;
        $pk = $this->queryScalar($sql);
        $head = (isset($opt) && array_key_exists('head', $opt)) ? $opt ['head'] : [];


        //$this->fileLogger->warning('count head : '.count( $head));

        if (empty($head)) {
            $sql = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='$table'";
            $headraw = $this->queryColumn($sql); // Get column names of table
            $head = isset($pk) ? array_unique(array_merge([$pk], $headraw)) : $headraw;
            $extraData['head'] = $head;

            $sql = "SELECT " . implode(',', array_map(function ($columnname) {
                    return "\"$columnname\"";
                }, $head)) . " FROM $table";
        } else {
            $sql = "SELECT " . implode(',', array_map(function ($columnname) {
                    return "\"$columnname\"";
                }, $head)) . " FROM $table";
        }

        $sql = isset($pk) ? $sql . " ORDER BY $pk ASC;" : $sql . ";";

        $this->query($sql);

        $this->walkDbResult($rowPopulator, $extraData);

        // close writer
        $this->fileWriter->close();
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return Reader|WriterInterface
     * @throws IOException
     * @throws UnsupportedTypeException
     */
    protected function getFileWriterObject(string $filepath, array $opt = []): Reader|WriterInterface
    {
        return self::getFileWriterStatic($filepath, $opt);
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return Reader|WriterInterface
     * @throws IOException
     * @throws UnsupportedTypeException
     */
    protected static function getFileWriterStatic(string $filepath, array $opt = []): Reader|WriterInterface
    {
        $fieldDelimiter = (isset ($opt) && array_key_exists('fieldDelimiter', $opt)) ? $opt ['fieldDelimiter'] : ",";
        $fieldEnclosure = (isset ($opt) && array_key_exists('fieldEnclosure', $opt)) ? $opt ['fieldEnclosure'] : '"';
        $writerType = (isset ($opt) && array_key_exists('writerType', $opt)) ? $opt ['writerType'] : Type::CSV;
        // Type::XLSX
        // Type::CSV
        // Type::ODS

        if ($writerType == Type::CSV && array_key_exists('fieldHandleSpecialCases', $opt) && $opt ['fieldHandleSpecialCases'] === true) {
            $writer = new Reader();
            $writer->setGlobalFunctionsHelper(new GlobalFunctionsHelper());
        } else {
            $writer = WriterFactory::create($writerType); // for $readerType files
        }

        $writer->setFieldDelimiter($fieldDelimiter);
        $writer->setFieldEnclosure($fieldEnclosure);

        $writer->openToFile($filepath);

        return $writer;
    }

    /**
     * @param callable $callback
     * @param $extraData
     * @throws Exception
     */
    public function walkDbResult(callable $callback, &$extraData): void
    {
        // Do nothing if
        if (!isset($this->queryResult) || !$this->queryResult) {
            return;
        }

        //$extraData = (isset ( $opt ) && array_key_exists ( 'extraData', $opt )) ? $opt ['extraData'] : [ ];

        //$beforeInsert = (isset ( $opt ) && array_key_exists ( 'beforeInsert', $opt )) ? $opt ['beforeInsert'] : function ($row, $rowNum, $extraData) {
        //};
        //$afterInsert = (isset ( $opt ) && array_key_exists ( 'afterInsert', $opt )) ? $opt ['afterInsert'] : function ($row, $rowNum, $extraData) {
        //};

        //$beforeInsert = $beforeInsert->bindTo ( $this );
        //$afterInsert = $afterInsert->bindTo ( $this );

        //$writer = (isset ( $opt ) && array_key_exists ( 'writer', $extraData )) ? $extraData ['writer'] : null;

        $head = (isset($extraData) && array_key_exists('head', $extraData)) ? $extraData ['head'] : [];
        if (isset($head)) {
            $headline = array_combine($head, $head);
            $extraData = $extraData + ['headline' => $headline];
            $this->insertRowIntoFile($this->fileWriter, $head, 0, function ($row, $rowNum, $extraData) {
                return $extraData['headline'];
            }, $extraData);
        }

        foreach ($this->queryResult as $rowNum => $rawRow) {

            $row = array_combine($head, $rawRow);

            if (isset ($this->fileWriter) && $this->fileWriter != null) {
                if (isset ($beforeInsert) && is_callable($beforeInsert)) {
                    $beforeInsert($row, $rowNum, $extraData);
                }

                if (isset ($callback) && is_callable($callback)) {
                    //$logger->warning('Logging rawrow: '.implode(',',$rawRow));
                    $this->insertRowIntoFile($this->fileWriter, $row, $rowNum, $callback, $extraData);
                }
                if (isset ($afterInsert) && is_callable($afterInsert)) {
                    $afterInsert($row, $rowNum, $extraData);
                }
            } else {
                $this->processClosure($callback, $row, $rowNum, $extraData);
            }
        }

        //$this->fileLogger->warning('Logging after.');
    }

    /**
     *
     * @param WriterInterface $writer
     * @param array $row
     * @param string|int|null $rowNum
     * @param callable|array|null $rowPopulator
     * @param array $extraData
     * @throws IOException
     * @throws WriterNotOpenedException
     */
    public function insertRowIntoFile(
        WriterInterface $writer,
        array           $row,
        string|int      $rowNum = null,
        callable|array  $rowPopulator = null,
        array           &$extraData = []
    ): void
    {
        $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rowNum, $extraData);
        $writer->addRow($rowToInsert);
    }

    /**
     *
     * @param string $filepath
     * @param callable $rowPopulator
     * @param array $opt
     * @param string $preferredSheet
     * @param null $result
     * @return array|null
     * @throws IOException
     * @throws ReaderNotOpenedException
     * @throws UnsupportedTypeException
     */
    public function mapReader(
        string   $filepath,
        callable $rowPopulator,
        array    &$opt = [],
        ?string  $preferredSheet = null,
        ?array   &$result = null
    ): ?array
    {
        $opt ??= [];
        ArrayHelper::initiateArrayKeyIfNeeded($opt, 'extraData');
        $extraData = &$opt['extraData'];

        $ignoreSecondRow = $opt['ignoreSecondRow'] ?: false;
        $rowNum = 0;
        $reader = $this->getFileReaderObject($filepath, $opt);
        foreach ($reader->getSheetIterator() as $sheet) {
            if (isset($preferredSheet) && $preferredSheet != $sheet->getName()) {
                continue;
            }
            $firstRow = true;
            $secondRow = false;
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
                $ret = $this->processClosure($rowPopulator, $row, $rowNum, $extraData);
                if (isset($result) && is_array($result)) {
                    $result[] = $ret;
                }
            }
        }
        $opt ['extraData'] = $extraData;
        if (isset($result) && is_array($result)) {
            return $result;
        }
        return null;
    }

}