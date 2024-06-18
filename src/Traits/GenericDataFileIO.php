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
use Box\Spout\Reader\ReaderFactory;
use Box\Spout\Reader\ReaderInterface;
use Box\Spout\Writer\WriterInterface;
use Exception;
use Iterator;
use nordgen\DbBatch\Models\CsvParserWrapper\Reader;
use Traversable;

/**
 *
 */
trait GenericDataFileIO
{
    /** @var ReaderInterface|null */
    protected ?ReaderInterface $fileReader = null;

    /** @var WriterInterface|null */
    protected ?WriterInterface $fileWriter = null;

    /**
     * Returns a sheet iterator
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     * @throws IOException
     * @throws ReaderNotOpenedException
     * @throws UnsupportedTypeException
     */
    public static function getSheetIteratorStatic(string $filepath, array &$opt = []): Traversable|Iterator|null
    {
        $opt ['fileReader'] = self::getFileReaderStatic($filepath, $opt);
        return $opt ['fileReader']->getSheetIterator();
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return Reader|ReaderInterface
     * @throws IOException
     * @throws UnsupportedTypeException
     */
    protected static function getFileReaderStatic(
        string $filepath,
        array  $opt = []
    ): CsvParserWrapper\Reader|ReaderInterface
    {
        $fieldDelimiter = (
            isset ($opt) && array_key_exists('fieldDelimiter', $opt)
        ) ? $opt ['fieldDelimiter'] : ",";
        $fieldEnclosure = (
            isset ($opt) && array_key_exists('fieldEnclosure', $opt)
        ) ? $opt ['fieldEnclosure'] : '"';
        $fieldEol = (isset ($opt) && array_key_exists('fieldEol', $opt)) ? $opt ['fieldEol'] : "\n";
        $readerType = (isset ($opt) && array_key_exists('readerType', $opt)) ? $opt ['readerType'] : Type::CSV;
        // Type::XLSX
        // Type::CSV
        // Type::ODS

        if (
            $readerType == Type::CSV
            && array_key_exists('fieldHandleSpecialCases', $opt)
            && $opt ['fieldHandleSpecialCases'] === true
        ) {
            $reader = new Reader ();
            $reader->setGlobalFunctionsHelper(new GlobalFunctionsHelper ());
        } else {
            $reader = ReaderFactory::create($readerType); // for $readerType files
        }

        if ($readerType == Type::CSV) {
            $reader->setFieldDelimiter($fieldDelimiter);
            $reader->setFieldEnclosure($fieldEnclosure);
            $reader->setEndOfLineCharacter($fieldEol);
        }


        $reader->open($filepath);

        return $reader;
    }

    /**
     * @return ReaderInterface|null
     */
    public function getInternalFileReader(): ?ReaderInterface
    {
        return $this->fileReader;
    }

    /**
     * @return WriterInterface|null
     */
    public function getInternalFileWriter(): ?WriterInterface
    {
        return $this->fileWriter;
    }

    /**
     * Populates given table in given database with data from file
     *
     * @param string $filepath name to file to populate from
     * @param array $opt
     *
     * @throws Exception
     */
    public function validateHeadRowItemDiff(string $filepath, array &$opt = []): void
    {
        $errorMsg = "";
        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);

        $rownum = -1;

        try {
            foreach ($sheetIterator as $sheet) {
                $firstRow = true;
                $head = [];
                foreach ($sheet->getRowIterator() as $rawrow) {
                    if ($firstRow) {
                        $firstRow = false;
                        $head = $rawrow;
                        continue;
                    }
                    $rownum++;
                    if (($headlength = count($head)) != ($rowlength = count($rawrow))) {
                        // print ("Row: $rownum, has different row length ($rowlength) compared to head length ($headlength)\n");
                        $errorMsg .= "Row: $rownum, has different row length ($rowlength) compared to head length ($headlength)\n";
                        // } else {
                        // $row = array_combine ( $head , $rawrow );
                    }
                }
            }

            if (!empty ($errorMsg)) {
                throw new Exception($errorMsg);
            }
        } catch (Exception $e) {
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
        }
    }

    /**
     * Returns a sheet iterator
     *
     * @param string $filepath
     * @param array $opt
     * @return Iterator|Traversable|NULL
     * @throws IOException
     * @throws ReaderNotOpenedException
     * @throws UnsupportedTypeException
     */
    public function getSheetIteratorObject(string $filepath, array &$opt = []): Traversable|Iterator|null
    {
        $this->fileReader = $this->getFileReaderObject($filepath, $opt);
        return $this->fileReader->getSheetIterator();
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return Reader|ReaderInterface
     * @throws IOException
     * @throws UnsupportedTypeException
     */
    protected function getFileReaderObject(string $filepath, array $opt = []): Reader|ReaderInterface
    {
        return self::getFileReaderStatic($filepath, $opt);
    }

}