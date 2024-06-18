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

namespace nordgen\DbBatch\Models\CsvParserWrapper;

use Box\Spout\Reader\IteratorInterface;

/**
 * Class SheetIterator
 * Iterate over CSV unique "sheet".
 *
 * @package nordgen\DbBatchDeprecated
 */
class SheetIterator implements IteratorInterface
{
    /** @var \Box\Spout\Reader\CSV\Sheet The CSV unique "sheet" */
    protected $sheet;
    /** @var bool Whether the unique "sheet" has already been read */
    protected $hasReadUniqueSheet = false;
    /**
     * @param resource $filePointer
     * @param string $fieldDelimiter Character that delimits fields
     * @param string $fieldEnclosure Character that enclose fields
     * @param string $encoding Encoding of the CSV file to be read
     * @param \Box\Spout\Common\Helper\GlobalFunctionsHelper $globalFunctionsHelper
     */
    public function __construct($filePointer, $fieldDelimiter, $fieldEnclosure, $encoding, $endOfLineCharacter, $globalFunctionsHelper)
    {
        $this->sheet = new Sheet($filePointer, $fieldDelimiter, $fieldEnclosure, $encoding, $endOfLineCharacter, $globalFunctionsHelper);
    }
    /**
     * Rewind the Iterator to the first element
     * @link http://php.net/manual/en/iterator.rewind.php
     *
     * @return void
     */
    public function rewind(): void
    {
        $this->hasReadUniqueSheet = false;
    }
    /**
     * Checks if current position is valid
     * @link http://php.net/manual/en/iterator.valid.php
     *
     * @return boolean
     */
    public function valid(): bool
    {
        return (!$this->hasReadUniqueSheet);
    }
    /**
     * Move forward to next element
     * @link http://php.net/manual/en/iterator.next.php
     *
     * @return void
     */
    public function next(): void
    {
        $this->hasReadUniqueSheet = true;
    }
    /**
     * Return the current element
     * @link http://php.net/manual/en/iterator.current.php
     *
     * @return \Box\Spout\Reader\CSV\Sheet
     */
    public function current(): mixed
    {
        return $this->sheet;
    }
    /**
     * Return the key of the current element
     * @link http://php.net/manual/en/iterator.key.php
     *
     * @return int
     */
    public function key(): mixed
    {
        return 1;
    }
    /**
     * Cleans up what was created to iterate over the object.
     *
     * @return void
     */
    public function end(): void
    {
        // do nothing
        return;
    }
}