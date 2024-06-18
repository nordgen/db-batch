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

use Box\Spout\Reader\SheetInterface;
use nordgen\DbBatch\Models\CsvParser\CsvParser;

/**
 * Class Sheet
 *
 * @package Box\Spout\Reader\CSV
 */
class Sheet implements SheetInterface
{
    /** @var \Box\Spout\Reader\CSV\RowIterator To iterate over the CSV's rows */
    protected $rowIterator;
    protected $parser;
    /**
     * @param resource $filePointer Pointer to the CSV file to read
     * @param string $fieldDelimiter Character that delimits fields
     * @param string $fieldEnclosure Character that enclose fields
     * @param string $encoding Encoding of the CSV file to be read
     * @param \Box\Spout\Common\Helper\GlobalFunctionsHelper $globalFunctionsHelper
     */
    public function __construct($filePointer, $fieldDelimiter, $fieldEnclosure, $encoding, $endOfLineCharacter, $globalFunctionsHelper)
    {
    	$meta_data = stream_get_meta_data($filePointer);
    	$filepath = $meta_data["uri"];
    	$this->parser = CsvParser::fromFile($filepath,['encoding' => 'UTF8', 'delimiter' => $fieldDelimiter, 'enclosure' => $fieldEnclosure, 'header'=>false]);
        $this->rowIterator = $this->parser->getIterator();
    }
    /**
     * @api
     * @return \Box\Spout\Reader\CSV\RowIterator
     */
    public function getRowIterator()
    {
        return $this->rowIterator;
    }
}