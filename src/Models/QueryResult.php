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

namespace nordgen\DbBatch\Models;

use Iterator;
use Countable;
use Throwable;
use Yiisoft\Db\Exception\Exception;
use Yiisoft\Db\Exception\InvalidConfigException;
use Yiisoft\Db\Query\Data\DataReaderInterface;

/**
 * @author kal
 * @template T
 */
class QueryResult implements Iterator, Countable
{
    /**
     * @var T $queryResult
     */
    protected $queryResult;

    /**
     * @var class-string<T>|null $classType
     */
    protected ?string $classType = null;

    /**
     * @param T $queryResult
     */
    public function __construct($queryResult)
    {

        $this->queryResult = $queryResult;
        $this->getClassType();

    }

    /**
     * @return class-string<T>|null
     */
    public function getClassType(): ?string
    {
        if (!isset($this->classType)) {
            $this->classType = get_class($this->queryResult);
            codecept_debug($this->classType);
            $this->classType = preg_replace("/^ADORecordSet(.+)/", "ADORecordSet", $this->classType);
        }
        return $this->classType;
    }

    /**
     * @return T
     */
    public function getQueryResult()
    {
        return $this->queryResult;
    }

    /**
     * @return mixed
     */
    public function key(): mixed
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                return $this->queryResult->CurrentRow();

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    return $this->queryResult->key();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    return $this->queryResult->key();
                }
                break;
        }
        return null;
    }

    /**
     * @return bool
     */
    public function valid(): bool
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                return true;

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    return $this->queryResult->valid();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    return $this->queryResult->valid();
                }
                break;
        }
        return false;
    }

    /**
     * @return int
     * @throws Throwable
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function count(): int
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                return $this->queryResult->RecordCount();

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    return $this->queryResult->count();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    return $this->queryResult->count();
                }
                break;
        }
        return 0;
    }

    /**
     * @return mixed
     * @throws Exception
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function read(): mixed
    {
        $row = $this->current();
        $this->next();
        return $row;
    }

    /**
     * @return mixed
     */
    public function current(): mixed
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                $rownum = $this->queryResult->CurrentRow();
                $row = $this->queryResult->FetchRow();
                $this->queryResult->Move($rownum);
                return $row;

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    return $this->queryResult->current();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    return $this->queryResult->current();
                }
                break;
        }
        return null;
    }

    /**
     * @return void
     * @throws Exception
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function next(): void
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                $this->queryResult->MoveNext();
                break;

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    $this->queryResult->next();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    $this->queryResult->next();
                }
                break;
        }
    }

    /**
     * @return array
     * @throws Exception
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function readAll(): array
    {
        $result = [];
        $this->rewind();
        foreach ($this->queryResult as $row) {
            $result[] = $row;
        }
        return $result;
        //return $this->queryResult->current();
    }

    /**
     * @return void
     * @throws Exception
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function rewind(): void
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                $this->queryResult->Move(0);
                break;

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    $this->queryResult->rewind();
                }
                break;

            default:
                if (is_iterable($this->queryResult)) {
                    $this->queryResult->rewind();
                }
                break;
        }
    }

    /**
     * @return int
     * @throws Throwable
     * @throws Exception
     * @throws InvalidConfigException
     */
    public function fieldCount(): int
    {
        switch ($this->getClassType()) {
            case 'ADORecordSet':
                return $this->queryResult->FieldCount();

            case 'Yiisoft\\Db\\Query\\Data\\DataReaderInterface':
                if ($this->queryResult instanceof DataReaderInterface) {
                    $qrCopy = clone $this->queryResult;
                    $qrCopy->rewind();
                    $row = $qrCopy->current();
                    return count($row);
                }


            case 'Laminas\\Db\\ResultSet\\ResultSet':
                return $this->queryResult->getFieldCount();
        }
        return 0;
    }

}
