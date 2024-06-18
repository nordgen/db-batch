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

use Exception;

/**
 *
 */
trait Connection
{

    /**
     * @var string|null
     */
    protected ?string $connectionType = null;

    /**
     *
     * @var mixed
     */
    protected mixed $connection = null;

    /**
     * @throws Exception
     */
    public static function convertConnection(mixed $connection = null): mixed
    {
        return $connection;
    }

    /**
     *
     * @param mixed|null $connection
     * @return string
     * @throws Exception
     */
    public function getConnectionType(mixed $connection = null): string
    {
        if (!isset($connection)) {
            $connection = $this->connection;
        }
        return static::getConnectionTypeStatic($connection);
    }

    /**
     *
     * @param mixed|null $connection
     * @return string
     * @throws Exception
     */
    public static function getConnectionTypeStatic(mixed $connection = null): string
    {
        $connectionType = get_class($connection);
        if ($connectionType === static::EXPECTED_CONNECTION_TYPE) {
            return $connectionType;
        }
        throw new Exception('Unmatched connection type.');
    }

    /**
     * @param string|null $connectionType
     * @return bool
     */
    public function validateConnectionType(string $connectionType = null): bool
    {
        if (!isset($connectionType)) {
            $connectionType = $this->connectionType;
        }
        return static::validateConnectionTypeStatic($connectionType);
    }

    /**
     * @param string|null $connectionType
     * @return bool
     */
    public static function validateConnectionTypeStatic(string $connectionType = null): bool
    {
        return $connectionType == static::EXPECTED_CONNECTION_TYPE;
    }

    /**
     * @return mixed
     */
    public function getConnection(): mixed
    {
        return $this->connection;
    }

}