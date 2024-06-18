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

use ADOConnection;
use ADODB_mysqli;
use ADODB_postgres8;
use ADODB_postgres9;
use Exception;

/**
 *
 */
trait DbBatchConnectionTrait
{

    /**
     * @throws Exception
     */
    public static function convertConnection(mixed $connection = null): mixed
    {
        if (is_array($connection)) {
            $connection = self::getAdodbConnection($connection);
        }
        return $connection;
    }

    /**
     *
     * @param array $opt
     * @return ADOConnection|ADODB_postgres8|ADODB_postgres9|ADODB_mysqli
     * @throws Exception
     */
    protected static function getAdodbConnection(array $opt): ADODB_postgres8|ADODB_mysqli|ADODB_postgres9|ADOConnection
    {
        $connection = ADONewConnection($opt ['db'] ['driver']); // eg. 'mysql' or 'oci8'
        if (!isset ($connection)) {
            throw new Exception ("No Adodb object was created.");
        }
        $connection->debug = isset ($opt ['db'] ['debug']) ? ($opt ['db'] ['debug'] ?: false) : false;
        $connection->Connect($opt ['db'] ['server'], $opt ['db'] ['user'], $opt ['db'] ['password'] ?: null, $opt ['db'] ['database']);
        $connection->SetFetchMode(ADODB_FETCH_ASSOC);
        return $connection;
    }

    /**
     *
     * @param mixed|null $connection
     * @return string
     */
    public static function getConnectionTypeStatic(mixed $connection = null): string
    {
        $connectionType = get_class($connection);
        if (str_starts_with($connectionType, self::EXPECTED_CONNECTION_TYPE)) {
            $connectionType = static::EXPECTED_CONNECTION_TYPE;
        }
        return $connectionType;
    }

}