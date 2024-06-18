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

namespace nordgen\DbBatch;

use Exception;
use nordgen\DbBatch\Adapters\Adodb\DbBatch as AdodbBatch;
use nordgen\DbBatch\Adapters\Laminas\DbBatch as LaminasDbBatch;
use nordgen\DbBatch\Adapters\Yiisoft\DbBatch as YiisoftDbBatch;

/**
 *
 */
class DbBatch
{

    /**
     * @param mixed $connection
     * @return YiisoftDbBatch|LaminasDbBatch|AdodbBatch|false
     * @throws Exception
     */
    public static function create(mixed $connection): YiisoftDbBatch|LaminasDbBatch|bool|AdodbBatch
    {
        $classname = AdodbBatch::getConnectionTypeStatic($connection);

        return match ($classname) {
            AdodbBatch::EXPECTED_CONNECTION_TYPE => new AdodbBatch($connection),
            LaminasDbBatch::EXPECTED_CONNECTION_TYPE => new LaminasDbBatch($connection),
            YiisoftDbBatch::EXPECTED_CONNECTION_TYPE => new YiisoftDbBatch($connection),
            default => false
        };
    }
}