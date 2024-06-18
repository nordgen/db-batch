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
use Monolog\Handler\StreamHandler;
use Monolog\Level;
use Monolog\Logger;
use nordgen\DbBatch\Traits\GenericDataFileIO;
use nordgen\DbBatch\Helpers\ArrayHelper;
use nordgen\DbBatch\Traits\Connection;
use nordgen\DbBatch\Traits\Export;
use nordgen\DbBatch\Traits\Import;
use nordgen\DbBatch\Traits\Query;
use nordgen\DbBatch\Traits\RecordManipulation;
use Throwable;

/**
 *
 * @mixin \nordgen\DbBatch\Adapters\Adodb\DbBatch|\nordgen\DbBatch\Adapters\Laminas\DbBatch|\nordgen\DbBatch\Adapters\Yiisoft\
 */
abstract class DbBatchAbstract implements DbBatchInterface
{
    use GenericDataFileIO;
    use Connection;
    use Query;
    use Import;
    use Export;
    use RecordManipulation;

    /**
     *
     */
    const EXPECTED_CONNECTION_TYPE = 'Invalid connection type';

    /**
     *
     * @var mixed
     */
    protected mixed $queryResult = null;


    /**
     * @var Logger|null
     */
    protected ?Logger $fileLogger = null;

    /**
     * Constructor
     *
     * @param mixed|null $connection
     * @throws Exception
     */
    public function __construct(mixed $connection = null)
    {
        if (!isset ($connection)) {
            return;
        }
        $connection = static::convertConnection($connection);

        $this->connectionType = static::getConnectionTypeStatic($connection);

        if (!self::validateConnectionType($this->connectionType)) {
            throw new Exception ("Database connection is not valid.");
        }

        $this->connection = $connection;

        $this->fileLogger = new Logger('Test');
        $this->fileLogger->pushHandler(new StreamHandler('dbBatch.log', Level::Warning));
        //$this->fileLogger->warning('Testing... ');
    }

    /**
     *
     * @param string $name
     * @param array $arguments
     * @return mixed
     */
    public static function __callStatic(string $name, array $arguments)
    {
        // Handle static methods with the same name as non-static methods
        switch ($name) {
            case 'getFileReader' :
                return call_user_func_array(array(
                    'self',
                    'getFileReaderStatic'
                ), $arguments);

            default :
                break;
        }

        return call_user_func_array(
            $name,
            $arguments
        );
    }

    /**
     *
     * @param string $name
     * @param array $arguments
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {

        // Handle non-static methods with the same name as static methods
        switch ($name) {
            case 'getFileReader' :
                return call_user_func_array(array(
                    $this,
                    'getFileReaderObject'
                ), $arguments);

            default :
                break;
        }

        return call_user_func_array(
            $name,
            $arguments
        );
    }

    /**
     */
    public function startTrans(): void
    {

    }

    /**
     * @return void
     * @throws Throwable
     */
    public function completeTrans(): void
    {

    }

    /**
     * @return void
     * @throws Throwable
     */
    public function rollbackTrans(): void
    {

    }

    /**
     * @return void
     * @throws Throwable
     */
    public function failTrans(): void
    {

    }

    /**
     * @param array $arr
     * @return bool
     */
    protected function isAssoc(array $arr): bool
    {
        return ArrayHelper::isAssoc($arr);
    }

}