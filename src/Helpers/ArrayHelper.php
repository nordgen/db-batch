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

namespace nordgen\DbBatch\Helpers;

use Closure;

/**
 *
 */
class ArrayHelper
{
    /**
     * Helper function to allow different length in head (keys) and row (values) arrays.
     * @param array $head
     * @param array $row
     *
     * @return array
     */
    public static function headRowArrayCombine(array $head, array $row): array
    {
        $min = min(count($head), count($row));
        return array_combine(array_slice($head, 0, $min), array_slice($row, 0, $min));
    }

    /**
     * @param array $array
     * @param string $key
     * @param mixed $default
     * @return void
     */
    public static function initiateArrayKeyIfNeeded(array &$array, string $key, mixed $default = []): void
    {
        if (!(isset ($opt) && array_key_exists('extraData', $opt))) {
            $opt ['extraData'] = $default;
        }
    }

    /**
     * @param array $array
     * @param string $key
     * @param mixed|null $default
     * @return mixed
     */
    public static function getArrayKeyValue(array $array, string $key, mixed $default = null): mixed
    {
        return (array_key_exists($key, $array) && isset($array[$key])) ? $array[$key] : $default;
    }

    /**
     * @param array $array
     * @param string $key
     * @param callable $default
     * @param object $that
     * @return Closure
     */
    public static function getArrayKeyCallableValue(array $array, string $key, callable $default, object $that): Closure
    {
        $callable = (array_key_exists($key, $array) && isset($array[$key])) && is_callable($array[$key])
            ? $array[$key]
            : $default;
        return $callable(...)->bindTo($that);
    }

    /**
     * @param callable $callback
     * @param array $arr
     * @return array
     */
    public static function arrayKeyMap(callable $callback, array $arr = []): array
    {
        $result = [];
        array_walk($arr, function ($value, $key) use ($callback, &$result) {
            $result[$key] = $callback($value, $key);
        });
        return $result;
    }


    /**
     * @param array $arr
     * @return bool
     */
    public static function isAssoc(array $arr): bool
    {
        if (array() === $arr)
            return false;
        return array_keys($arr) !== range(0, count($arr) - 1);
    }

}