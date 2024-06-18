<?php

namespace nordgen\DbBatch\Traits;

use Exception;

trait RecordManipulation
{

    /**
     * @param array $opt
     * @param array $filterFunctions
     * @param string $recordFieldValueKeyName
     * @return mixed
     * @throws Exception
     */
    public static function parseRecordValueFilters(
        array &$opt,
        array $filterFunctions = [],
        string $recordFieldValueKeyName = 'fieval'
    ): mixed {
        if (!array_key_exists($recordFieldValueKeyName, $opt)) {
            throw new Exception('First option parameter \$opt does\'t contain expected keys in argument ');
        }
        foreach ($filterFunctions as $filterFunction) {
            if (isset($filterFunction) && is_callable($filterFunction)) {
                $opt[$recordFieldValueKeyName] = call_user_func_array($filterFunction, [$opt]);
            }
        }
        return $opt[$recordFieldValueKeyName];
    }


    /**
     * @param mixed $fieldnames
     * @return array|false
     */
    public static function createTableRecordByFieldnames(mixed $fieldnames): bool|array
    {
        if (gettype($fieldnames) === 'string') {
            $fieldnames = preg_replace('/\s*,\s*/', ',', trim($fieldnames));
            $record_keys = explode(",", $fieldnames);
        } elseif (gettype($fieldnames) === 'array') {
            $record_keys = $fieldnames;
        } else {
            // Error !!!
            return array();
        }

        /**
         * @param string $str
         * @return string
         */
        function map_filter(string $str): string
        {
            // Matches the cases in order schema.column as (alias), schema.(column) and (column)
            return ((bool)preg_match("/.+?[.].+\s+as\s+(.*)|.+?[.](.*)|(.*)/", $str, $matches))
                ? "${matches[1]}${matches[2]}${matches[3]}"
                : $str;
        }

        if (!function_exists('array_fill_keys')) {
            function array_fill_keys($keys, $value = ''): array
            {
                return array_combine($keys, array_fill(0, count($keys), $value));
            }
        }

        return array_fill_keys(array_unique(array_map("map_filter", $record_keys)), "");  // Php 5.2 <
    }

}