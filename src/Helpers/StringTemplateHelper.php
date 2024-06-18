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


/**
 *
 */
class StringTemplateHelper
{

    /**
     * @var array|null
     */
    protected static ?array $keyValues;

    /**
     * @param array $keyValues
     * @return void
     */
    public static function setKeyValues(array $keyValues): void
    {
        self::$keyValues = $keyValues;
    }

    /**
     * @param $match
     * @return mixed|string
     */
    public static function replacement($match): mixed
    {
        $values = self::$keyValues;
        $callback = function ($matches) use ($values) {
            return self::replacementInternal($matches, $values);
        };

        return $callback($match);
    }


    /**
     * @param $match
     * @param $values
     * @return mixed|string
     */
    protected static function replacementInternal($match, $values): mixed
    {
        //$values = self::$keyValues;
        if (($result = self::resolveArray($values, $match[0])) != false) {
            return $result;
        } // Check if "any" in "#{any}" is of pattern "(key=='value')?'true_case':'false_case'"
        elseif (preg_match("/\((?P<key>\w+?)(?P<op>[=!<>]{1,2})'(?P<value>.+?)'\)\?'(?P<true_case>.+?)':'(?P<false_case>.*)'/", $match[0], $match2)) {
            $condition = false;
            $key = $values["${match2['key']}"];
            $key_exists = gettype($key) === "string" || gettype($key) === 'number';
            if ($key_exists) {

                // Convert key if numeric
                if (is_numeric($key)) {
                    if (is_int($key)) {
                        $key = intval($key);
                    } else {
                        $key = floatval($key);
                    }
                }
                $value = $match2['value'];
                if (is_numeric($value)) {
                    if (is_int($value)) {
                        $value = intval($value);
                    } else {
                        $value = floatval($value);
                    }
                }

                switch ("${match2['op']}") {
                    case '==':
                        $condition = $key === $value;
                        break;
                    case '!=':
                        $condition = $key !== $value;
                        break;
                    case '>':
                        $condition = $key > $value;
                        break;
                    case '>=':
                        $condition = $key >= $value;
                        break;
                    case '<':
                        $condition = $key < $value;
                        break;
                    case '<=':
                        $condition = $key <= $value;
                        break;

                    default:
                        break;
                }
                return ($condition) ? $match2['true_case'] : $match2['false_case'];
            }


            // return $match2[false_case];
        }

        return gettype($values["$match[1]"]) === "string" || gettype($values["$match[1]"]) === 'number' ? $values["$match[1]"] : "$match[0]";
    }

    /**
     * @param $arr
     * @param $keyStr
     * @return false|mixed
     */
    public static function resolveArray($arr, $keyStr): mixed
    {
        $re = '/(\w+)((\[\w+])*)/';
        if (preg_match($re, $keyStr, $matches)) {
            $keyArr = [$matches[1]];
            $keyArr = array_merge($keyArr, self::convertBracketStringToArray($matches[2]));
            return self::resolveArrayInternal($arr, $keyArr);
        }
        return false;
    }

    /**
     * @param $str
     * @return array
     */
    protected static function convertBracketStringToArray($str): array
    {
        $str = substr($str, 1, -1);
        return explode('][', $str);
    }

    /**
     * @param $arr
     * @param $keyArr
     * @return mixed
     */
    protected static function resolveArrayInternal($arr, $keyArr): mixed
    {
        $key = array_shift($keyArr);
        $arr = $arr[$key];
        if (is_array($arr)) {
            return self::resolveArrayInternal($arr, $keyArr);
        }
        return $arr;
    }

    /**
     * @param $template
     * @param $values
     * @return array|string|null
     */
    public static function template($template, $values): array|string|null
    {
        //self::setKeyValues($values);

        //return preg_replace_callback('/#\{([^{}]*)}/', '_replacement', $template);
        //return preg_replace_callback('/#\{([^{}]*)}/', array('StringTemplateHelper','replacement'), $template);
        //return preg_replace_callback('/(.+?#\{([^{]*)})+?/', 'StringTemplateHelper::replacement', $template);
        //return preg_replace_callback('/#\{([^{}]*)}/', 'StringTemplateHelper::replacement', $template);


        $callback = function ($matches) use ($values) {
            return self::replacementInternal($matches, $values);
        };


        return preg_replace_callback('/#\{([^{}]*)}/', $callback, $template);
    }

    /**
     * @param $dirpath
     * @return array|null
     */
    public static function createSqltHelperStruct($dirpath): ?array
    {
        $struct = array();
        if (!isset($dirpath)) {
            return null;
        }

        //echo "$dirpath";
        $filenames = array_diff(scandir($dirpath), array(
            '..',
            '.'
        ));

        function __find_files_with_sqlt_ext($file): bool|int
        {
            return preg_match("/(.+)\.sqlt$/", $file);
        }

        $filenames = array_filter($filenames, "__find_files_with_sqlt_ext");

        foreach ($filenames as $filename) {
            $filepath = $dirpath . $filename;
            $filepathini = $dirpath . basename($filename, "sqlt") . "ini";
            $struct[$filename] = array(
                "keys" => self::scrapeFileForKeyNames($filepath),
                "keyvalues" => self::readIniFile($filepathini)
            );
        }

        return $struct;
    }

    /**
     * @param $filename
     * @return array
     */
    public static function scrapeFileForKeyNames($filename)
    {
        return self::scrapeStringForKeyNames(file_get_contents($filename));
    }

    /**
     * Scrapes a string for key names
     *
     * @param string $template
     *            A template string with #{xxx} key names
     * @return array: An array with key names
     */
    public static function scrapeStringForKeyNames($template)
    {
        $matches = array();
        if (preg_match_all('/#\{([^{}]*)}/', $template, $matches)) {
            array_shift($matches);
            return array_unique($matches[0]);
        }
        return array();
    }

    /**
     * @param $filename
     * @return bool|array
     */
    public static function readIniFile($filename): bool|array
    {
        //global $json;
        // $data = parse_ini_file($filename, true, INI_SCANNER_RAW);
        $data = self::parseIniString(file_get_contents($filename));

        foreach ($data as $sectionName => $section) {

            //echo "\$sectionName=$sectionName\n";

            foreach ($section as $key => $value) {

                $value = trim($value);

                if (preg_match('/^({.?|.+})$/', $value)) {
                    //echo "Matched -- \"$key\":\"$value\" \n";
                    $decodedValue = json_decode($value, true);
                    //$decodedValue = $json->decode($value);

                    //echo "\$decodedValue=" . print_r($decodedValue, true) . "\n";
                    $data[$sectionName][$key] = $decodedValue;
                }
            }
        }

        return $data;
    }

    /**
     * @param $str
     * @return bool|array
     */
    static protected function parseIniString($str): bool|array
    {
        if (empty($str))
            return false;

        $lines = explode("\n", $str);
        $ret = array();
        $inside_section = false;

        foreach ($lines as $line) {

            $line = trim($line);

            if (!$line || $line[0] == "#" || $line[0] == ";")
                continue;

            if ($line[0] == "[" && $endIdx = strpos($line, "]")) {
                $inside_section = substr($line, 1, $endIdx - 1);
                continue;
            }

            if (!strpos($line, '='))
                continue;

            $tmp = explode("=", $line, 2);

            if ($inside_section) {

                $key = rtrim($tmp[0]);
                $value = ltrim($tmp[1]);

                if (preg_match("/^\".*\"$/", $value) || preg_match("/^'.*'$/", $value)) {
                    $value = mb_substr($value, 1, mb_strlen($value) - 2);
                }

                $t = preg_match("^\[(.*?)]^", $key, $matches);
                if (!empty($matches) && isset($matches[0])) {

                    $arr_name = preg_replace('#\[(.*?)]#is', '', $key);

                    if (!isset($ret[$inside_section][$arr_name]) || !is_array($ret[$inside_section][$arr_name])) {
                        $ret[$inside_section][$arr_name] = array();
                    }

                    if (isset($matches[1]) && !empty($matches[1])) {
                        $ret[$inside_section][$arr_name][$matches[1]] = $value;
                    } else {
                        $ret[$inside_section][$arr_name][] = $value;
                    }
                } else {
                    $ret[$inside_section][trim($tmp[0])] = $value;
                }
            } else {

                $ret[trim($tmp[0])] = ltrim($tmp[1]);
            }
        }
        return $ret;
    }
}

