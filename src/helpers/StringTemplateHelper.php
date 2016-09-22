<?php
/**
 * StringTemplateHelper
 *
 *
 */
namespace nordgen\DbBatch\helpers;
/*
if (!isset($json)) {
    sleep( 3 );
    require_once "$scpurl/libraries/JSON.php";
    $json = new Services_JSON( );
}
global $json;
*/

/**
 *
 * @author kal
 *        
 */
class StringTemplateHelper
{

    protected static $keyValues;

    static function setKeyValues(array $keyValues)
    {
        self::$keyValues = $keyValues;
    }

    static function replacement($match)
    {
        $values = self::$keyValues;
        if (($result = self::resolveArray($values, $match[0])) != false) {
            return $result;
        }
        // Check if "any" in "#{any}" is of pattern "(key=='value')?'true_case':'false_case'"
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
                        $condition = $key > $value;
                        break;
                    case '<=':
                        $condition = $key >= $value;
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
    
    static function resolveArray($arr, $keyStr) {
        $re = '/(\w+)((\[\w+])*)/';
        if(preg_match($re, $keyStr, $matches)) {
            $keyArr = [$matches[1]];
            $keyArr = array_merge($keyArr,self::convertBracketStringToArray($matches[2]));
            return self::resolveArrayInternal($arr, $keyArr);
        }
        return false;
    }
    
    
    protected static function resolveArrayInternal($arr, $keyArr) {
        $key = array_shift($keyArr);
        $arr = $arr[$key];
        if(is_array($arr)) {
            return self::resolveArrayInternal($arr, $keyArr);
        }
        return $arr;
    }
    
    protected static function convertBracketStringToArray($str) {
        $str = substr($str, 1, -1);
        return explode('][', $str);
    }
    
    

    static function template($template, $values)
    {
        self::setKeyValues($values);
        //return preg_replace_callback('/#\{([^{}]*)}/', '_replacement', $template);
        return preg_replace_callback('/#\{([^{}]*)}/', array('StringTemplateHelper','replacement'), $template);
    }

    /**
     * Scrapes a string for key names
     *
     * @param string $str
     *            A template string with #{xxx} key names
     * @return multitype: An array with key names
     */
    static function scrapeStringForKeyNames($template)
    {
        $matches = array();
        if (preg_match_all('/#\{([^{}]*)}/', $template, $matches)) {
            array_shift($matches);
            return array_unique($matches[0]);
        }
        return array();
    }

    static function scrapeFileForKeyNames($filename)
    {
        return self::scrapeStringForKeyNames(file_get_contents($filename));
    }

    static protected function parseIniString($str)
    {
        if (empty($str))
            return false;
        
        $lines = explode("\n", $str);
        $ret = Array();
        $inside_section = false;
        
        foreach ($lines as $line) {
            
            $line = trim($line);
            
            if (! $line || $line[0] == "#" || $line[0] == ";")
                continue;
            
            if ($line[0] == "[" && $endIdx = strpos($line, "]")) {
                $inside_section = substr($line, 1, $endIdx - 1);
                continue;
            }
            
            if (! strpos($line, '='))
                continue;
            
            $tmp = explode("=", $line, 2);
            
            if ($inside_section) {
                
                $key = rtrim($tmp[0]);
                $value = ltrim($tmp[1]);
                
                if (preg_match("/^\".*\"$/", $value) || preg_match("/^'.*'$/", $value)) {
                    $value = mb_substr($value, 1, mb_strlen($value) - 2);
                }
                
                $t = preg_match("^\[(.*?)\]^", $key, $matches);
                if (! empty($matches) && isset($matches[0])) {
                    
                    $arr_name = preg_replace('#\[(.*?)\]#is', '', $key);
                    
                    if (! isset($ret[$inside_section][$arr_name]) || ! is_array($ret[$inside_section][$arr_name])) {
                        $ret[$inside_section][$arr_name] = array();
                    }
                    
                    if (isset($matches[1]) && ! empty($matches[1])) {
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

    static function readIniFile($filename)
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
            ;
        }
        
        return $data;
    }

    static function createSqltHelperStruct($dirpath)
    {
        $struct = array();
        if (! isset($dirpath)) {
            return null;
        }
        
        //echo "$dirpath";
        $filenames = array_diff(scandir($dirpath), array(
            '..',
            '.'
        ));

        function __find_files_with_sqlt_ext($file)
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
}

?>
