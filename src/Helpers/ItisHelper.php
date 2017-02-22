<?php

namespace nordgen\DbBatch\Helpers;

class ItisJsonHelper {

    protected $url = 'https://www.itis.gov/ITISWebService/jsonservice/ITISService/';


    /**
     * @var /ItisJsonHelper
     */
    protected static $instance = null;

    public static function getInstance() 
    {
        if (!isset(self::$instance)) {
            self::$instance = new self();
        };
        return self::$instance;
    }

    function __get($name,$params)
    {


        switch ($name) {

            case 'searchByCommonName':
            case 'searchByCommonNameBeginsWith':
            case 'searchByCommonNameEndsWith':
            case 'searchByScientificName':


                $ch = curl_init();
                curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
                curl_setopt($ch,CURLOPT_URL,$this->url.$name.'?'.http_build_query($params));
                $output=curl_exec($ch);
                curl_close($ch);
                return $output;
                ;
                break;

            default:
                ;
                break;
        }

        return false;
    }

    function __callstatic($name,$params)
    {
        $instance = self::getInstance();
        $instance->__get($name,$params);
    }
    
    public static function test($param) 
    {
        ;
    }
}