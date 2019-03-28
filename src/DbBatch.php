<?php

namespace nordgen\DbBatch;

use Box\Spout\Reader\ReaderFactory;
use Box\Spout\Writer\WriterFactory;
use Box\Spout\Common\Type;
use nordgen\DbBatch\CsvParser\CsvParser;
use Box\Spout\Common\Helper\GlobalFunctionsHelper;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use nordgen\DbBatch\Helpers\StringTemplateHelper;
use Zend\Db\Adapter\Adapter;
use Zend\Db\Adapter\Driver\Pgsql\Result as PgsqlResult;
use Zend\Db\Adapter\Driver\Pdo\Result as PdoResult;
use Zend\Db\Adapter\Driver\Mysqli\Result as MysqlResult;
use Zend\Db\Adapter\Driver\ResultInterface;
use Zend\Db\Adapter\StatementContainerInterface;
use Zend\Db\ResultSet\ResultSet;
use Zend\Stdlib\ArrayObject;
use Zend\Db\Sql\Sql;
use Zend\Db\Adapter\Exception\InvalidQueryException;

/**
 *
 * @author Kjell-Åke Lundblad <kjellake.lundblad@nordgen.org>
 *
 */
class DbBatch
{
    protected $yiiTransaction = null;

    protected $connectionType = null;

    /** @var \Box\Spout\Reader\ReaderInterface */
    protected $fileReader = null;

    public function getInternalFileReader()
    {
        $this->fileReader;
    }

    /** @var \Box\Spout\Writer\WriterInterface */
    protected $fileWriter = null;

    public function getInternalFileWriter()
    {
        $this->fileWriter;
    }

    protected $fileLogger = null;

    /**
     *
     * @var mixed
     */
    protected $queryResult = null;

    /**
     *
     * @var mixed
     */
    protected $queryResultSet = null;

    /**
     *
     * @var \Zend\Db\Adapter\Adapter|\ADOConnection|\ADODB_postgres8|\ADODB_postgres9|\ADODB_mysql|\ADODB_mysqli|\ADODB_mysqlt|Adapter|mixed
     */
    protected $db = null;

    /**
     * Constructor
     *
     * @param mixed
     * @throws \Exception
     */
    public function __construct($db = null)
    {
        if (!isset ($db)) {
            return;
        }
        if (is_array($db)) {
            $db = $this->getAdodbConnection($db);
        }
        $this->connectionType = $this->getConnectionType($db);
        if (!(in_array($this->connectionType, ['ADODB', 'yii\\db\\Connection', 'Zend\\Db\\Adapter\\Adapter']))) {
            throw new \Exception ("Database connection is not valid.");
        }
        $this->db = $db;

        $this->fileLogger = new Logger('Test');
        $this->fileLogger->pushHandler(new StreamHandler('mytest.log', Logger::WARNING));
        //$this->fileLogger->warning('Testing... ');
    }

    /**
     *
     * @param string $name
     * @param array $arguments
     * @return mixed
     */
    public function __call($name, $arguments)
    {

        // Handle non-static methods with the same name as static methods
        switch ($name) {
            case 'getFileReader' :
                return call_user_func_array(array(
                    $this,
                    'getFileReaderObject'
                ), $arguments);
                break;

            case 'isAssoc' :
                return call_user_func_array(array(
                    $this,
                    'isAssoc'
                ), $arguments);
                break;

            default :
                ;
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
    public static function __callStatic($name, $arguments)
    {
        // Handle static methods with the same name as non-static methods
        switch ($name) {
            case 'getFileReader' :
                return call_user_func_array(array(
                    'self',
                    'getFileReaderStatic'
                ), $arguments);
                break;

            case 'getCsvRowIterator' :
                return call_user_func_array(array(
                    'self',
                    'getCsvRowIteratorStatic'
                ), $arguments);

            default :
                ;
                break;
        }

        return call_user_func_array(
            $name,
            $arguments
        );
    }

    public function getDb()
    {
        return $this->db;
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return \Box\Spout\Reader\ReaderInterface
     */
    protected function getFileReaderObject($filepath, $opt = [])
    {
        return self::getFileReaderStatic($filepath, $opt);
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return \Box\Spout\Reader\ReaderInterface
     */
    protected static function getFileReaderStatic($filepath, $opt = [])
    {
        $fieldDelimiter = (isset ($opt) && array_key_exists('fieldDelimiter', $opt)) ? $opt ['fieldDelimiter'] : ",";
        $fieldEnclosure = (isset ($opt) && array_key_exists('fieldEnclosure', $opt)) ? $opt ['fieldEnclosure'] : '"';
        $fieldEol = (isset ($opt) && array_key_exists('fieldEol', $opt)) ? $opt ['fieldEol'] : "\n";
        $readerType = (isset ($opt) && array_key_exists('readerType', $opt)) ? $opt ['readerType'] : Type::CSV;
        // Type::XLSX
        // Type::CSV
        // Type::ODS

        if ($readerType == Type::CSV && array_key_exists('fieldHandleSpecialCases', $opt) && $opt ['fieldHandleSpecialCases'] === true) {
            $reader = new \nordgen\DbBatch\CsvParserWrapper\Reader ();
            $reader->setGlobalFunctionsHelper(new GlobalFunctionsHelper ());
        } else {
            $reader = ReaderFactory::create($readerType); // for $readerType files
        }

        if ($readerType == Type::CSV) {
            $reader->setFieldDelimiter($fieldDelimiter);
            $reader->setFieldEnclosure($fieldEnclosure);
            $reader->setEndOfLineCharacter($fieldEol);
        }


        $reader->open($filepath);

        return $reader;
    }


    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return \Box\Spout\Writer\WriterInterface
     */
    protected function getFileWriterObject($filepath, $opt = [])
    {
        return self::getFileWriterStatic($filepath, $opt);
    }

    /**
     * Returns Box\Spout reader object
     *
     * @param string $filepath
     * @param array $opt
     * @return \Box\Spout\Writer\WriterInterface
     */
    protected static function getFileWriterStatic($filepath, $opt = [])
    {
        $fieldDelimiter = (isset ($opt) && array_key_exists('fieldDelimiter', $opt)) ? $opt ['fieldDelimiter'] : ",";
        $fieldEnclosure = (isset ($opt) && array_key_exists('fieldEnclosure', $opt)) ? $opt ['fieldEnclosure'] : '"';
        // TODO: Test this!!!
        // $fieldEol = (isset ( $opt ) && array_key_exists( 'fieldEol', $opt )) ? $opt ['fieldEol'] : "\n";
        $writerType = (isset ($opt) && array_key_exists('writerType', $opt)) ? $opt ['writerType'] : Type::CSV;
        // Type::XLSX
        // Type::CSV
        // Type::ODS

        if ($writerType == Type::CSV && array_key_exists('fieldHandleSpecialCases', $opt) && $opt ['fieldHandleSpecialCases'] === true) {
            $writer = new \nordgen\DbBatch\CsvParserWrapper\Reader();
            $writer->setGlobalFunctionsHelper(new GlobalFunctionsHelper());
        } else {
            $writer = WriterFactory::create($writerType); // for $readerType files
        }

        $writer->setFieldDelimiter($fieldDelimiter);
        $writer->setFieldEnclosure($fieldEnclosure);

        // TODO: Test this!!!
        //if ($writerType == Type::CSV) {
        //$writer->setEndOfLineCharacter( $fieldEol );
        //}


        $writer->openToFile($filepath);

        return $writer;
    }

    /**
     * Returns iterator of for a csv file
     *
     * @param string $filepath
     * @param array $opt
     * @return \Iterator|\Traversable|NULL
     */
    protected function getCsvRowIteratorObject($filepath, $opt = [])
    {
        return self::getCsvRowIteratorStatic($filepath, $opt);
    }

    /**
     * Returns iterator of for a csv file
     *
     * @param string $filepath
     * @param array $opt
     * @return \Iterator|\Traversable|NULL
     */
    public static function getCsvRowIteratorStatic($filepath, $opt = [])
    {
        $delimiter = (isset ($opt) && array_key_exists('fieldDelimiter', $opt)) ? $opt ['fieldDelimiter'] : ",";
        $enclosure = (isset ($opt) && array_key_exists('fieldEnclosure', $opt)) ? $opt ['fieldEnclosure'] : '"';
        $iterator = CsvParser::fromFile(realpath($filepath), [
            'encoding' => 'UTF8',
            'delimiter' => $delimiter,
            'enclosure' => $enclosure,
            'header' => false,
            'filepath' => $filepath
        ]);
        return isset($iterator) ? $iterator->getIterator() : null;
    }

    /**
     * Returns a sheet iterator
     *
     * @param string $filepath
     * @param array $opt
     * @return \Iterator|\Traversable|NULL
     */
    public function getSheetIteratorObject($filepath, &$opt = [])
    {
        // if (isset($opt) && array_key_exists('fieldHandleSpecialCases', $opt) && $opt['fieldHandleSpecialCases'] && ((array_key_exists ( 'readerType', $opt )) ? $opt ['readerType'] : Type::CSV) == Type::CSV) {
        // //$reader = $this->getCsvRowIterator($filepath, $opt);
        // $this->fileReader = \nordgen\DbBatch\CsvParserWrapper\Reader();
        // $this->getFileReader($filepath, $opt);
        // $sheetIterator = (new \ArrayObject($reader))->getIterator();
        // } else {


        $this->fileReader = $this->getFileReaderObject($filepath, $opt);
        $sheetIterator = $this->fileReader->getSheetIterator();
        // }

        return $sheetIterator;
    }

    /**
     * Returns a sheet iterator
     *
     * @param string $filepath
     * @param array $opt
     * @return \Iterator|\Traversable|NULL
     */
    public static function getSheetIteratorStatic($filepath, &$opt = [])
    {
        // if (isset($opt) && array_key_exists('fieldHandleSpecialCases', $opt) && $opt['fieldHandleSpecialCases'] && ((array_key_exists ( 'readerType', $opt )) ? $opt ['readerType'] : Type::CSV) == Type::CSV) {
        // echo "----------------";
        // $fileReader = self::getCsvRowIterator($filepath, $opt);
        // $opt['fileReader'] = $fileReader;
        // $sheetIterator = (new \ArrayObject($fileReader))->getIterator();
        // } else {
        $opt ['fileReader'] = self::getFileReaderStatic($filepath, $opt);
        $sheetIterator = $opt ['fileReader']->getSheetIterator();
        // }

        return $sheetIterator;
    }

    /**
     * Populates given table in given database with data from file
     *
     * @param string $filepath name to file to populate from
     * @param array $opt
     *
     * @uses \ADOConnection|\ADODB_postgres8|\ADODB_postgres9|\ADODB_mysql|\ADODB_mysqli|\ADODB_mysqlt|yii\db\connection $this->db database connector
     *
     * @throws \Exception
     */
    public function validateHeadRowItemDiff($filepath, &$opt = [])
    {
        $errorMsg = "";
        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);

        $rownum = -1;

        try {
            foreach ($sheetIterator as $sheet) {
                $firstRow = true;
                $head = [];
                foreach ($sheet->getRowIterator() as $rawrow) {
                    if ($firstRow) {
                        $firstRow = false;
                        $head = $rawrow;
                        continue;
                    }
                    $rownum++;
                    if ($headlength = (count($head)) != ($rowlength = count($rawrow))) {
                        // print ("Row: $rownum, has different row length ($rowlength) compared to head length ($headlength)\n");
                        $errorMsg .= "Row: $rownum, has different row length ($rowlength) compared to head length ($headlength)\n";
                        // } else {
                        // $row = array_combine ( $head , $rawrow );
                    }
                }
            }

            if (!empty ($errorMsg)) {
                throw new \Exception($errorMsg);
            }
        } catch (\Exception $e) {
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
        }
    }

    /**
     * Helper function to allow different length in head (keys) and row (values) arrays.
     * @param array $head
     * @param array $row
     *
     * @return array
     */
    public function headRowArrayCombine(array $head, array $row)
    {
        $min = min(count($head), count($row));
        return array_combine(array_slice($head, 0, $min), array_slice($row, 0, $min));
    }

    /**
     * Populates given table in given database with data from file
     *
     * <p>NB $opt['extraData']['pk'] has to be set if 'id' is not primary key</p>
     *
     * @param string $filepath name to file to populate from
     * @param string $table tablename
     * @param callable $rowPopulator closure to handle each row
     * @param array $opt
     * @param int $preferedSheet
     *
     * @uses \ADOConnection|\ADODB_postgres8|\ADODB_postgres9|\ADODB_mysql|\ADODB_mysqli|\ADODB_mysqlt|yii\db\connection $this->db database connector
     *
     * @throws \Exception
     * @return bool success
     */
    public function populate($filepath, $table = "", $rowPopulator = null, &$opt = [], $preferedSheet = null)
    {
        if (!(isset ($opt) && array_key_exists('extraData', $opt))) {
            $opt ['extraData'] = [];
        }
        $extraData = &$opt['extraData'];
        $beforeInsert = (isset ($opt) && array_key_exists('beforeInsert', $opt) && isset($opt['beforeInsert'])) ? $opt ['beforeInsert'] : function ($row, $rownum, &$extraData) {
        };
        $afterInsert = (isset ($opt) && array_key_exists('afterInsert', $opt) && isset($opt['afterInsert'])) ? $opt ['afterInsert'] : function ($row, $rownum, &$extraData) {
        };

        $beforeInsert = $beforeInsert->bindTo($this);
        $afterInsert = $afterInsert->bindTo($this);

        $ignoreSecondRow = $opt['ignoreSecondRow'] ?: false;

        /*
         * if (isset($opt) && array_key_exists('fieldHandleSpecialCases', $opt) && $opt['fieldHandleSpecialCases'] && ((array_key_exists ( 'readerType', $opt )) ? $opt ['readerType'] : Type::CSV) == Type::CSV) {
         * $sheetIterator = (new ArrayObject([$this->getCsvRowIterator($filepath, $opt)]))->getIterator();
         * } else {
         * $this->fileReader = $this->getFileReader($filepath, $opt);
         * $sheetIterator = $this->fileReader->getSheetIterator();
         * }
         */
        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);

        $rownum = 0;
        $successTotal = true;

        $this->startTrans();

        try {
            foreach ($sheetIterator as $sheet) {
                if (isset($preferedSheet) && $preferedSheet != $sheet->getName()) {
                    continue;
                }
                if ($sheet->getName() == $preferedSheet) {
                    $firstRow = true;
                    $secondRow = false;
                    $successTotal = true;
                    $head = [];
                    foreach ($sheet->getRowIterator() as $rawrow) {
                        if ($firstRow) {
                            $firstRow = false;
                            $secondRow = true;
                            $head = $rawrow;
                            $rownum++;
                            continue;
                        }
                        if ($secondRow && $ignoreSecondRow) {
                            $secondRow = false;
                            $rownum++;
                            continue;
                        }
                        $rownum++;
                        $row = array_combine($head, $rawrow) ?: $this->headRowArrayCombine($head, $rawrow);

                        if (isset ($beforeInsert) && is_callable($beforeInsert)) {
                            $beforeInsert ($row, $rownum, $extraData);
                        }

                        if (isset ($rowPopulator) && is_callable($rowPopulator)) {
                            $success = $this->insertRowIntoTable($table, $row, $rownum, $rowPopulator, $extraData);
                            $successTotal = $successTotal && !!$success;
                        }
                        if (isset ($afterInsert) && is_callable($afterInsert)) {
                            $afterInsert ($row, $rownum, $extraData);
                        }
                    }
                }
            }

            $this->completeTrans();
        } catch (\Exception $e) {
            $this->rollbackTrans();
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
            $opt ['extraData'] = $extraData;
        }

        return $successTotal;
    }


    /**
     * Updates given table in given database with data from file
     *
     * <p>NB $opt['extraData']['pk'] has to be set if 'id' is not primary key</p>
     *
     * @param string $filepath name to file to update from
     * @param string $table tablename
     * @param callable $rowUpdator closure to handle each row
     * @param array $opt
     * @param int $preferedSheet
     *
     * @uses ADODB|yii\db\connection $this->db database connector
     *
     * @throws \Exception
     * @return bool success
     */
    public function update($filepath, $table = "", $rowUpdator = null, &$opt = [], $preferedSheet = null)
    {
        $extraData = (isset ($opt) && array_key_exists('extraData', $opt)) ? $opt['extraData'] : [];
        $beforeUpdate = (isset ($opt) && array_key_exists('beforeUpdate', $opt) && isset($opt['beforeUpdate'])) ? $opt ['beforeUpdate'] : function ($row, $rownum, $extraData) {
        };
        $afterUpdate = (isset ($opt) && array_key_exists('afterUpdate', $opt) && isset($opt['afterUpdate'])) ? $opt ['afterUpdate'] : function ($row, $rownum, $extraData) {
        };

        $beforeUpdate = $beforeUpdate->bindTo($this);
        $afterUpdate = $afterUpdate->bindTo($this);

        $ignoreSecondRow = $opt['ignoreSecondRow'] ?: false;
        $updateWhereCondition = $opt['updateWhereCondition'] ?: null;


        // Define an array_keymap function that takes an array and a closure and then returns key mapped closure result

        $array_keymap = function ($callback, $arr) {
            $result = [];
            array_walk($arr, function ($value, $key) use ($callback, &$result) {
                $result[$key] = $callback($value, $key);
            });
            return $result;
        };


        //$someresult = StringTemplateHelper::template($query, $_REQUEST['kv']);


        $sheetIterator = $this->getSheetIteratorObject($filepath, $opt);

        $rownum = 0;

        $this->startTrans();

        try {
            $successTotal = false;
            foreach ($sheetIterator as $sheet) {
                if (isset($preferedSheet) && $preferedSheet != $sheet->getName()) {
                    continue;
                }
                $firstRow = true;
                $secondRow = false;
                $successTotal = true;
                $head = [];
                foreach ($sheet->getRowIterator() as $rawrow) {
                    if ($firstRow) {
                        $firstRow = false;
                        $secondRow = true;
                        $head = $rawrow;
                        $rownum++;
                        continue;
                    }
                    if ($secondRow && $ignoreSecondRow) {
                        $secondRow = false;
                        $rownum++;
                        continue;
                    }
                    $rownum++;
                    $row = array_combine($head, $rawrow) ?: $this->headRowArrayCombine($head, $rawrow);
                    if (isset ($beforeUpdate) && is_callable($beforeUpdate)) {
                        $beforeUpdate ($row, $rownum, $extraData);
                    }

                    if (isset ($rowUpdator) && is_callable($rowUpdator)) {
                        $success = $this->updateRowInTable($table, $row, $rownum, $rowUpdator, $updateWhereCondition, $extraData);
                        $successTotal = $successTotal && !!$success;
                    }
                    if (isset ($afterUpdate) && is_callable($afterUpdate)) {
                        $afterUpdate ($row, $rownum, $extraData);
                    }
                }
            }

            $this->completeTrans();
        } catch (\Exception $e) {
            $this->rollbackTrans();
            throw $e;
        } finally {
            if (isset($this->fileReader) && method_exists($this->fileReader, 'close')) {
                $this->fileReader->close();
            }
            $opt ['extraData'] = $extraData;
        }

        return $successTotal;
    }


    /**
     * Populates given table in given database with data from file
     *
     * @param string $filepath
     *            name to file to populate from
     * @param string $table
     *            tablename
     * @param callable $rowPopulator
     *            clousre to handle each row
     * @param array $opt
     *
     * @uses ADODB|yii\db\connection $this->db database connector
     *
     * @throws \Exception
     */
    public function export($filepath, $table = "", $rowPopulator = null, &$opt = [])
    {
        if (!(isset ($opt) && array_key_exists('extraData', $opt))) {
            $opt ['extraData'] = [];
        }

        $this->fileWriter = $this->getFileWriterObject($filepath, $opt);

        if (array_key_exists('head', $opt)) {
            $extraData['head'] = $opt['head'];
        }

        if (!isset($rowPopulator)) {
            $rowPopulator = function ($row, $rownum, $extraData) {
                return $row;
            };
        }

        $sql = <<<SQL
SELECT Col.Column_Name from 
    INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
    INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
WHERE 
    Col.Constraint_Name = Tab.Constraint_Name
    AND Col.Table_Name = Tab.Table_Name
    AND Constraint_Type = 'PRIMARY KEY'
    AND Col.Table_Name = '$table'
SQL;
        $pk = $this->queryScalar($sql);
        $head = (isset ($opt) && array_key_exists('head', $opt)) ? $opt ['head'] : [];


        //$this->fileLogger->warning('count head : '.count( $head));

        if (empty($head)) {
            $sql = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='$table'";
            $headraw = $this->queryColumn($sql); // Get column names of table
            $head = isset($pk) ? array_unique(array_merge([$pk], $headraw)) : $headraw;
            $extraData['head'] = $head;

            $sql = "SELECT " . implode(',', array_map(function ($columnname) {
                    return "\"$columnname\"";
                }, $head)) . " FROM $table";
        } else {
            $sql = "SELECT " . implode(',', array_map(function ($columnname) {
                    return "\"$columnname\"";
                }, $head)) . " FROM $table";
        }

        $sql = isset($pk) ? $sql . " ORDER BY $pk ASC;" : $sql . ";";

        $this->query($sql);

        $this->walkDbResult($rowPopulator, $extraData);

        // close writer
        $this->fileWriter->close();
    }

    /**
     *
     * @param string $filepath
     * @param callable $rowCallback
     * @param array $opt
     * @param null|array $result
     * @return array|null
     * @throws \Exception
     */
    public function mapReader($filepath, callable $rowPopulator, &$opt = [], $preferedSheet = null, &$result = null)
    {
        if (!(isset ($opt) && array_key_exists('extraData', $opt))) {
            $opt ['extraData'] = [];
        }
        $extraData = &$opt ['extraData'];
        $ignoreSecondRow = $opt['ignoreSecondRow'] ?: false;
        $rownum = 0;
        $reader = $this->getFileReaderObject($filepath, $opt);
        foreach ($reader->getSheetIterator() as $sheet) {
            if (isset($preferedSheet) && $preferedSheet != $sheet->getName()) {
                continue;
            }
            $firstRow = true;
            $secondRow = false;
            foreach ($sheet->getRowIterator() as $rawrow) {
                if ($firstRow) {
                    $firstRow = false;
                    $secondRow = true;
                    $head = $rawrow;
                    $rownum++;
                    continue;
                }
                if ($secondRow && $ignoreSecondRow) {
                    $secondRow = false;
                    $rownum++;
                    continue;
                }
                $rownum++;
                $row = array_combine($head, $rawrow) ?: $this->headRowArrayCombine($head, $rawrow);
                $ret = $this->processClosure($rowPopulator, $row, $rownum, $extraData);
                if (isset ($result) && is_array($result)) {
                    $result [] = $ret;
                }
            }
        }
        $opt ['extraData'] = $extraData;
        if (isset ($result) && is_array($result)) {
            return $result;
        }
        return null;
    }

    /**
     *
     * @param mixed
     * @return string|mixed
     */
    public function getConnectionType($db = null)
    {
        if (!isset($db)) {
            $db = $this->db;
        }

        return self::getConnectionTypeStatic($db);
    }


    /**
     *
     * @param mixed
     * @return string|mixed
     */
    public static function getConnectionTypeStatic($db = null)
    {
        $connectionType = get_class($db);
        if (strpos($connectionType, 'ADODB') === 0) {
            $connectionType = 'ADODB';
        }
        return $connectionType;
    }

    /**
     *
     * @param array $opt
     * @throws \Exception
     * @return \ADOConnection|\ADODB_postgres8|\ADODB_postgres9|\ADODB_mysql|\ADODB_mysqli|\ADODB_mysqlt
     */
    public static function getAdodbConnection($opt)
    {
        try {
            $db = ADONewConnection($opt ['db'] ['driver']); // eg. 'mysql' or 'oci8'
            if (!isset ($db)) {
                throw new \Exception ("No Adodb object was created.");
            }
            $db->debug = isset ($opt ['db'] ['debug']) ? ($opt ['db'] ['debug'] ?: false) : false;
            $db->Connect($opt ['db'] ['server'], $opt ['db'] ['user'], $opt ['db'] ['password'] ?: null, $opt ['db'] ['database']);
            $db->SetFetchMode(ADODB_FETCH_ASSOC);
            return $db;
        } catch (\Exception $e) {
            throw $e;
        }
    }

    /**
     * insertRowIntoTable
     * NB $extraData['pk'] has to be set if 'id' is not primary key
     * @param string $table
     * @param array $row
     * @param array|callable $rowPopulator
     * @param array $extraData
     * @throws \Exception
     */
    public function insertRowIntoTable($table, $row, $rownum, $rowPopulator, &$extraData = [])
    {
        $isThrowExceptionEnabled = isset ($extraData ['isThrowExceptionEnabled']) ? $extraData ['isThrowExceptionEnabled'] === true : false;
        switch ($this->connectionType) {
            case 'ADODB' :
                $pk = isset ($extraData ['pk']) ? $extraData ['pk'] : 'id';

                $noInsertOnEmptyRow = isset ($extraData ['noInsertOnEmptyRow']) ? $extraData ['noInsertOnEmptyRow'] === true : false;

                if ($noInsertOnEmptyRow && empty(array_filter($row))) {
                    return true;
                }

                // Create empty recordset
                $sql = "SELECT * FROM $table WHERE $pk = -1";
                $rs = $this->db->Execute($sql); // Execute the query and get the empty recordset
                $extraData ['rs'] = $rs;
                $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rownum, $extraData);

                // Ignore row if it is false
                if (!!$rowToInsert) {
                    $insertSQL = $this->db->GetInsertSQL($rs, $rowToInsert);
                    $result = $this->db->Execute($insertSQL); // Insert the record into the database;
                    if (!$result && $isThrowExceptionEnabled) {
                        throw new \Exception($this->db->ErrorMsg());
                    }
                    return !!$result;
                } elseif ($noInsertOnEmptyRow) {
                    return true;
                } elseif ($isThrowExceptionEnabled) {
                    throw new \Exception("Could not prepare an insert sql.");
                }
                return false;

                break;
            case 'yii\\db\\Connection' :
                $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rownum, $extraData);
                // Ignore row if it is false
                if (!!$rowToInsert) {
                    $this->db->createCommand()->insert($table, $rowToInsert);
                    if ($isThrowExceptionEnabled) {
                        return !!$this->db->createCommand()->insert($table, $rowToInsert)->execute();
                    } else {
                        try {
                            return !!$this->db->createCommand()->insert($table, $rowToInsert)->execute();
                        } catch (\Exception $e) {
                            return false;
                        }
                    }
                }
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                //$pk = isset ( $extraData ['pk'] ) ? $extraData ['pk'] : 'id';

                $noInsertOnEmptyRow = isset ($extraData ['noInsertOnEmptyRow']) ? $extraData ['noInsertOnEmptyRow'] === true : false;

                if ($noInsertOnEmptyRow && empty(array_filter($row))) {
                    return true;
                }

                $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rownum, $extraData);

                // Ignore row if it is false
                if (!!$rowToInsert) {
                    $sql = new Sql($this->db);
                    $insert = $sql->insert($table);
                    $insert->values($rowToInsert);
                    $statement = $sql->prepareStatementForSqlObject($insert);
                    try {
                        $statement->execute();
                    } catch (\Exception $e) {
                        if ($isThrowExceptionEnabled) {
                            throw $e;
                        }
                        return false;
                    }
                } elseif ($noInsertOnEmptyRow) {
                    return true;
                } elseif ($isThrowExceptionEnabled) {
                    throw new \Exception("Could not prepare an insert sql.");
                }
                return false;

                break;
            default :
                return false;
                break;
        }
        return false;
    }


    /**
     *
     * @param string $table
     * @param array $row
     * @param array|callable $rowUpdator
     * @param array $extraData
     * @throws \Exception
     */
    public function updateRowInTable($table, $row, $rownum, $rowUpdator, $condition = null, &$extraData = [])
    {
        $isThrowExceptionEnabled = isset ($extraData ['isThrowExceptionEnabled']) ? $extraData ['isThrowExceptionEnabled'] === true : false;

        if ($isThrowExceptionEnabled && !!$condition) {
            throw new \Exception("Update without condition.");
        }

        switch ($this->connectionType) {
            case 'ADODB' :
                $pk = isset ($extraData ['pk']) ? $extraData ['pk'] : 'id';
                // Create empty recordset
                $sql = "SELECT * FROM $table WHERE $pk = -1";
                $rs = $this->db->Execute($sql); // Execute the query and get the empty recordset

                $extraData ['rs'] = $rs;

                $rowToUpdate = $this->getRowToInsert($rowUpdator, $row, $rownum, $extraData);

                // parse $condition with $rowToUpdate context
                $templateData = [
                    'fileRow' => &$row,
                    'updateRow' => &$rowToUpdate,
                    'extraData' => &$extraData
                ];

                if (isset($condition) && is_array($condition) && count($condition) > 0) {
                    $callback = function ($value, $key) use ($row) {
                        return "$key = $value";
                    };
                    $condition = implode(' and ', self::arrayKeyMap($callback, $condition));
                }

                // i.e. $condition = "accide = #extraData[accide]"; =>
                // $condition = "accide = 345345; =>

                $condition = StringTemplateHelper::template($condition, $templateData);

                // Select recordset to update
                $sql = "SELECT * FROM $table WHERE $condition";
                $rs = $this->db->Execute($sql); // Execute the query and get selected recordset

                // Ignore row if it is false
                if (!!$rowToUpdate) {

                    $updateSQL = $this->db->GetUpdateSQL($rs, $rowToUpdate);
                    $result = $this->db->Execute($updateSQL); // Update the record in the database;
                    if (!$result && $isThrowExceptionEnabled) {
                        throw new \Exception($this->db->ErrorMsg());
                    }

                    return !!$result;

                } elseif ($isThrowExceptionEnabled) {
                    throw new \Exception("Could not prepare an insert sql.");
                }

                return false;

                break;
            case 'yii\\db\\Connection' :
                $rowToUpdate = $this->getRowToInsert($rowUpdator, $row, $rownum, $extraData);
                // Ignore row if it is false
                if (!!$rowToUpdate) {

                    if ($isThrowExceptionEnabled) {
                        return !!$this->db->createCommand()->update($table, $rowToUpdate)->execute();
                    } else {
                        try {
                            return !!$this->db->createCommand()->update($table, $rowToUpdate)->execute();
                        } catch (\Exception $e) {
                            return false;
                        }
                    }

                }
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                //$pk = isset ( $extraData ['pk'] ) ? $extraData ['pk'] : 'id';
                // Create empty recordset

                $rowToUpdate = $this->getRowToInsert($rowUpdator, $row, $rownum, $extraData);

                // parse $condition with $rowToUpdate context
                $templateData = [
                    'fileRow' => &$row,
                    'updateRow' => &$rowToUpdate,
                    'extraData' => &$extraData
                ];

                if (isset($condition) && is_array($condition) && count($condition) > 0) {
                    $callback = function ($value, $key) use ($row) {
                        return "$key = $value";
                    };
                    $condition = implode(' and ', self::arrayKeyMap($callback, $condition));
                }

                $condition = StringTemplateHelper::template($condition, $templateData);

                // Select recordset to update
                //$sql = "SELECT * FROM $table WHERE $condition";
                //$rs = $this->db->Execute($sql); // Execute the query and get selected recordset

                // Ignore row if it is false
                if (!!$rowToUpdate) {
                    $sql = new Sql($this->db);
                    $update = $sql->update($table);
                    $update->set($rowToUpdate);
                    $update->where($condition);
                    $statement = $sql->prepareStatementForSqlObject($update);
                    try {
                        $statement->execute();
                    } catch (\Exception $e) {
                        if ($isThrowExceptionEnabled) {
                            throw $e;
                        }
                        return false;
                    }
                } elseif ($isThrowExceptionEnabled) {
                    throw new \Exception("Could not prepare an insert sql.");
                }

                return false;

                break;
            default :
                return false;
                break;
        }
        return false;
    }

    /**
     *
     * @param \Box\Spout\Writer\WriterInterface $table
     * @param array $row
     * @param array|callable $rowPopulator
     * @param array $extraData
     * @throws \Exception
     */
    public function insertRowIntoFile(\Box\Spout\Writer\WriterInterface $writer, $row, $rownum = null, $rowPopulator = null, &$extraData = [])
    {
        $rowToInsert = $this->getRowToInsert($rowPopulator, $row, $rownum, $extraData);
        $writer->addRow($rowToInsert);
    }

    /**
     *
     * @param array|callable $rowPopulator
     * @param array $row
     * @param array $extraData
     * @throws \Exception
     * @return mixed|\Closure
     */
    public function getRowToInsert($rowPopulator, $row, $rownum, &$extraData)
    {
        $overideRowWithKeyVals = isset ($extraData ['overideRowWithKeyVals']) ? $extraData ['overideRowWithKeyVals'] : [];
        return $overideRowWithKeyVals + $this->processClosure($rowPopulator, $row, $rownum, $extraData);
    }

    /**
     *
     * @param array|callable $rowPopulator
     * @param array $row
     * @param array $extraData
     * @throws \Exception
     * @return mixed|\Closure
     */
    public function processClosure($rowPopulator, $row, $rownum, &$extraData)
    {
        //$overideRowWithKeyVals = isset ( $extraData ['overideRowWithKeyVals'] ) ? $extraData ['overideRowWithKeyVals'] : [];
        // TODO: Check if it is ok to remove $overideRowWithKeyVals
        switch (gettype($rowPopulator)) {
            case 'object' :
                if (is_callable($rowPopulator)) {
                    if (method_exists($rowPopulator, 'bindTo')) {
                        $rowPopulator = $rowPopulator->bindTo($this);
                    }

                    //return $overideRowWithKeyVals + $rowPopulator ( $row, $rownum, $extraData );
                    return $rowPopulator ($row, $rownum, $extraData);
                }
                if ($rowPopulator instanceof \Closure) {
                    $rowPopulator = $rowPopulator->bindTo($this);
                    return call_user_func_array($rowPopulator, [
                        $row,
                        $rownum,
                        &$extraData
                    ]);
                }

                break;
            case 'array' :
            case 'string' :
                if (is_callable($rowPopulator)) {
                    return call_user_func_array($rowPopulator, [
                        $row,
                        $rownum,
                        &$extraData
                    ]);
                } elseif (is_array($rowPopulator)) {
                    return $rowPopulator;
                }
                break;

            case 'NULL' :
                //return $overideRowWithKeyVals + $row;
                return $row;
                break;

            default :
                throw new \Exception ("RowPopulator was neither callable or an array. Row number: " . ($rownum ?: "unknown") . ".");
                break;
        }
        throw new \Exception("RowPopulator was neither callable or an array. Row number: " . ($rownum ?: "unknown") . ".");
    }

    /**
     *
     * @param string $sql
     * @throws \Exception
     */
    public function getQueryFieldNames($sql)
    {
        switch ($this->connectionType) {
            case 'ADODB':
                if (preg_match("/^(.+)\\;\\s*$/", $sql, $matches)) {
                    $sql = $matches[1];
                }

                $sql = "$sql Limit 1;";

                $rs = $this->db->Execute($sql); // Execute the query and get the empty recordset
                if (!$rs) {
                    throw new \Exception("Adodb error " . $this->db->ErrorNo() . ": " . $this->db->ErrorMsg());
                }


                # Get Field Names:
                $aRet = array();
                $lngCountFields = 0;
                if (!$rs->EOF) {
                    for ($i = 0; $i < $rs->FieldCount(); $i++) {
                        $fld = $rs->FetchField($i);
                        $aRet[$lngCountFields] = $fld->name;
                        $lngCountFields++;
                    }
                }
                return $aRet;
                break;
            case 'yii\\db\\Connection':
                // $this->db->createCommand ( $sql )->execute ();
                return [];
                break;
            case 'Zend\\Db\\Adapter\\Adapter':
                if (preg_match("/^(.+)\\;\\s*$/", $sql, $matches)) {
                    $sql = $matches[1];
                }
                $sql = "$sql Limit 1;";
                $this->query($sql);

                return $this->getQueryFieldNamesFromQueryResultSet();
                break;
            default:
                ;
                break;
        }
        return false;
    }

    /**
     *
     * @throws \Exception
     */
    public function getQueryFieldNamesFromQueryResultSet()
    {
        switch ($this->connectionType) {
            case 'ADODB':
                $rs = $this->getQueryResult();
                // Get Field Names:
                $aRet = array();
                $lngCountFields = 0;
                if (!$rs->EOF) {
                    for ($i = 0; $i < $rs->FieldCount(); $i++) {
                        $fld = $rs->FetchField($i);
                        $aRet[$lngCountFields] = $fld->name;
                        $lngCountFields++;
                    }
                }
                return $aRet;
                break;
            case 'yii\\db\\Connection':
                // $this->db->createCommand ( $sql )->execute ();
                return [];
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $rs = $this->getQueryResult();
                $rsArr = [];
                if (isset($rs) && $rs instanceof ResultSet && $rs->valid()) {
                    // Get Field Names:
                    $rs->rewind();
                    $r = $rs->current();
                    $rsArr = array_keys($r->getArrayCopy());
                }
                return $rsArr;
                break;
            default :
                ;
                break;
        }
        return false;
    }


    /**
     * @param callback $callback
     * @param array $opt
     */
    public function iterateQueryResultWithCallback(callable $callback = null, $opt = [])
    {

        switch ($this->connectionType) {
            case 'ADODB':
                $rs = $this->getQueryResult();
                if (!isset($callback)) {
                    $callback = function ($currentrow, $opt = []) {
                        ;
                    };
                }

                if (!$rs) {
                    break;
                }

                while (!$rs->EOF) {
                    $callback($rs->fields, $opt);
                    $rs->MoveNext();
                }

                break;
            case 'yii\\db\\Connection':


                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $rs = $this->getQueryResult();

                if (!isset($callback)) {
                    $callback = function ($currentrow, $opt = []) {
                        ;
                    };
                }

                if (!$rs) {
                    break;
                }

                try {
                    $noOfParams = self::getNrOfParams($callback);
                } catch (\Exception $e) {
                    $noOfParams = 3;
                }

                switch ($noOfParams) {
                    case 1:
                        foreach ($rs as $row) {
                            $callback($row);
                        }
                        break;
                    case 2:
                        foreach ($rs as $row) {
                            $callback($row, $opt);
                        }
                        break;
                    default:
                        foreach ($rs as $rownum => $row) {
                            $callback($row, $rownum, $opt);
                        }
                }


                break;
            default:
                ;
                break;
        }
    }


    /**
     *
     * @param string $sql
     * @param array $parameters
     * @throws \Exception
     */
    public function execute($sql, $parameters = null)
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $rs = $this->db->Execute($sql); // Execute the query and get the empty recordset
                if (!$rs) {
                    throw new \Exception ("Adodb error " . $this->db->ErrorNo() . ": " . $this->db->ErrorMsg());
                }
                break;
            case 'yii\\db\\Connection' :
                $this->db->createCommand($sql)->execute();
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                if ($this->db instanceof \Zend\Db\Adapter\Adapter) {
                    $parameters = ($parameters === null) ? Adapter::QUERY_MODE_EXECUTE : $parameters;
                    if (is_array($parameters)) {
                        try {
                            $statement = $this->db->createStatement($sql);
                            if ($statement instanceof StatementContainerInterface) {

                                $statement->prepare();
                                $result = $statement->execute($parameters);

                                $preparedSql = $statement->getSql();
                                $preparedParams = $statement->getParameterContainer();


                                if (!(isset($result) && $result instanceof ResultInterface && (
                                        $result->valid() ||
                                        $result->count() > 0 ||
                                        in_array('getAffectedRows', get_class_methods($result)) && $result->getAffectedRows() > 0)
                                )) {
                                    throw new \Exception ("Db query failed");
                                } else {
                                    return in_array('getAffectedRows', get_class_methods($result))
                                        ? $result->getAffectedRows() : null;
                                }
                            } else {
                                throw new \Exception ("Adapter statement is not implementing StatementContainerInterface");
                            }


                        } catch (InvalidQueryException $e) {
                            throw new \Exception ("Db query failed: " . $e->getMessage());
                        }

                    } elseif ($parameters === Adapter::QUERY_MODE_EXECUTE) {
                        try {
                            $result = $this->db->query($sql, $parameters);
                            return in_array('getAffectedRows', get_class_methods($result))
                                ? $result->getAffectedRows() : null;
                        } catch (InvalidQueryException $e) {
                            throw new \Exception ("Db query failed: " . $e->getMessage());
                        }

                    }
                }

                break;
            default :
                ;
                break;
        }
        return false;
    }

    /**
     *
     * @param string $sql
     * @param array $parameters
     * @throws \Exception
     */
    public function query($sql, $parameters = [])
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $rs = $this->db->Execute($sql); // Execute the query and get the empty recordset
                if (!$rs) {
                    throw new \Exception ("Adodb error " . $this->db->ErrorNo() . ": " . $this->db->ErrorMsg());
                }
                $this->queryResult = $rs;
                break;
            case 'yii\\db\\Connection' :
                $rs = $this->db->createCommand($sql)->query();
                $this->queryResult = new QueryResult ($rs);
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $parameters = ($parameters===null) ? Adapter::QUERY_MODE_EXECUTE : $parameters;
                if (is_array($parameters)) {
                    $statement = $this->db->createStatement($sql);
                    $statement->prepare();
                    $result = $statement->execute($parameters);
                    if ($result instanceof ResultInterface && $result->isQueryResult()) {
                        $resultSet = new ResultSet;
                        $resultSet->initialize($result);
                        $this->queryResult = $result;
                        $this->queryResultSet = $resultSet;
                    } else {
                        throw new \Exception ("Db query failed");
                    }
                }
                elseif ($parameters === Adapter::QUERY_MODE_EXECUTE) {
                    $result = $this->db->query($sql, $parameters);
                    if (!(isset($result) && ($result->valid() || $result->count()>0 /*|| $result->getAffectedRows()>0*/))) {
                        throw new \Exception ("Db query failed");
                    } else {
                        $this->queryResultSet = $result;
                    }
                }

                break;
            default :
                ;
                break;
        }

        return $this;
    }

    /**
     * @return false|PdoResult|PgsqlResult|MysqlResult
     */
    public function getQueryResult()
    {
        return $this->queryResult;
    }

    /**
     * @return false|ResultSet|ADORecordSet
     */
    public function getQueryResultSet()
    {
        return $this->queryResultSet;
    }


    /**
     * @param callable $callback
     * @param $extraData
     * @return array|FALSE
     * @throws \Exception
     */
    public function mapDbResult(callable $callback, &$extraData)
    {
        // Do nothing if
        if (!isset ($this->queryResult) || !$this->queryResult) {
            return false;
        }
        $arr = [];
        foreach ($this->queryResult as $rownum => $row) {
            $arr [] = $this->processClosure($callback, $row, $rownum, $extraData);
        }
        return $arr;
    }


    /**
     * @param callable $callback
     * @param $extraData
     * @throws \Exception
     */
    public function walkDbResult(callable $callback, &$extraData)
    {


        // Do nothing if
        if (!isset ($this->queryResult) || !$this->queryResult) {
            return;
        }

        //$extraData = (isset ( $opt ) && array_key_exists ( 'extraData', $opt )) ? $opt ['extraData'] : [ ];

        //$beforeInsert = (isset ( $opt ) && array_key_exists ( 'beforeInsert', $opt )) ? $opt ['beforeInsert'] : function ($row, $rownum, $extraData) {
        //};
        //$afterInsert = (isset ( $opt ) && array_key_exists ( 'afterInsert', $opt )) ? $opt ['afterInsert'] : function ($row, $rownum, $extraData) {
        //};

        //$beforeInsert = $beforeInsert->bindTo ( $this );
        //$afterInsert = $afterInsert->bindTo ( $this );

        //$writer = (isset ( $opt ) && array_key_exists ( 'writer', $extraData )) ? $extraData ['writer'] : null;

        $head = (isset ($extraData) && array_key_exists('head', $extraData)) ? $extraData ['head'] : [];


        if (isset($head)) {
            $headline = array_combine($head, $head);
            $extraData = $extraData + ['headline' => $headline];
            $this->insertRowIntoFile($this->fileWriter, $head, 0, function ($row, $rownum, $extraData) {
                return $extraData['headline'];
            }, $extraData);
        }


        //$rowarr = $this->queryResult->GetAll();


        //$it = $this->queryResult->getIterator();

        foreach ($this->queryResult as $rownum => $rawrow) {

            $row = array_combine($head, $rawrow);

            if (isset ($this->fileWriter) && $this->fileWriter != null) {
                if (isset ($beforeInsert) && is_callable($beforeInsert)) {
                    $beforeInsert ($row, $rownum, $extraData);
                }

                if (isset ($callback) && is_callable($callback)) {
                    //$logger->warning('Logging rawrow: '.implode(',',$rawrow));
                    $this->insertRowIntoFile($this->fileWriter, $row, $rownum, $callback, $extraData);
                }
                if (isset ($afterInsert) && is_callable($afterInsert)) {
                    $afterInsert ($row, $rownum, $extraData);
                }
            } else {
                $this->processClosure($callback, $row, $rownum, $extraData);
            }
        }

        //$this->fileLogger->warning('Logging after.');


    }

    /**
     * @param null $limit
     * @param int $offset
     * @return array
     */
    public function pageNextDbResult($limit = null, $offset = 0)
    {
        $result = [];
        foreach ($this->queryResult as $row) {
            if ($this->queryResult->key() < $offset) {
                continue;
            }
            if (isset ($limit) && $limit == 0) {
                break;
            }
            $result [] = $row;
            if (isset ($limit)) {
                $limit--;
            }
        }

        return $result;
    }

    /**
     * Returns first row and first column
     *
     * @param string $sql
     * @param array $parameters
     * @return mixed|null
     */
    public function queryScalar($sql, $parameters = [])
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                return $this->db->GetOne($sql); // Execute the query
                break;
            case 'yii\\db\\Connection' :
                return $this->db->createCommand($sql)->queryScalar();
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $statement = $this->db->createStatement($sql);
                $statement->prepare();
                $result = $statement->execute($parameters);

                if ($result instanceof ResultInterface && $result->isQueryResult() && $result->getAffectedRows() > 0) {
                    $resultSet = new ResultSet;
                    $resultSet->initialize($result);
                    $row = $resultSet->current();
                    if (!(isset($row) && $row->count() > 0)) {
                        return null;
                    }
                    $rowArr = $row->getArrayCopy();
                    return array_shift($rowArr);
                }
                return null;
                break;
            default :
                return null;
                break;
        }
    }

    /**
     * Returns an array of first column in each rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array|false
     */
    public function queryColumn($sql, $parameters = [])
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                return $this->db->GetCol($sql); // Execute the query
                break;
            case 'yii\\db\\Connection' :
                return $this->db->createCommand($sql)->queryColumn();
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $statement = $this->db->createStatement($sql);
                $statement->prepare();
                $result = $statement->execute($parameters);

                if ($result instanceof ResultInterface && $result->isQueryResult()) {
                    $resultSet = new ResultSet;
                    $resultSet->initialize($result);

                    $firstcolumn = [];

                    if ($resultSet->getReturnType() === 'arrayobject') {
//                        $firstcolumn[] = $row->offsetGet(0);
                        foreach ($resultSet->toArray() as $item) {
                            $firstcolumn[] = array_shift($item);
                        }
                        return $firstcolumn;
                    }
//                    elseif (is_array($resultSet)) {
//                        $firstcolumn[] = array_shift($row);
//                    }

//
//                    foreach ($resultSet as $row) {
//                        if ($row instanceof ArrayObject) {
//                            $firstcolumn[] = $row->offsetGet(0);
//                        } elseif (is_array($row)) {
//                            $firstcolumn[] = array_shift($row);
//                        }
//                    }

                }
                return null;
                break;
            default :
                return false;
                break;
        }
    }

    /**
     * Returns first row
     *
     * @param string $sql
     * @param array $parameters
     * @return array|false
     */
    public function queryOne($sql, $parameters = null)
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                return $this->db->GetRow($sql); // Execute the query
                break;
            case 'yii\\db\\Connection' :
                return $this->db->createCommand($sql)->queryOne();
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $statement = $this->db->createStatement($sql);
                $statement->prepare();
                $parameters = ($parameters === null) ? [] : $parameters;
                $result = $statement->execute($parameters);

                if ($result instanceof ResultInterface && $result->isQueryResult()) {
                    $resultSet = new ResultSet;
                    $resultSet->initialize($result);

                    $row = $resultSet->current();

                    if (!isset($row)) {
                        return null;
                    }

                    return $row->getArrayCopy();
                }
                return false;
                break;
            default :
                return false;
                break;
        }
    }

    /**
     * Returns all rows
     *
     * @param string $sql
     * @param array $parameters
     * @return array|false
     */
    public function queryAll($sql, $parameters = [])
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                return $this->db->GetAll($sql); // Execute the query
                break;
            case 'yii\\db\\Connection' :
                return $this->db->createCommand($sql)->queryAll();
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :

                $statement = $this->db->createStatement($sql);
                $statement->prepare();
                $result = $statement->execute($parameters);

                if ($result instanceof ResultInterface && $result->isQueryResult()) {
                    $resultSet = new ResultSet;
                    $resultSet->initialize($result);
                    return $resultSet->toArray();
                }
                return false;
            default :
                return false;
                break;
        }
    }


    public function getAllFromResultSet()
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                try {
                    return $this->getQueryResultSet()->toArray(); // Execute the query
                } catch (\Exception $e) {
                    return false;
                }
                break;
            case 'yii\\db\\Connection' :
                // Not implemented yet
                return false;
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                try {
                    return $this->getQueryResultSet()->toArray();
                } catch (\Exception $e) {
                    return false;
                }


            default :
                return false;
                break;
        }
    }

    /**
     */
    public function startTrans()
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $this->db->StartTrans();
                break;
            case 'yii\\db\\Connection' :
                $this->yiiTransaction = $this->db->beginTransaction();
                // $db->execute($sql);
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $this->db->getDriver()->getConnection()->beginTransaction();
                break;
            default :
                ;
                break;
        }
    }

    /**
     */
    public function failTrans()
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $this->db->FailTrans();
                break;
            case 'yii\\db\\Connection' :
                if (isset($this->yiiTransaction)) {
                    $this->yiiTransaction->rollBack();
                }
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $this->db->getDriver()->getConnection()->rollback();
                break;
            default :
                ;
                break;
        }
    }

    /**
     */
    public function rollbackTrans()
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $this->db->FailTrans();
                $this->db->CompleteTrans();
                break;
            case 'yii\\db\\Connection' :
                if (isset($this->yiiTransaction)) {
                    $this->yiiTransaction->rollBack();
                }
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $this->db->getDriver()->getConnection()->rollback();
                break;
            default :
                ;
                break;
        }
    }

    /**
     */
    public function completeTrans()
    {
        switch ($this->connectionType) {
            case 'ADODB' :
                $this->db->CompleteTrans();
                break;
            case 'yii\\db\\Connection' :
                if (($this->db instanceof \yii\db\Transaction) && isset($this->yiiTransaction)) {
                    $this->yiiTransaction->commit();
                }
                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $this->db->getDriver()->getConnection()->commit();
                break;
            default :
                ;
                break;
        }
    }


    public function getQueryRecordCount()
    {
        $ret = null;
        switch ($this->connectionType) {
            case 'ADODB' :
                $ret = $this->queryResult->RecordCount();
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $ret = $this->queryResult->count();
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function getQueryRecordFieldsCount()
    {
        $ret = null;
        switch ($this->connectionType) {
            case 'ADODB' :
                $ret = $this->queryResult->FieldCount();
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $ret = $this->queryResult->getFieldCount();
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function rewindQueryRecord()
    {
        $ret = null;
        switch ($this->connectionType) {
            case 'ADODB' :
                $ret = $this->queryResult->moveFirst();
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $rs = $this->getQueryResultSet();
                $rs->buffer();
                $ret = $rs->rewind();
                break;
            default :
                ;
                break;
        }
        return $ret;
    }

    public function nextQueryRecord()
    {
        $ret = null;
        switch ($this->connectionType) {
            case 'ADODB' :
                $ret = $this->queryResult->moveNext();
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $rs = $this->getQueryResultSet();
                $ret = $rs->next();
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function currentQueryRecord()
    {
        $ret = null;
        switch ($this->connectionType) {
            case 'ADODB' :
                $ret = $this->queryResult->fetchObj();
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $rs = $this->getQueryResultSet();
                $ret = $rs->current();
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function queryRecordSetToHtml($attributes = '')
    {
        $html = '';
        $attributes = trim($attributes);

        $rs = $this->getQueryResultSet();

        if (!isset($rs) || $rs->count()===0) {
            return $html;
        }

        $html .= <<<HTML
<table $attributes>

HTML;

        // Header
        $rs->rewind();
        foreach ($rs as $r) {

            $html .= <<<HTML
    <thead>
        <tr>
HTML;

            foreach ($r as $col => $val) {
                $html .= <<<HTML
            <th>$col</th>
HTML;

            }

            $html .= <<<HTML
        </tr>
    </thead>
HTML;

            break;
        } // Header -- end
        // Body

        $html .= <<<HTML
    <tbody>
HTML;


        $rs->rewind();
        foreach ($rs as $r) {

            $html .= <<<HTML
        <tr>
HTML;

            foreach ($r as $col => $val) {
                $html .= <<<HTML
            <td>$val</td>
HTML;

            }

            $html .= <<<HTML
        </tr>
HTML;

        } // Body -- end
        $html .= <<<HTML
    </tbody>
HTML;

        $html .= <<<HTML
</table>

HTML;

        return $html;
    }


    /**
     * @param array $arr
     * @return bool
     */
    protected function isAssoc(array $arr)
    {
        return self::isAssocStatic($arr);
    }

    protected static function isAssocStatic(array $arr)
    {
        if (array() === $arr)
            return false;
        return array_keys($arr) !== range(0, count($arr) - 1);
    }


    /**
     * @param callable $callback
     * @param array $arr
     * @return array
     */
    protected static function arrayKeyMap(callable $callback, array $arr = [])
    {
        $result = [];
        array_walk($arr, function ($value, $key) use ($callback, &$result) {
            $result[$key] = $callback($value, $key);
        });
        return $result;
    }

    public function createParameterizedUpdateSqlString($tabnam, $fields_or_record, $where)
    {
        return self::createParameterizedUpdateSqlStringStatic($tabnam, $fields_or_record, $where, $this->db);
    }

    public static function createParameterizedUpdateSqlStringStatic($tabnam, $fields_or_record, $where, $db = null)
    {
        $keys = self::isAssocStatic($fields_or_record) ? array_keys($fields_or_record) : $fields_or_record;
        return "UPDATE $tabnam SET "
            . implode(
                ', ',
                array_map(
                    function ($key) use ($db) {
                        return self::quoteIdentifierStatic($key, $db) . " = " . self::formatParameterNameStatic($key, $db);
                    },
                    $keys
                )
            )
            . " WHERE $where";
    }

    public function createParameterizedInsertSqlString($tabnam, $fields_or_record)
    {
        return self::createParameterizedInsertSqlStringStatic($tabnam, $fields_or_record, $this->db);
    }

    public static function createParameterizedInsertSqlStringStatic($tabnam, $fields_or_record, $db = null)
    {
        $keys = self::isAssocStatic($fields_or_record) ? array_keys($fields_or_record) : $fields_or_record;
        return "INSERT INTO $tabnam ("
            . implode(', ', array_map(
                    function ($key) use ($db) {
                        return self::quoteIdentifierStatic($key, $db);
                    },
                    $keys
                )
            ) . ") VALUES ("
            . implode(', ', array_map(
                    function ($key) use ($db) {
                        return self::formatParameterNameStatic($key, $db);
                    },
                    $keys
                )
            ) . ")";
    }

    public function quoteIdentifier($name)
    {
        return self::quoteIdentifierStatic($name, $this->db);
    }

    public static function quoteIdentifierStatic($name, $db = null)
    {
        $ret = $name;
        switch (self::getConnectionTypeStatic($db)) {
            case 'ADODB' :

                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                if ($db instanceof \Zend\Db\Adapter\Adapter) {
                    $ret = $db->platform->quoteIdentifier($name);
                }
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function formatParameterName($name)
    {
        return self::formatParameterNameStatic($name, $this->db);
    }

    public static function formatParameterNameStatic($name, $db = null)
    {
        $ret = $name;
        switch (self::getConnectionTypeStatic($db)) {
            case 'ADODB' :

                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                if ($db instanceof \Zend\Db\Adapter\Adapter) {
                    $ret = $db->driver->formatParameterName($name);
                }
                break;
            default :
                ;
                break;
        }
        return $ret;
    }

    public function getEmptyTableRecord($tabnam = null, $fields = null)
    {
        $ret = [];
        if (is_string($fields)) {
            $fields = explode(',', $fields);
        }

        switch ($this->connectionType) {
            case 'ADODB' :
                $metaColumns = $this->db->metaColumns($tabnam, false);
                $metaColumnsFiltered = array_filter($metaColumns, function ($k) use ($fields) {
                    return array_key_exists($k, $fields);
                });

                foreach ($metaColumnsFiltered as $key => $value) {
                    $ret[$key] = array_key_exists('not_null', $value) && !$value['not_null'] ? null :
                        (array_key_exists('type', $value) && in_array($this->db->metaType($value['type']), ['C', 'C2', 'X', 'X2', 'XL']) ? '' : null);
                }
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $metadata = new \Zend\Db\Metadata\Metadata($this->db);
                $table = $metadata->getTable($tabnam);
                foreach ($table->getColumns() as $columnObj) {
                    if ($columnObj instanceof \Zend\Db\Metadata\Object\ColumnObject) {
                        $columnName = $columnObj->getName();
                        if (in_array($columnName, $fields)) {
                            $ret[$columnName] = $columnObj->isNullable() ? null :
                                ($columnObj->getDataType() == 'string' ? '' : 0);
                        }
                    }
                }
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    public function convertTableRecordValuesToExpectedDataType(string $tabnam, array $record)
    {
        $ret = [];

        switch ($this->connectionType) {
            case 'ADODB' :
                $metaColumns = $this->db->metaColumns($tabnam, false);
                $metaColumnsFiltered = array_filter($metaColumns, function ($k) use ($record) {
                    return array_key_exists($k, $record);
                });

                foreach ($metaColumnsFiltered as $key => $value) {
                    $typeRaw = array_key_exists('type', $value) && $this->db->metaType($value['type']) !== null
                        ? $this->db->metaType($value['type'])
                        : null;

                    $ret[$key] = self::convertValueToExpectedtype($value, self::convertAdodbRawTypeToExpectedType($typeRaw));

                }
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $keys = array_keys($record);
                $metadata = new \Zend\Db\Metadata\Metadata($this->db);
                $table = $metadata->getTable($tabnam);
                foreach ($table->getColumns() as $columnObj) {
                    if ($columnObj instanceof \Zend\Db\Metadata\Object\ColumnObject) {
                        $columnName = $columnObj->getName();
                        if (in_array($columnName, $keys)) {
                            $value = $record[$columnName];
                            $expectedType = $columnObj->getDataType();
                            $newValue = self::convertValueToExpectedtype($value, $expectedType);
                            $ret[$columnName] = $columnObj->isNullable() && empty($newValue) ? null : $newValue;
                        }
                    }
                }
                break;
            default :
                ;
                break;
        }
        return $ret;
    }


    /**
     * @param string $tabnam
     * @param string $fieldname
     * @param array $record
     * @return bool|float|int|string|null
     */
    public function convertTableRecordValueToExpectedDataTypeByFieldName(string $tabnam, string $fieldname, array $record)
    {
        $retval = false;

        switch ($this->connectionType) {
            case 'ADODB' :
                $metaColumns = $this->db->metaColumns($tabnam, false);
                $metaColumn = $metaColumns[$fieldname];
                $typeRaw = array_key_exists('type', $metaColumn) && $this->db->metaType($metaColumn['type']) !== null
                    ? $this->db->metaType($metaColumn['type'])
                    : null;

                $retval = self::convertValueToExpectedtype($metaColumn, self::convertAdodbRawTypeToExpectedType($typeRaw));
                break;
            case 'yii\\db\\Connection' :

                break;
            case 'Zend\\Db\\Adapter\\Adapter' :
                $keys = array_keys($record);
                $metadata = new \Zend\Db\Metadata\Metadata($this->db);

                $columnObj = $metadata->getColumn($fieldname, $tabnam);
                $columnName = $fieldname;
                if (in_array($columnName, $keys)) {
                    $value = $record[$columnName];
                    $expectedType = $columnObj->getDataType();
                    $newValue = self::convertValueToExpectedtype($value, $expectedType);
                    $retval = $columnObj->isNullable() && empty($newValue) ? null : $newValue;
                }

                break;
            default :
                ;
                break;
        }
        return $retval;
    }


    /**
     * @param string $typeRaw
     * @return string
     */
    protected static function convertAdodbRawTypeToExpectedType(string $typeRaw): string
    {
        switch ($typeRaw) {
            case 'C':
            case 'C2':
            case 'X':
            case 'X2':
            case 'XL':
                $expectedType = 'string';
                break;
            case 'B':
                $expectedType = 'blob';
                break;
            case 'D':
                $expectedType = 'date';
                break;
            case 'T':
                $expectedType = 'timestamp';
                break;
            case 'L':
                $expectedType = 'boolean';
                break;
            case 'I':
                $expectedType = 'integer';
                break;
            case 'N':
                $expectedType = 'double';
                break;
            default:
                $expectedType = 'unknown type';
        }
        return $expectedType;
    }


    /**
     * @param array $opt
     * @param array $filterFunctions
     * @return mixed
     * @throws \Exception
     */
    public static function parseRecordValueFilters(array &$opt, $filterFunctions = [])
    {
        if (!array_key_exists('fieval', $opt)) {
            throw new \Exception('First option parameter \$opt does\'t contain expected keys in argument ');
        }
        foreach ($filterFunctions AS $filterFunction) {
            if (isset($filterFunction) && is_callable($filterFunction)) {
                $opt['fieval'] = call_user_func_array($filterFunction, [$opt]);
            }
        }
        return $opt['fieval'];
    }

    /**
     * @param $value
     * @param $expectedType
     * @return bool|float|int|string
     */
    protected static function convertValueToExpectedtype($value, $expectedType)
    {

        $type = gettype($value);
        if ($type === 'string') {
            $value = trim($value);
        }

        switch ($expectedType) {
            case 'boolean':
                if ($type !== 'boolean') {
                    switch (strtolower("$value")) {
                        case '1':
                        case 'true':
                        case 't':
                        case 'on':
                        case 'yes':
                        case 'y':
                            $newValue = true;
                            break;
                        default:
                            $newValue = false;
                    }
                } else {
                    $newValue = $value;
                }
                break;
            case 'double':
                $newValue = is_numeric($value) ? floatval("$value") : $value;
                break;
            case 'integer':
                $newValue = is_numeric($value) ? intval("$value") : $value;
                break;

            default:
                $newValue = $value;

        }


        return $newValue;
    }


    /**
     * @param $fieldnames
     * @return array|false
     */
    public static function createTableRecordByFieldnames($fieldnames)
    {
        if (gettype($fieldnames) === 'string') {
            $fieldnames = preg_replace('/[\s]*,[\s]*/', ',', trim($fieldnames));
            $record_keys = explode(",", $fieldnames);
        } elseif (gettype($fieldnames) === 'array') {
            $record_keys = $fieldnames;
        } else {
            // Error !!!
            return array();
        }

        function map_filter($str)
        {
            // Matches the cases in order schema.column as (alias), schema.(column) and (column)
            return ((bool)preg_match("/.+?[\\.].+\\s+as\\s+(.*)|.+?[\\.](.*)|(.*)/", $str, $matches)) ? "${matches[1]}${matches[2]}${matches[3]}" : $str;
        }


        if (!function_exists('array_fill_keys')) {
            function array_fill_keys($keys, $value = '')
            {
                return array_combine($keys, array_fill(0, count($keys), $value));
            }
        }

        return array_fill_keys(array_unique(array_map("map_filter", $record_keys)), "");  // Php 5.2 <
    }


    /**
     * @param $callable
     * @return int
     * @throws \ReflectionException
     */
    protected static function getNrOfParams($callable)
    {
        $CReflection = is_array($callable) ?
            new \ReflectionMethod($callable[0], $callable[1]) :
            new \ReflectionFunction($callable);
        return $CReflection->getNumberOfParameters();
    }
}
