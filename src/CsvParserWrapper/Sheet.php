<?php
namespace nordgen\DbBatch\CsvParserWrapper;
use Box\Spout\Reader\SheetInterface;
use nordgen\DbBatch\CsvParser\CsvParser;
/**
 * Class Sheet
 *
 * @package Box\Spout\Reader\CSV
 */
class Sheet implements SheetInterface
{
    /** @var \Box\Spout\Reader\CSV\RowIterator To iterate over the CSV's rows */
    protected $rowIterator;
    protected $parser;
    /**
     * @param resource $filePointer Pointer to the CSV file to read
     * @param string $fieldDelimiter Character that delimits fields
     * @param string $fieldEnclosure Character that enclose fields
     * @param string $encoding Encoding of the CSV file to be read
     * @param \Box\Spout\Common\Helper\GlobalFunctionsHelper $globalFunctionsHelper
     */
    public function __construct($filePointer, $fieldDelimiter, $fieldEnclosure, $encoding, $endOfLineCharacter, $globalFunctionsHelper)
    {
    	$meta_data = stream_get_meta_data($filePointer);
    	$filepath = $meta_data["uri"];
    	$this->parser = CsvParser::fromFile($filepath,['encoding' => 'UTF8', 'delimiter' => $fieldDelimiter, 'enclosure' => $fieldEnclosure, 'header'=>false]);
        $this->rowIterator = $this->parser->getIterator();
    }
    /**
     * @api
     * @return \Box\Spout\Reader\CSV\RowIterator
     */
    public function getRowIterator()
    {
        return $this->rowIterator;
    }
}