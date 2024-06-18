<?php

namespace nordgen\DbBatch\Models\CsvParser\Iterator;

use Iterator;

/**
 * Iterator for converting csv lines to array
 *
 * @author Kazuyuki Hayashi <hayashi@valnur.net>
 */
class CsvIterator implements Iterator
{

    /**
     * @var Iterator $iterator
     */
    private $iterator;


    /**
     * @var array $option
     */
    private $option = array();
    /**
     * @var int $key Current index
     */
    private $key = 0;
    /**
     * @var string $revert
     */
    private $revert = '';
    /**
     * @var bool $continue
     */
    private $continue = false;
    /**
     * @var array $result
     */
    private $result = array();
    /**
     * @var int $col
     */
    private $col = 0;

    /**
     * @param Iterator $iterator
     * @param array $option
     */
    public function __construct(Iterator $iterator, array $option = array())
    {
        $this->iterator = $iterator;
        $this->option = array_merge(array(
            'delimiter' => ',',
            'enclosure' => '"',
            'encoding' => 'CP932',
            'header' => false
        ), $option);
    }

    /**
     * @return array|bool[]|string[]
     */
    public function getOption()
    {
        return $this->option;
    }

    /**
     * Return the current element
     *
     * @link http://php.net/manual/en/iterator.current.php
     *
     * @return mixed Can return any type.
     */
    public function current()
    {
        $this->result = array();

        // revert necessary delimiter
        $this->revert = $this->option['delimiter'];

        // loop over the lines
        while ($this->iterator->valid()) {
            $observe = false;

            $line = $this->iterator->current();
            $line = mb_convert_encoding($line, 'UTF-8', isset($this->option['encoding']) ? $this->option['encoding'] : 'auto');

            //echo "$line\n";
            //echo "delimiter=[".$this->option['delimiter']."]\n";

            //echo "header=";
            //var_dump($this->option['header']);

            // split the line by 'delimiter'
            $tokens = explode($this->option['delimiter'], $line);

            // loop over the columns
            foreach ($tokens as $value) {
                //echo "\$value=[$value]\n";
                //if ($value=='1407') {
                //    $observe=true;
                //    echo "$line\n";
                //}
                //if ($this->col==0) {
                //    echo ($this->option['filepath'] ?: '').".accide=[$value]\n";
                //}


                $value = preg_replace('/"(\r\n|\r|\n)*$/', '"', $value);

                //if ($observe) {
                //echo "accide=[$value]\n";
                //}


                // check the first letter is 'enclosure' or not
                if (!$this->continue && substr($value, 0, 1) == $this->option['enclosure']) {
                    // check the last letter is not 'enclosure'
                    if (($last = substr($value, -1)) != $this->option['enclosure']) {
                        //echo "Case: A1\n";
                        $this->processContinuousField($value, $this->option);
                        // check if string length is 1, then string started with delimiter character
                        // or two last character are '""'
                    } elseif (strlen($value) == 1 || ($twolast = substr($value, -2)) == '""' && $this->option['enclosure'] !== '"') {
                        //echo "Case: A2\n";
                        $this->processContinuousField($value, $this->option);
                        // check if second last character is not 'enclosure'
                    } elseif ($twolast !== ($this->option['enclosure'] . $this->option['enclosure'])) {
                        //echo "Case: A3\n";
                        $this->processEnclosedField($value, $this->option);
                        // check if the last characters are pairs of 'enclosure'
                    } elseif ($this->option['enclosure'] === '"' && preg_match('/[^\"]((\"\")+$)/', $value)) {
                        //echo "Case: A4\n";
                        $this->processContinuousField($value, $this->option);
                    } else {
                        //echo "Case: A5\n";
                        $this->processEnclosedField($value, $this->option);
                    }

                } else { // first letter is NOT 'enclosure'
                    if ($this->continue) {
                        // check the last letter is not 'enclosure'
                        if (($tokenlength = strlen($value)) == 0 || ($last = substr($value, -1)) !== $this->option['enclosure']) {
                            //echo "Case: B1\n";
                            $this->processContinuousField($value, $this->option);
                        } elseif ($last === $this->option['enclosure'] && strlen($value) == 1) {
                            //echo "Case: B2\n";
                            $this->processClosingField($value, $this->option);
                            // check if string length is 0, then string started with delimiter character
                            // or two last character are '""'
                        } elseif (($twolast = substr($value, -2)) === '""' && $this->option['enclosure'] !== '"') {
                            //echo "Case: B3\n";
                            $this->processContinuousField($value, $this->option);
                            // check if second last character is not 'enclosure'
                        } elseif ($twolast !== ($this->option['enclosure'] . $this->option['enclosure']) && $last === $this->option['enclosure']) {
                            //echo "Case: B4\n";
                            $this->processClosingField($value, $this->option);
                            // check if the last characters are pairs of 'enclosure'
                        } elseif ($this->option['enclosure'] === '"' && preg_match('/[^\"]((\"\")+$)/', $value)) {
                            //echo "Case: B5\n";
                            $this->processContinuousField($value, $this->option);
                        } else {
                            //echo "Case: B6\n";
                            $this->processClosingField($value, $this->option);
                        }

                        // check the last letter is 'enclosure'
                    } elseif (substr($value, -1) == $this->option['enclosure']) {
                        //echo "Case: B7\n";
                        $this->processClosingField($value, $this->option);
                    } else {
                        //echo "Case: B8\n";
                        $this->processField($value, $this->option);
                    }
                }

                if ($this->revert == "") {
                    $this->revert = $this->option['delimiter'];
                }
            }

            // If the cell is closed, reset the column index and go to next row.
            if (!$this->continue) {
                //echo "Closing cell.\n";
                $this->col = 0;
                break;
            }

            $this->revert = "";
            $this->iterator->next();
        }


        return $this->result;
    }

    /**
     * Checks if current position is valid
     *
     * @link http://php.net/manual/en/iterator.valid.php
     *
     * @return boolean The return value will be casted to boolean and then evaluated.
     *          Returns true on success or false on failure.
     */
    public function valid()
    {
        return $this->iterator->valid();
    }

    /**
     * Process enclosed and multiple line field
     *
     * example: "value\n
     *
     * @param string $value Current token
     * @param array $option Option
     */
    private function processContinuousField($value, array $option)
    {
        //echo __FUNCTION__."\n";
        $cell = $this->trimLeftEnclosure($value, $option['enclosure']);
        $cell = $this->unescapeEnclosure($cell, $option['enclosure']);

        if ($this->continue) {
            $this->joinCell($this->revert . $cell);
//            return;
        } else {
            $this->setCell($cell);
        }

        $this->continue = true;
    }

    /**
     * Strip an enclosure string from beginning of the string
     *
     * @param string $value
     * @param string $enclosure
     * @return string
     */
    private function trimLeftEnclosure($value, $enclosure)
    {
        if (substr($value, 0, 1) == $enclosure) {
            $value = substr($value, 1);
        }

        return $value;
    }

    /**
     * Convert double enclosure to single enclosure
     *
     * @param $value
     * @param $enclosure
     *
     * @return mixed
     */
    private function unescapeEnclosure($value, $enclosure)
    {
        return str_replace(str_repeat($enclosure, 2), $enclosure, $value);
    }

    /**
     * Append value to current cell
     *
     * @param string $cell
     */
    private function joinCell($cell)
    {
        $this->result[$this->getIndexForColumn($this->col)] .= $cell;
    }

    /**
     * @param $column
     */
    private function getIndexForColumn($column)
    {
        if (is_array($this->option['header']) && isset($this->option['header'][$column])) {
            return $this->option['header'][$column];
        }

        return $column;
    }

    /**
     * Set value to current cell
     *
     * @param string $cell
     */
    private function setCell($cell)
    {
        $this->result[$this->getIndexForColumn($this->col)] = $cell;
    }

    /**
     * Process enclosed field
     *
     * example: "value"
     *
     * @param string $value Current token
     * @param array $option Option
     */
    private function processEnclosedField($value, array $option)
    {
        //echo __FUNCTION__."\n";
        // then, remove enclosure and line feed
        $cell = $this->trimEnclosure($value, $option['enclosure']);

        // replace the escape sequence "" to "
        $cell = $this->unescapeEnclosure($cell, $option['enclosure']);

        $this->setCell($cell);

        // Check if last char and the one before are ", if that are true, continue
        //if (substr($value, -2) == $this->option['enclosure'].$this->option['enclosure'] && substr($value, -3, 1) != $this->option['enclosure']) {
        //    $this->continue = true;
        //} else {
        $this->col++;
        $this->continue = false;
        //}
    }

    /**
     * String enclosure string from beginning and end of the string
     *
     * @param string $value
     * @param string $enclosure
     * @return string
     */
    private function trimEnclosure($value, $enclosure)
    {
        $value = $this->trimLeftEnclosure($value, $enclosure);
        $value = $this->trimRightEnclosure($value, $enclosure);

        return $value;
    }

    /**
     * Strip an enclosure string from end of the string
     *
     * @param string $value
     * @param string $enclosure
     * @return string
     */
    private function trimRightEnclosure($value, $enclosure)
    {
        if (substr($value, -1) == $enclosure) {
            $value = substr($value, 0, -1);
        }

        return $value;
    }

    /**
     * Process end of enclosure
     *
     * example: value"
     *
     * If previous token was not closed, this token is joined,
     * otherwise this token is a new cell.
     *
     * @param string $value Current token
     * @param array $option Option
     */
    private function processClosingField($value, array $option)
    {
        //echo __FUNCTION__."\n";
        //echo "col:".$this->col."\n";


        // Check if last char and the one before are ", if that are true, continue
        //if (substr($value, -2) == $this->option['enclosure'].$this->option['enclosure'] && substr($value, -3, 1) != $this->option['enclosure']) {
        //echo "Fake ".__FUNCTION__."\n";
        //    $cell = $this->unescapeEnclosure($value, $option['enclosure']);
        //    $this->continue = true;
        //} else {
        $cell = $this->trimRightEnclosure($value, $option['enclosure']);
        $cell = $this->unescapeEnclosure($cell, $option['enclosure']);

        $this->joinCell($this->revert . $cell);

        $this->col++;
        $this->continue = false;
        //}
        //$this->continue = false;
        //$this->col++;
    }

    /**
     * Process unenclosed field
     *
     * example: value
     *
     * @param string $value Current token
     * @param array $option Option
     */
    private function processField($value, array $option)
    {
        //echo __FUNCTION__."\n";
        if ($this->continue) {
            $cell = $this->unescapeEnclosure($value, $option['enclosure']);
            $this->joinCell($this->revert . $cell);
        } else {
            $cell = rtrim($value);
            $cell = $this->unescapeEnclosure($cell, $option['enclosure']);
            $this->setCell($cell);
            $this->col++;
        }
    }

    /**
     * Move forward to next element
     *
     * @link http://php.net/manual/en/iterator.next.php
     *
     * @return void Any returned value is ignored.
     */
    public function next()
    {
        $this->iterator->next();
        $this->key++;
    }

    /**
     * Return the key of the current element
     *
     * @link http://php.net/manual/en/iterator.key.php
     *
     * @return mixed scalar on success, or null on failure.
     */
    public function key()
    {
        return $this->key;
    }

    /**
     * Rewind the Iterator to the first element
     *
     * @link http://php.net/manual/en/iterator.rewind.php
     *
     * @return void Any returned value is ignored.
     */
    public function rewind()
    {
        $this->iterator->rewind();
        $this->key = 0;
    }

}