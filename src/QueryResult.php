<?php


namespace nordgen\DbBatch;

use Iterator;
use Countable;

/**
 * @author kal
 *
 */
class QueryResult implements Iterator,Countable {
	/**
	 * @var unknown
	 */
	protected $queryResult;
	
	protected $classtype;
	
	/**
	 * @return unknown
	 */
	public function getQueryResult() {
		return $this->queryResult;
	}
	
	public function getClassType() {
		if (!isset($this->classType)) {
			$this->classtype = get_class($this->queryResult);
		    $this->classtype = preg_filter("/^ADORecordSet(.+)/", "ADORecordSet", $this->classtype);
		}
	    return $this->classtype;
	}
	
	/**
	 * @param unknown $queryResult
	 */
	public function __construct($queryResult) {
		
		$this->queryResult = $queryResult;
	}
	
	
	public function current () 
	{
		switch ($this->getClassType()) {
			case 'ADORecordSet':
				$rownum = $this->queryResult->CurrentRow();
				$row = $this->queryResult->FetchRow();
				$this->queryResult->Move($rownum);
				return $row;
			break;
			
			case 'yii\\db\\DataReader':
				return $this->queryResult->current();
				break;
			
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->current();
				}
			break;
		}
		
	}
	
	public function next () 
	{
		switch ($this->getClassType()) {
			case 'ADORecordSet':
				return $this->queryResult->MoveNext();
				break;
					
			case 'yii\\db\\DataReader':
				return $this->queryResult->next();
				break;
					
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->next();
				}
				break;
		}
		
	}
	
	public function key () 
	{
		switch ($this->getClassType()) {
			case 'ADORecordSet':
				return $this->queryResult->CurrentRow();
				break;
					
			case 'yii\\db\\DataReader':
				return $this->queryResult->key();
				break;
					
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->key();
				}
				break;
		}
	}
	
	public function valid () 
	{
		switch ($this->getClassType()) {
			case 'ADORecordSet':
				return true;
				break;
					
			case 'yii\\db\\DataReader':
				return $this->queryResult->valid();
				break;
					
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->valid();
				}
				break;
		}
	}
	
	public function rewind () 
	{
		switch ($this->getClassType()) {
			case 'ADORecordSet':
				return $this->queryResult->Move(0);
				break;
					
			case 'yii\\db\\DataReader':
				return $this->queryResult->rewind();
				break;
					
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->rewind();
				}
				break;
		}		
	}
	
	
	public function count() {
			switch ($this->getClassType()) {
			case 'ADORecordSet':
				return $this->queryResult->RecordCount();
				break;
					
			case 'yii\\db\\DataReader':
				return $this->queryResult->count();
				break;
					
			default:
				if ($this->queryResult instanceof Interator) {
					return $this->queryResult->count();
				}
				break;
		}		
	}
	
	
	public function read()
	{
		$row = $this->current();
		$this->next();
		return $row;
	}
	
	
	public function readAll()
	{
		$result = [];
		$this->rewind();
		foreach ($this->queryResult as $row) {
			$result[] = $row;
		}
		return $result;
		//return $this->queryResult->current();
	}
	
}
