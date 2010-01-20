<?php
/**
 * CakePHP CSV datasource
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * based on http://bakery.cakephp.org/articles/view/csv-datasource-for-reading-your-csv-files
 *
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @author Andrea Dal Ponte <dalpo85@gmail.com>
 * @created 20/01/2009
 * @version 0.2
 **/

class CsvSource extends DataSource {

  /**
   * Description string for this Data Source.
   *
   * @public unknown_type
   */
  public $description = "CSV file datasource";

  public $delimiter = null; // delimiter between the columns
  public $maxCol = 0;
  public $fields = null;
  public $page = 1; // start always on the first page
  public $limit = 0; // just to make the chunks not too big
  protected $_fileHandler = null;
  protected $_fileHeader  = null;
  protected $_lineNumber = null;

  /**
   * Default configuration.
   *
   * @public unknown_type
   */
  public $_baseConfig = array(
          'datasource' => 'csv',
          'path' => '.', // local path on the server relative to WWW_ROOT
          'extension' => 'csv', // file extension of CSV Files
          'recursive' => false, // only false is supported at the moment
          'delimiter' => ',',
          'startline' => 0
  );

  /**
   * Constructor
   */
  function __construct($config = null, $autoConnect = true) {
    parent::__construct($config);
    $this->debug = Configure::read('debug') > 0;
    $this->fullDebug = Configure::read('debug') > 1;
    $this->connected = false;
    $this->delimiter = $this->config['delimiter'];
    if ($autoConnect) {
      return $this->connect();
    } else {
      return true;
    }
  }

  /**
   * Destructor
   */
  function __destruct() {
    $this->close();
  }


  /**
   * Open the csv file
   */
  function connect() {
    $this->connected = false;
    if($this->_initFilePointer()) {
      $this->connected = true;
      $this->__getDescriptionFromFirstLine();
    }
    return $this->connected;
  }

  /**
   * Returns a Model description (metadata) or null if none found.
   *
   * @return mixed
   **/
  function describe($model) {
    $this->__getDescriptionFromFirstLine($model);
    return $this->fields;
  }

  /**
   * __getDescriptionFromFirstLine and store into class variables
   *
   */
  private function __getDescriptionFromFirstLine() {
    if(!$this->connected) {
      $this->connect();
    }
    if($this->_lineNumber != $this->config['startline']) {
      $this->_initFilePointer();
    }
    $columns = fgetcsv($this->_fileHandler, 0, $this->config['delimiter']);
    $this->fields = $columns;
    $this->maxCol = count($columns);
    $this->_initFilePointer();

    return (bool)$this->maxCol;
  }

  protected function _initFilePointer() {
    $this->_lineNumber = 0;
    $this->_fileHeader = '';
    if($this->_fileHandler = fopen($this->config['path'], "r+")) {
      while( ++$this->_lineNumber < $this->config['startline'] && !feof($this->_fileHandler) ) {
        $this->_fileHeader.= fgets($this->_fileHandler);
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * File close
   */
  function close() {
    if ($this->connected || $this->_fileHandler) {
      @fclose($this->_fileHandler);
      $this->_fileHandler = null;
      $this->connected = false;
    }
  }

  /**
   * 
   */
  function read(&$model, $queryData = array(), $recursive = null) {
    if (!$this->connected) { $this->connect(); }
    if ($this->_lineNumber != $this->config['startline']) { $this->_initFilePointer(); }
    $queryData = $this->__scrubQueryData($queryData);

    // get the limit
    if (isset($queryData['limit']) && !empty($queryData['limit'])) {
      $this->limit = $queryData['limit'];
    }

    // get the page#
    if (isset($queryData['page']) && !empty($queryData['page'])) {
      $this->page = $queryData['page'];
    }

    if (empty($queryData['fields'])) {
      $fields = $this->fields;
      $allFields = true;
    } else {
      $fields = $queryData['fields'];
      $allFields = false;
      $_fieldIndex = array();
      $index = 0;
      // generate an index array of all wanted fields
      foreach($this->fields as $field) {
        if (in_array($field,  $fields)) {
          $_fieldIndex[] = $index;
        }
        $index++;
      }
    }

    $recordCount = 0;
    $resultSet = array();
    if(!$this->limit) { $this->page = 1; }

    // Daten werden aus der Datei in ein Array $data gelesen
    while (($data = fgetcsv($this->_fileHandler, 0, $this->delimiter))  && (!feof($this->_fileHandler)) ) {
      if ($this->_lineNumber++ <= $this->config['startline']) {
        continue;
      } else {
        $recordCount++;

        if($this->limit) {
          $currentPage = floor( ($recordCount - 1) / $this->limit) + 1;
        } else {
          $currentPage = 1;
        }
        

        // do have have reached our requested page ?
        if ($this->page != $currentPage) { continue; }

        $record = array();
        if ($allFields) {
          $i = 0;
          $record['id'] = $recordCount;
          foreach($fields as $field) {
            $record[$field] = $data[$i++];
          }
        } else {
          $record['id'] = $recordCount;
          if (count($_fieldIndex) > 0) {
            foreach($_fieldIndex as $i) {
              $record[$this->fields[$i]] = $data[$i];
            }
          }
        }
        $resultSet[] = array($model->alias => $record);
        unset($record);

        $breakConditions = $recordCount > ( $this->limit * $this->page);
        if ( $breakConditions ) { break; }

      }
    }
    
    return $resultSet;
  }

  /**
   * Private helper method to remove query metadata in given data array.
   *
   * @param array $data
   * @return array
   */
  function __scrubQueryData($data) {
    foreach (array('conditions', 'fields', 'joins', 'order', 'limit', 'offset', 'group') as $key) {
      if (!isset($data[$key]) || empty($data[$key])) {
        $data[$key] = array();
      }
    }
    return $data;
  }

}

?>