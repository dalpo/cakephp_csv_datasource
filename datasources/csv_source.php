<?php
/**
 * CakePHP CSV datasource
 *
 * Licensed under The MIT License
 * Redistributions of files must retain the above copyright notice.
 *
 * Originally based on http://bakery.cakephp.org/articles/view/csv-datasource-for-reading-your-csv-files
 *
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @author Andrea Dal Ponte <dalpo85@gmail.com> - http://github.com/dalpo/cakephp_csv_datasource

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

  protected $_fileHeader  = null;
  protected $_rowNumber = null;

  /**
   * Default configuration.
   *
   * @public unknown_type
   */
  public $_baseConfig = array(
          'datasource' => 'csv',
          'path' => '.', // local path on the server relative to WWW_ROOT
          'recursive' => false, // only false is supported at the moment
          'delimiter' => ',',
          'header_row' => 1
  );

  /**
   * Constructor
   */
  function __construct($config = null, $autoConnect = true) {
    parent::__construct($config);
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
    parent::__destruct();
  }


  /**
   * Open csv file
   */
  function connect() {
    $this->connected = false;
    if($this->_initConnection()) {
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
    if($this->fields) {
      $this->__getDescriptionFromFirstLine($model);
    }
    return $this->fields;
  }

  /**
   * __getDescriptionFromFirstLine and store into class variables
   *
   */
  private function __getDescriptionFromFirstLine() {
    if(!$this->connected || $this->_rowNumber != $this->config['header_row']) {
      $this->_initConnection();
    }
    $columns = fgetcsv($this->connection, 0, $this->config['delimiter']);
    $this->fields = $columns;
    $this->maxCol = count($columns);
    $this->_initConnection();

    return (bool)$this->maxCol;
  }

  protected function _initConnection() {
    $this->_rowNumber = 0;
    $this->_fileHeader = '';
    if($this->connection = fopen($this->config['path'], "r+")) {
      while( ++$this->_rowNumber < $this->config['header_row'] && !feof($this->connection) ) {
        $this->_fileHeader.= fgets($this->connection);
      }
//      debug($this->_fileHeader);die(__LINE__);
      return $this->connected = true;
    } else {
      return $this->connected = false;
    }
  }

  /**
   * Closes the current datasource connection.
   */
  function close() {
    if ($this->connected || $this->connection || $this->_rowNumber) {
      @fclose($this->connection);
      $this->connection = null;
      $this->connected = false;
      $this->_rowNumber = 0;
    }
    return true;
  }

  /**
   *
   */
  function read(&$model, $queryData = array(), $recursive = null) {
    if (!$this->connected || ($this->_rowNumber != $this->config['header_row']) ) {
      $this->_initConnection();
    }

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
    if(!$this->limit) {
      $this->page = 1;
    }


    // Daten werden aus der Datei in ein Array $data gelesen ??? cruken..
    while ( $data = $this->_getNextRow() ) {
//      Debugger::dump($data);
//      die();
//      if ($this->_rowNumber++ <= $this->config['header_row']) { //??? è necessario???
//        continue;
//      } else {
      $recordCount++;

      if($this->limit) {
        $currentPage = floor( ($recordCount - 1) / $this->limit) + 1;
      } else {
        $currentPage = 1;
      }


      // do have have reached our requested page ?
      if ($this->page != $currentPage) {
        continue;
      }

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
      if ( $breakConditions ) {
        break;
      }

    }
//    }

    return $resultSet;
  }

  /**
   * Private helper method to remove query metadata in given data array.
   *
   * @param array $data
   * @return array
   */
  private function __scrubQueryData($data) {
    foreach (array('conditions', 'fields', 'joins', 'order', 'limit', 'offset', 'group') as $key) {
      if (!isset($data[$key]) || empty($data[$key])) {
        $data[$key] = array();
      }
    }
    return $data;
  }


  /**
   * Get the next cvs row
   */
  protected function _getNextRow() {
    if(!$this->connection) {
      $this->_initConnection();
    }

    if( !feof($this->connection) && ($data = fgetcsv($this->connection, 0, $this->delimiter)) ) {
      $this->_rowNumber++;
      return $data;
    } else {
      return false;
    }


  }

}

?>