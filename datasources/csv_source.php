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
 *
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
  public $limit = false;
  public $page = 1;

  protected $_fileHeader  = null;
  protected $_rowNumber = 0;

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
    if(!$this->fields) {
      $this->__getDescriptionFromFirstLine($model);
    }
    return $this->fields;
  }

  /**
   * __getDescriptionFromFirstLine and store into class variables
   *
   */
  private function __getDescriptionFromFirstLine() {
    if(!$this->config['header_row']) {
      return false;
    }
    if(!$this->connected || $this->_rowNumber != $this->config['header_row']) {
      $this->_initConnection();
    }
    $columns = $this->_getNextRow();
    $this->fields = array_map(function($column) {
      return (trim(strtolower($column)));
    }, $columns);
    $this->maxCol = count($columns);

    return (bool)$this->maxCol;
  }

  protected function _initConnection() {
    if($this->connected) {
      $this->close();
    }
    $this->_rowNumber = 0;
    $this->_fileHeader = '';
    if($this->connection = fopen($this->config['path'], "r+")) {
      while( !feof($this->connection) && ($this->_rowNumber + 1) < $this->config['header_row'] ) {
        $this->_rowNumber++;
        $this->_fileHeader.= fgets($this->connection);
      }
//      debug($this->_fileHeader);
//      die();
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
    if (!$this->connected) {
      $this->_initConnection();
    }

    if (isset($queryData['page']) && !empty($queryData['page'])) {
      $this->page = $queryData['page'];
    }

    // get the limit
    if (isset($queryData['limit']) && !empty($queryData['limit'])) {
      $this->limit = (int) $queryData['limit'];
    }
    // generate an index array of all wanted fields
    if (empty($queryData['fields'])) {
      $fields = $this->fields;
      $allFields = true;
    } else {
      $fields = $queryData['fields'];
      $allFields = false;
      $_fieldIndex = array();
      $index = 0;
      foreach($this->fields as $field) {
        if (in_array($field,  $fields)) {
          $_fieldIndex[] = $index;
        }
        $index++;
      }
    }

    // retrive data
    $resultSet = array();
    $recordCount = 0;
    while ( (!$this->limit || ( $recordCount < ($this->limit * $this->page) - 1 ) ) && ($data = $this->_getNextRow()) ) {
      $recordCount = $this->_rowNumber - $this->config['header_row'];
      if ($this->_rowNumber  <= $this->config['header_row']) {
        continue;
      }

      if($this->limit) {
        $_currentPage = floor($recordCount / $this->limit) + 1;
        if ($this->page > $_currentPage) {
          continue;
        }
      }

      $record = array();

      if ($allFields) {

        $i = 0;
        $record['id'] = $recordCount;
        foreach($fields as $field) {
          $record[trim(strtolower($field))] = $data[$i++];
        }

      } else {

        $record['id'] = $recordCount;
        if (count($_fieldIndex) > 0) {
          foreach($_fieldIndex as $i) {
            $record[$this->fields[$i]] = $data[$i];
          }
        }

      }
      $record['_row_number'] = $this->_rowNumber;
      $resultSet[] = array($model->alias => $record);
      unset($record);
    }

    $this->_initConnection();

    /*Filtering results*/
    if (isset($queryData['conditions'])) {
      if (!empty($queryData['conditions'])) {
        $filtered_data = array();
        foreach ($resultSet as $id => $row) {
          $total_conditions = count($queryData['conditions']);
          $iterator_conditions = 0;
          foreach ($queryData['conditions'] as $column_operator => $value) {
            list($column, $operator) = array_pad(explode(' ', $column_operator, 2), 2, null);
            if (isset($row[$model->alias][$column])) {
              switch ($operator) {
                case '>=':
                  if ($row[$model->alias][$column] >= $value) {
                    $iterator_conditions += 1;
                  }
                  break;
                case '>':
                  if ($row[$model->alias][$column] > $value) {
                    $iterator_conditions += 1;
                  }
                  break;
                case '=':
                  if ($row[$model->alias][$column] == $value) {
                    $iterator_conditions += 1;
                  }
                  break;
                case '<':
                  if ($row[$model->alias][$column] < $value) {
                    $iterator_conditions += 1;
                  }
                  break;
                case '<=':
                  if ($row[$model->alias][$column] <= $value) {
                    $iterator_conditions += 1;
                  }
                  break;
                default:
                  if ($row[$model->alias][$column] == $value) {
                    $iterator_conditions += 1;
                  }
                  break;
              }
            }
          }
          if ($total_conditions == $iterator_conditions) {
            $filtered_data[] = $row;
          }
        }
        $resultSet = $filtered_data;
      }
    }

    $this->data = $resultSet;

    if ($model->findQueryType === 'count') {
      return array(array(array('count' => count($resultSet))));
    } else {
      return $this->data;
    }
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
    if(!$this->connected) {
      $this->_initConnection();
    }

    if( !feof($this->connection) && ($data = fgetcsv($this->connection, 0, $this->delimiter)) ) {
      $this->_rowNumber++;
      return $data;
    } else {
      return false;
    }

  }

  /**
   * Calculate
   *
   * @param Model $model
   * @param mixed $func
   * @param array $params
   * @return array
   * @access public
   */
  function calculate(&$model, $func, $params = array()) {
    return array('count' => true);
  }

  private function __arrayfilter($array, $callback = null) {
    if ($callback == null) {
        $callback = function($key, $val) {
            return (bool) $val;
        };
    }
    $return = array();
    foreach ($array as $key => $val) {
        if ($callback($key, $val)) {
            $return[$key] = $val;
        }
    }
    return $return;
  }

}

?>
