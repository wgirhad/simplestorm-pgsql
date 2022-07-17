<?php

namespace Wgirhad\SimplestOrm\Postgres;
use Exception;
use Throwable;
use Iterator;

class TableRecord implements Iterator {
    protected $data;
    protected $columns;
    protected $primaryKey;
    protected $table;
    protected $conn;

    function __construct($table, $data = null) {
        $this->conn = Conn::getInstance();

        if (!$this->conn->tableExists($table)) {
            throw new Exception("Table \"$table\" does not exist");
        }

        $this->table = $table;
        $this->columns    = $this->conn->fetchTableMeta($table);
        $this->primaryKey = $this->conn->fetchTablePK($table);

        if ($data === null) {
            $data = array();
            foreach ($this->columns as $field => $value) {
                $data[$field] = null;
            }
        }

        $data = array_combine(
            array_map('mb_strtolower', array_keys($data)),
            array_values($data)
        );

        $this->data = $data;
    }

    public function __isset($key) {
        $key = mb_strtolower($key);
        return isset($this->data[$key]);
    }

    function __set($key, $value) {
        $key = mb_strtolower($key);
        if (is_bool($value)) {
            $value = $value? 'true': 'false';
        }
        $this->data[$key] = $value;
    }

    function __get($key) {
        $key = mb_strtolower($key);
        return $this->data[$key] ?? null;
    }

    function getTable() {
        return $this->table;
    }

    function toArray() {
        return $this->data;
    }

    function post() {
        return $this->shouldInsert()? $this->insert(): $this->update();
    }

    function insert() {
        $this->stripInexistentFields();
        $this->dealWithPKOnInsert();

        $param = $this->assembleInsertQuery();

        return $this->parseResult($param[0], $param[1], true);
    }

    function update() {
        $this->stripInexistentFields();
        $param = $this->assembleUpdateQuery();

        return $this->parseResult($param[0], $param[1]);
    }

    function delete() {
        $this->stripInexistentFields();
        $param = $this->assembleDeleteQuery();

        return $this->parseResult($param[0], $param[1]);
    }

    protected function parseResult($sql, $param, $insert = false) {
        $result = array();

        try {
            if ($insert && empty($this->data[$this->primaryKey])) {
                $this->data[$this->primaryKey] = $this->conn->runInsert($sql, $param);
                $result["status"] = true;
            } else {
                $result["status"] = $this->conn->executeSQL($sql, $param);
            }
        } catch (Throwable $e) {
            $result["status"] = false;
            $result["error"]  = $e->getMessage();
        }

        return $result;
    }

    protected function assembleDeleteQuery() {
        $param = array($this->data[$this->primaryKey]);
        $sql = "delete from \"$this->table\" where \"$this->primaryKey\" = ?";

        return array($sql, $param);
    }

    protected function assembleInsertQuery() {
        $into  = array_keys($this->data);
        $param = array_values($this->data);

        $values = trim(str_repeat('?,', count($param)), ',');
        $into = implode('", "', $into);

        $sql = "insert into \"$this->table\"(\"$into\") values($values)";

        return array($sql, $param);
    }

    protected function assembleUpdateQuery() {
        $set = array();

        foreach ($this->data as $key => $value) {
            array_push($set, "\"$key\" = ?");
        }

        $set = implode(', ', $set);
        $param = array_values($this->data);

        array_push($param, $this->data[$this->primaryKey]);

        $sql = "update \"$this->table\" set $set where \"$this->primaryKey\" = ?";

        return array($sql, $param);
    }

    protected function dealWithPKOnInsert() {
        $primaryKey = isset($this->data[$this->primaryKey])? $this->data[$this->primaryKey]: "";

        if (empty($primaryKey)) {
            unset($this->data[$this->primaryKey]);
        }
    }

    protected function shouldInsert() {
        return ($this->data[$this->primaryKey] == 0 || strlen($this->data[$this->primaryKey]) == 0);
    }

    protected function stripInexistentFields() {
        $this->data = array_intersect_key($this->data, $this->columns);

        foreach ($this->data as $k => $value) {
            if ($value === '' && in_array($this->columns[$k], ['int', 'integer', 'bigint', 'numeric'])) {
                unset($this->data[$k]);
            }
        }
    }

    public static function fetch($table, $idValue, $field = "id") {
        $result = array();

        $rows = Conn::getInstance()->fetchTableData($table, $field, $idValue);

        foreach ($rows as $row) {
            array_push($result, new self($table, $row));
        }

        if (count($result) > 0) {
            return $result[0];
        } else {
            return false;
        }
    }

    public static function indexResultSet($set, $indexField = "id") {
        $result = [];

        foreach ($set as $row) {
            $id = $row[$indexField];

            $result[$id] = $row;
        }

        return $result;
    }

    public static function fetchList($table, $idList, $field = "id") {
        $result = array();
        $filter = array();

        // You may send a simple variable as parameter
        if (!is_array($idList)) {
            $idList = array($idList);
        }

        foreach ($idList as $id) {
            array_push($filter, array(
                "query"   => "$field = ?",
                "param"   => $id
            ));
        }

        $rows = Conn::getInstance()->fetchSimpleData($table, array(
            "andOr"   => "or",
            "orderby" => [$field],
            "filter"  => $filter
        ));

        foreach ($rows as $row) {
            array_push($result, new self($table, $row));
        }

        return $result;
    }

    /**
     * Iterator Methods
     */
    public function rewind() {
        return reset($this->data);
    }

    public function current() {
        return current($this->data);
    }

    public function key() {
        return key($this->data);
    }

    public function next() {
        return next($this->data);
    }

    public function valid() {
        return key($this->data) !== null;
    }
}
