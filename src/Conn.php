<?php

namespace wgirhad\SimplestORM\Postgres;
use Spatie\Once\Cache;
use Exception;
use PDO;

class Conn extends PDO {
    protected static $dbs = [];
    private static $instances = [];
    protected $dbName;

    function __construct($conf) {
        $depth = 0;
        while (is_string($conf)) {
            if (++$depth > 10) throw new Exception("Configuration Error");
            $conf = self::$dbs[$conf];
        }
        $dsn = self::template($conf['DSN'], $conf);
        parent::__construct($dsn, $conf['user'], $conf['password']);
        $this->dbName = $conf["db"];
    }

    public static function loadConfigs($dbs) {
        self::$dbs = $dbs;
    }

    public static function getInstance($db = 'default') {
        if (!isset(self::$instances[$db])) {
            self::$instances[$db] = new self($db);
        }

        return self::$instances[$db];
    }

    protected static function template($str, $arr) {
        foreach ($arr as $key => $value) {
            if (is_array($value)) continue;
            $str = str_replace('{{' . $key . '}}', $value, $str);
        }

        return $str;
    }

    public function fetchTableData($table, $field = null, $values = null, $operator = "=", $orderby = null, $invert = false, $limit = false, $fields = "*") {
        $where = array();

        if (!is_array($values)) {
            $values = array($values);
        }

        foreach ($values as $i => $value) {
            if ($value !== null && $field !== '') {
                $filter = array();

                if ($invert) {
                    $filter['query'] = "? $operator $field";
                } else {
                    $filter['query'] = "$field $operator ?";
                }
                $filter['param'] = $value;

                array_push($where, $filter);
            }
        }

        return $this->fetchTableDataF($table, $where, $orderby, $limit, 'or', $fields);
    }

    public function fetchTableDataF($table, $filter = array(), $orderby = null, $limit = false, $andOr = "AND", $fields = "*") {
        $param = array();
        $where = array();

        if ($orderby === null) {
            $orderby = [$this->fetchTablePK($table)];
        }

        foreach ($filter as $key => $value) {
            array_push($param, $value['param']);
            array_push($where, $value['query']);
        }

        $where = implode(" $andOr ", $where);
        if (strlen(trim($where)) > 0) $where = "where $where";


        if ($limit !== false) {
            $limit = (int) $limit;
            $limit = "limit $limit";
        }

        if (is_array($orderby)) {
            $orderby = implode(', ', array_filter($orderby));
            if (strlen(trim($orderby)) > 0) {
                $orderby = "order by $orderby";
            }
        } else {
            $orderby = '';
        }

        $sql = "select $fields from \"$table\" $where $orderby $limit";

        return $this->getSQLArray($sql, $param);
    }

    public function fetchSimpleData($table, $options = array()) {
        $list = array();
        $list["filter"] = array();
        $list["orderby"] = null;
        $list["limit"] = false;
        $list["andOr"] = "and";
        $list["fields"] = '*';

        foreach ($options as $key => $value) {
            if (isset($list[$key])) {
                $list[$key] = $value;
            }
        }

        $result = $this->fetchTableDataF($table, $list["filter"], $list["orderby"], $list["limit"], $list["andOr"], $list["fields"]);

        return $result;
    }

    public function assembleFilter($values, $operator = "=") {
        $result = array();

        $isContaining = ($operator == "containing");

        foreach ($values as $key => $value) {
            if ($isContaining) {
                $op = "like";
                $value = "%$value%";
            } else {
                $op = $operator;
            }

            array_push($result, array(
                "query" => "$key $op ?",
                "param" => $value
            ));
        }

        return $result;
    }

    public function fetchTableMeta($table) {
        return once(function () use ($table) {
            $sql =
            "select
                lower(column_name) as column_name,
                data_type
            from information_schema.columns
            where
                table_name = '$table'
            and table_schema = 'public'
            ";

            $array = $this->getSQLArray($sql);

            $result = array();

            foreach ($array as $value) {
                $result[$value['column_name']] = $value['data_type'];
            }

            return $result;
        });
    }

    public function fetchTablePK($table) {
        return once(function () use ($table) {
            $table = mb_strtolower($table);
            $sql =
            "select distinct
                lower(b.column_name) as column_name
            from information_schema.table_constraints a
            join information_schema.constraint_column_usage b using (constraint_schema, constraint_name)
            where a.table_schema = 'public'
            and a.table_name = '$table'
            and a.constraint_type = 'PRIMARY KEY'
            ";

            $array = $this->getSQLArray($sql);

            if (empty($array)) return null;

            return $array[0]["column_name"];
        });
    }

    public function tableExists($table, $force = false) {
        if ($force) Cache::flush();
        return !empty($this->fetchTableMeta($table));
    }

    public function getSQLArray($sql, $param = array()) {
        $result = array();

        $stmt = $this->prepare($sql);
        $param = $this->sanitizeParam($param);

        if (!$stmt->execute($param)) return $result;

        while ($row = $stmt->fetch(parent::FETCH_ASSOC)) {
            array_push($result, $row);
        }

        return $result;
    }

    public function executeRaw($sql) {
        if ($this->exec($sql) === false) {
            $err = $this->errorInfo();
            throw new Exception($err[2], (int) $err[0]);
        }
    }

    public function insert($table, $data) {
        $builder = new QueryBuilder;
        $result = $builder->insert($table, [$data]);
        extract($result);
        return $this->runInsert($query, $param);
    }

    public function insertMulti($table, $dataset) {
        $builder = new QueryBuilder;
        $result = $builder->insert($table, $dataset);
        extract($result);
        $this->executeSQL($query, $param);
    }

    public function executeSQL($sql, $param = array()) {
        $stmt = $this->prepare($sql);
        $param = $this->sanitizeParam($param);

        if (!$stmt->execute($param)) {
            $err = $stmt->errorInfo();
            throw new Exception($err[2], (int) $err[0]);
            return false;
        }

        return true;
    }

    public function runInsert($sql, $param = array()) {
        $result = false;

        $this->beginTransaction();
        $stmt = $this->prepare($sql);
        $param = $this->sanitizeParam($param);

        if ($stmt->execute($param)) {
            $result = $this->lastInsertId();
            $this->commit();
        } else {
            $err = $stmt->errorInfo();
            $this->rollBack();
            throw new Exception($err[2]);
        }

        return $result;
    }

    protected function sanitizeParam($param) {
        return array_map(function($a) {
            if (is_bool($a)) {
                return $a ? 1 : 0;
            }
            return $a;
        }, $param);
    }
}
