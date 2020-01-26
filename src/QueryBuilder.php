<?php

namespace wgirhad\SimplestORM\Postgres;
use Exception;

class QueryBuilder {
    public function insert($table, $dataset) {
        if (empty($dataset)) {
            throw new Exception("Empty Insert");
        }

        $first = array_keys($dataset)[0];

        if (!is_array($dataset[$first])) {
            throw new Exception("Invalid Dataset");
        }

        $headers = array_keys($dataset[$first]);

        $fields = implode(', ', array_map(function ($name) {
            return "\"$name\"";
        }, $headers));

        $values = [];
        $param = [];

        foreach ($dataset as $row) {
            $value = [];
            foreach ($headers as $field) {
                $value[] = '?';
                $param[] = $row[$field] ?? null;
            }
            $value = implode(', ', $value);
            $values[] = "($value)";
        }

        $values = implode(', ', $values);

        $query = "insert into \"$table\"($fields) values $values;";

        return compact('query', 'param');
    }
}
