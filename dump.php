<?php
// dump_adnum_final.php

class AdnumOracleDumper {
    private $connection;
    private $username = 'adnum';
    private $password = 'it';
    private $connection_string = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.8.8.75)(PORT=1521)))(CONNECT_DATA=(SID=dwh.it.com)))';
    
    public function connect() {
        $this->connection = oci_connect($this->username, $this->password, $this->connection_string, 'AL32UTF8');
        if (!$this->connection) {
            throw new Exception("Ошибка подключения: " . oci_error());
        }
        return true;
    }
    
    public function getTables() {
        $sql = "SELECT table_name FROM user_tables ORDER BY table_name";
        $stmt = oci_parse($this->connection, $sql);
        oci_execute($stmt);
        
        $tables = [];
        while ($row = oci_fetch_assoc($stmt)) {
            $tables[] = $row['TABLE_NAME'];
        }
        oci_free_statement($stmt);
        return $tables;
    }
    
    public function createDump() {
        $this->connect();
        
        $timestamp = date('Y-m-d_H-i-s');
        $dump_dir = '/tmp/adnum_dump_' . $timestamp;
        
        if (!is_dir($dump_dir)) {
            mkdir($dump_dir, 0755, true);
        }
        
        echo "🚀 СОЗДАНИЕ ДАМПА СХЕМЫ ADNUM\n";
        echo "==============================\n";
        echo "📁 Директория: {$dump_dir}\n";
        echo "👤 Пользователь: {$this->username}\n";
        echo "🔗 SID: dwh.it.com\n\n";
        
        $sql_file = $dump_dir . '/adnum_schema_dump.sql';
        $handle = fopen($sql_file, 'w');
        
        fwrite($handle, "-- Oracle Database Dump - ADNUM Schema\n");
        fwrite($handle, "-- Created: " . date('Y-m-d H:i:s') . "\n");
        fwrite($handle, "-- Schema: " . $this->username . "\n");
        fwrite($handle, "-- Host: 10.8.8.75:1521\n");
        fwrite($handle, "-- SID: dwh.it.com\n");
        fwrite($handle, "SET DEFINE OFF;\n\n");
        
        $tables = $this->getTables();
        $processed = 0;
        $total_rows = 0;
        
        foreach ($tables as $table) {
            $processed++;
            echo "🔄 {$processed}. Обрабатываем: {$table}\n";
            
            try {
                $rows_exported = $this->dumpTable($handle, $table, 100);
                $total_rows += $rows_exported;
                echo "   ✅ Экспортировано строк: {$rows_exported}\n";
            } catch (Exception $e) {
                echo "   ❌ Ошибка: " . $e->getMessage() . "\n";
                fwrite($handle, "-- ERROR: " . $e->getMessage() . "\n\n");
            }
        }
        
        fclose($handle);
        $this->close();
        
        // Создаем статистику
        $this->createStatsFile($dump_dir, count($tables), $processed, $total_rows);
        
        echo "\n" . str_repeat("=", 50) . "\n";
        echo "✅ ДАМП ADNUM УСПЕШНО ЗАВЕРШЕН!\n";
        echo str_repeat("=", 50) . "\n";
        echo "📊 Всего таблиц: " . count($tables) . "\n";
        echo "✅ Обработано: {$processed}\n";
        echo "📊 Строк экспортировано: {$total_rows}\n";
        echo "📁 Файл дампа: {$sql_file}\n";
        
        return $dump_dir;
    }
    
    private function dumpTable($handle, $tableName, $limit = 100) {
        // Структура таблицы
        $structure = $this->getTableStructure($tableName);
        
        fwrite($handle, "--\n-- Table: {$tableName}\n--\n");
        fwrite($handle, "DROP TABLE {$tableName} CASCADE CONSTRAINTS;\n\n");
        fwrite($handle, "CREATE TABLE {$tableName} (\n");
        
        $columns = [];
        foreach ($structure as $col) {
            $def = "    {$col['COLUMN_NAME']} {$col['DATA_TYPE']}";
            
            if (in_array($col['DATA_TYPE'], ['VARCHAR2', 'CHAR', 'RAW'])) {
                $def .= "({$col['DATA_LENGTH']})";
            } elseif ($col['DATA_TYPE'] == 'NUMBER' && $col['DATA_PRECISION']) {
                if ($col['DATA_SCALE'] > 0) {
                    $def .= "({$col['DATA_PRECISION']},{$col['DATA_SCALE']})";
                } else {
                    $def .= "({$col['DATA_PRECISION']})";
                }
            }
            
            if ($col['NULLABLE'] == 'N') {
                $def .= " NOT NULL";
            }
            
            if ($col['DATA_DEFAULT']) {
                $def .= " DEFAULT {$col['DATA_DEFAULT']}";
            }
            
            $columns[] = $def;
        }
        
        fwrite($handle, implode(",\n", $columns) . "\n);\n\n");
        
        // Данные таблицы
        $rows_exported = $this->dumpTableData($handle, $tableName, $limit);
        
        fwrite($handle, "-- End of table {$tableName}\n\n");
        
        return $rows_exported;
    }
    
    private function getTableStructure($tableName) {
        $sql = "
            SELECT 
                column_name,
                data_type,
                data_length,
                data_precision,
                data_scale,
                nullable,
                data_default
            FROM user_tab_columns 
            WHERE table_name = :table_name 
            ORDER BY column_id
        ";
        
        $stmt = oci_parse($this->connection, $sql);
        oci_bind_by_name($stmt, ':table_name', $tableName);
        oci_execute($stmt);
        
        $structure = [];
        while ($row = oci_fetch_assoc($stmt)) {
            $structure[] = $row;
        }
        oci_free_statement($stmt);
        return $structure;
    }
    
    private function dumpTableData($handle, $tableName, $limit) {
        // Проверяем на LOB поля
        if ($this->hasLobColumns($tableName)) {
            fwrite($handle, "-- Data skipped - table contains LOB columns\n");
            return 0;
        }
        
        $sql = "SELECT * FROM {$tableName} WHERE ROWNUM <= {$limit}";
        $stmt = oci_parse($this->connection, $sql);
        
        if (!oci_execute($stmt)) {
            fwrite($handle, "-- Data skipped - query error\n");
            return 0;
        }
        
        $num_fields = oci_num_fields($stmt);
        $field_names = [];
        for ($i = 1; $i <= $num_fields; $i++) {
            $field_names[] = oci_field_name($stmt, $i);
        }
        
        $row_count = 0;
        $total_rows = $this->getTableRowCount($tableName);
        
        fwrite($handle, "-- Data for {$tableName} ({$row_count} of {$total_rows} rows)\n");
        
        while ($row = oci_fetch_array($stmt, OCI_ASSOC+OCI_RETURN_NULLS)) {
            $values = [];
            foreach ($field_names as $field) {
                $value = $row[$field];
                if ($value === null) {
                    $values[] = 'NULL';
                } else {
                    $value = str_replace("'", "''", $value);
                    if (strlen($value) > 1000) {
                        $value = substr($value, 0, 1000) . '...';
                    }
                    $values[] = "'" . $value . "'";
                }
            }
            
            fwrite($handle, "INSERT INTO {$tableName} (" . 
                  implode(', ', $field_names) . ") VALUES (" . 
                  implode(', ', $values) . ");\n");
            $row_count++;
        }
        
        fwrite($handle, "-- Total rows exported: {$row_count}\n\n");
        oci_free_statement($stmt);
        
        return $row_count;
    }
    
    private function hasLobColumns($tableName) {
        $sql = "
            SELECT COUNT(*) as lob_count 
            FROM user_tab_columns 
            WHERE table_name = :table_name 
            AND data_type IN ('BLOB', 'CLOB', 'LONG')
        ";
        
        $stmt = oci_parse($this->connection, $sql);
        oci_bind_by_name($stmt, ':table_name', $tableName);
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);
        oci_free_statement($stmt);
        
        return $row['LOB_COUNT'] > 0;
    }
    
    private function getTableRowCount($tableName) {
        $sql = "SELECT COUNT(*) as cnt FROM " . $tableName;
        $stmt = oci_parse($this->connection, $sql);
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);
        oci_free_statement($stmt);
        return $row['CNT'];
    }
    
    private function createStatsFile($dump_dir, $total_tables, $processed, $total_rows) {
        $stats_file = $dump_dir . '/STATISTICS.txt';
        $content = "
Oracle Database Dump Statistics - ADNUM Schema
==============================================

Date: " . date('Y-m-d H:i:s') . "
Schema: {$this->username}
Host: 10.8.8.75:1521
SID: dwh.it.com

Summary:
--------
Total Tables: {$total_tables}
Successfully Processed: {$processed}
Total Rows Exported: {$total_rows}

The dump file contains:
- Table structures (CREATE TABLE)
- Sample data (up to 100 rows per table, excluding LOB fields)
- Ready to import SQL commands

To restore, use:
sqlplus adnum/it@//10.8.8.75:1521/dwh.it.com @adnum_schema_dump.sql

Note: Tables with BLOB/CLOB fields were skipped.
        ";
        
        file_put_contents($stats_file, $content);
    }
    
    public function close() {
        if ($this->connection) {
            oci_close($this->connection);
        }
    }
}

// Запуск дампа для adnum
try {
    $dumper = new AdnumOracleDumper();
    $result_dir = $dumper->createDump();
    
    echo "\n🎉 ДАМП СХЕМЫ ADNUM УСПЕШНО СОЗДАН!\n";
    echo "📋 Проверьте файлы в директории: {$result_dir}\n";
    
} catch (Exception $e) {
    echo "❌ КРИТИЧЕСКАЯ ОШИБКА: " . $e->getMessage() . "\n";
}
?>
