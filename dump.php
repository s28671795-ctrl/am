<?php
// complete_dump_fixed_v2.php

class OraclePHPDumper {
    private $connection;
    private $username = 'ITC';
    private $password = 'upkV9V32';
    private $connection_string = '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.8.8.75)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=dwh.it.com)))';
    
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
    
    public function getTableStructure($tableName) {
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
    
    public function getTableRowCount($tableName) {
        $sql = "SELECT COUNT(*) as row_count FROM " . $tableName;
        $stmt = oci_parse($this->connection, $sql);
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);
        oci_free_statement($stmt);
        return $row['ROW_COUNT'];
    }
    
    // Улучшенная функция для обработки LOB данных
    private function getLobValue($lob) {
        if ($lob === null) {
            return null;
        }
        
        // Если это уже строка, возвращаем как есть
        if (is_string($lob)) {
            return $lob;
        }
        
        // Если это OCI-Lob объект
        if (is_object($lob) && (get_class($lob) === 'OCI-Lob' || $lob instanceof OCI-Lob)) {
            try {
                $size = $lob->size();
                if ($size === false || $size > 10485760) { // Ограничение 10MB
                    return '[LOB_DATA_TOO_LARGE]';
                }
                
                $content = $lob->load();
                if ($content === false) {
                    return '[LOB_LOAD_ERROR]';
                }
                
                // Для BLOB возвращаем hex, для CLOB - текст
                if ($lob->type() === OCI_T_BLOB) {
                    return '0x' . bin2hex($content);
                } else {
                    return $content;
                }
            } catch (Exception $e) {
                return '[LOB_ERROR: ' . $e->getMessage() . ']';
            }
        }
        
        return $lob;
    }
    
    // Функция для проверки, содержит ли таблица LOB поля
    private function hasLobColumns($tableName) {
        $sql = "
            SELECT COUNT(*) as lob_count 
            FROM user_tab_columns 
            WHERE table_name = :table_name 
            AND data_type IN ('BLOB', 'CLOB', 'NCLOB')
        ";
        
        $stmt = oci_parse($this->connection, $sql);
        oci_bind_by_name($stmt, ':table_name', $tableName);
        oci_execute($stmt);
        $row = oci_fetch_assoc($stmt);
        oci_free_statement($stmt);
        
        return $row['LOB_COUNT'] > 0;
    }
    
    public function exportTableData($tableName, $limit = null) {
        $hasLob = $this->hasLobColumns($tableName);
        $sql = "SELECT * FROM " . $tableName;
        if ($limit) {
            $sql .= " WHERE ROWNUM <= " . $limit;
        }
        
        $stmt = oci_parse($this->connection, $sql);
        
        // Для таблиц с LOB используем специальный режим
        if ($hasLob) {
            oci_execute($stmt);
        } else {
            oci_execute($stmt);
        }
        
        $data = [];
        $numFields = oci_num_fields($stmt);
        
        // Получаем имена полей
        $fieldNames = [];
        $fieldTypes = [];
        for ($i = 1; $i <= $numFields; $i++) {
            $fieldNames[] = $columnName = oci_field_name($stmt, $i);
            $fieldTypes[$columnName] = oci_field_type($stmt, $i);
        }
        
        while ($row = oci_fetch_array($stmt, OCI_ASSOC+OCI_RETURN_NULLS)) {
            $processedRow = [];
            foreach ($row as $key => $value) {
                // Обрабатываем LOB поля отдельно
                if (in_array($fieldTypes[$key], ['BLOB', 'CLOB', 'LONG'])) {
                    $processedRow[$key] = $this->getLobValue($value);
                } else {
                    $processedRow[$key] = $value;
                }
            }
            $data[] = $processedRow;
        }
        
        oci_free_statement($stmt);
        return ['fields' => $fieldNames, 'data' => $data, 'has_lob' => $hasLob];
    }
    
    public function createCompleteDump($maxRowsPerTable = 1000) {
        $this->connect();
        
        $timestamp = date('Y-m-d_H-i-s');
        $dump_dir = '/tmp/oracle_dump_' . $timestamp;
        
        if (!is_dir($dump_dir)) {
            mkdir($dump_dir, 0755, true);
        }
        
        echo "📁 Создаем дамп в директории: {$dump_dir}\n";
        echo "==========================================\n\n";
        
        $tables = $this->getTables();
        $total_tables = count($tables);
        
        // Основной SQL файл
        $sql_file = $dump_dir . '/mnp_prod_complete_dump.sql';
        $sql_handle = fopen($sql_file, 'w');
        
        // Файл статистики
        $stats_file = $dump_dir . '/dump_statistics.txt';
        $stats_handle = fopen($stats_file, 'w');
        
        // Файл лога ошибок
        $error_file = $dump_dir . '/dump_errors.log';
        $error_handle = fopen($error_file, 'w');
        
        // Заголовок дампа
        fwrite($sql_handle, "-- Oracle Database Dump\n");
        fwrite($sql_handle, "-- Created: " . date('Y-m-d H:i:s') . "\n");
        fwrite($sql_handle, "-- Schema: " . $this->username . "\n");
        fwrite($sql_handle, "-- Total Tables: " . $total_tables . "\n");
        fwrite($sql_handle, "SET DEFINE OFF;\n\n");
        
        fwrite($stats_handle, "Статистика дампа базы данных\n");
        fwrite($stats_handle, "==============================\n");
        fwrite($stats_handle, "Дата создания: " . date('Y-m-d H:i:s') . "\n");
        fwrite($stats_handle, "Схема: " . $this->username . "\n");
        fwrite($stats_handle, "Всего таблиц: " . $total_tables . "\n\n");
        
        $processed_tables = 0;
        $total_rows = 0;
        $error_tables = 0;
        $lob_tables = 0;
        
        foreach ($tables as $table) {
            $processed_tables++;
            echo "🔄 Обрабатывается таблица {$processed_tables}/{$total_tables}: {$table}\n";
            
            try {
                // Получаем структуру
                $structure = $this->getTableStructure($table);
                $row_count = $this->getTableRowCount($table);
                $has_lob = $this->hasLobColumns($table);
                
                if ($has_lob) {
                    $lob_tables++;
                    echo "  ⚠️  Таблица содержит LOB поля\n";
                }
                
                fwrite($stats_handle, "{$table}: {$row_count} строк" . ($has_lob ? " (LOB)" : "") . "\n");
                
                // Создаем DROP TABLE
                fwrite($sql_handle, "--\n-- Table: {$table}\n--\n");
                fwrite($sql_handle, "DROP TABLE {$table} CASCADE CONSTRAINTS;\n\n");
                
                // Создаем CREATE TABLE
                fwrite($sql_handle, "CREATE TABLE {$table} (\n");
                
                $column_defs = [];
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
                    
                    $column_defs[] = $def;
                }
                
                fwrite($sql_handle, implode(",\n", $column_defs) . "\n);\n\n");
                
                // Экспортируем данные
                if ($row_count > 0) {
                    echo "  📊 Экспортируем данные ({$row_count} строк)... ";
                    
                    try {
                        $export_data = $this->exportTableData($table, $maxRowsPerTable);
                        $exported_rows = count($export_data['data']);
                        
                        fwrite($sql_handle, "-- Data for {$table} ({$exported_rows} of {$row_count} rows)");
                        if ($has_lob) {
                            fwrite($sql_handle, " - Contains LOB data");
                        }
                        fwrite($sql_handle, "\n");
                        
                        $insert_count = 0;
                        foreach ($export_data['data'] as $row) {
                            $values = [];
                            foreach ($export_data['fields'] as $field) {
                                $value = $row[$field];
                                if ($value === null) {
                                    $values[] = 'NULL';
                                } else {
                                    // Экранируем кавычки
                                    $value = str_replace("'", "''", $value);
                                    // Обрезаем слишком длинные значения
                                    if (strlen($value) > 4000) {
                                        $value = substr($value, 0, 4000) . '... [TRIMMED]';
                                    }
                                    $values[] = "'" . $value . "'";
                                }
                            }
                            
                            fwrite($sql_handle, "INSERT INTO {$table} (" . 
                                  implode(', ', $export_data['fields']) . ") VALUES (" . 
                                  implode(', ', $values) . ");\n");
                            $insert_count++;
                        }
                        
                        fwrite($sql_handle, "\n");
                        echo "экспортировано {$exported_rows} строк\n";
                        $total_rows += $exported_rows;
                        
                    } catch (Exception $e) {
                        echo "ошибка данных: " . $e->getMessage() . "\n";
                        fwrite($sql_handle, "-- ERROR exporting data: " . $e->getMessage() . "\n\n");
                        fwrite($error_handle, "Таблица {$table} данные: " . $e->getMessage() . "\n");
                    }
                } else {
                    echo "  ℹ️  Таблица пустая\n";
                }
                
                fwrite($sql_handle, "-- End of table {$table}\n\n");
                
            } catch (Exception $e) {
                $error_tables++;
                echo "  ❌ Ошибка обработки таблицы: " . $e->getMessage() . "\n";
                fwrite($error_handle, "Таблица {$table}: " . $e->getMessage() . "\n");
                fwrite($sql_handle, "-- ERROR processing table {$table}: " . $e->getMessage() . "\n\n");
            }
        }
        
        fclose($sql_handle);
        fclose($stats_handle);
        fclose($error_handle);
        
        // Создаем README файл
        $readme_file = $dump_dir . '/README.txt';
        file_put_contents($readme_file, 
            "Oracle Database Dump\n" .
            "====================\n\n" .
            "Created: " . date('Y-m-d H:i:s') . "\n" .
            "Schema: " . $this->username . "\n" .
            "Total Tables: " . $total_tables . "\n" .
            "Tables Processed: " . $processed_tables . "\n" .
            "Rows Exported: " . $total_rows . "\n" .
            "Tables with LOB: " . $lob_tables . "\n" .
            "Tables with Errors: " . $error_tables . "\n\n" .
            "Files:\n" .
            "- mnp_prod_complete_dump.sql - Main SQL dump\n" .
            "- dump_statistics.txt - Statistics\n" .
            "- dump_errors.log - Error log\n" .
            "- README.txt - This file\n"
        );
        
        echo "\n" . str_repeat("=", 60) . "\n";
        echo "✅ ДАМП ЗАВЕРШЕН!\n";
        echo str_repeat("=", 60) . "\n";
        echo "📊 Таблиц обработано: {$processed_tables}/{$total_tables}\n";
        echo "📊 Строк экспортировано: {$total_rows}\n";
        echo "⚡ Таблиц с LOB: {$lob_tables}\n";
        echo "❌ Таблиц с ошибками: {$error_tables}\n";
        echo "📁 Файлы созданы в: {$dump_dir}\n";
        
        $this->close();
        
        return [
            'dump_dir' => $dump_dir,
            'tables_processed' => $processed_tables,
            'total_tables' => $total_tables,
            'rows_exported' => $total_rows,
            'lob_tables' => $lob_tables,
            'tables_with_errors' => $error_tables
        ];
    }
    
    public function close() {
        if ($this->connection) {
            oci_close($this->connection);
        }
    }
}

// Выполняем дамп
try {
    echo "🚀 ЗАПУСК СОЗДАНИЯ ДАМПА БАЗЫ ДАННЫХ\n";
    echo "=====================================\n\n";
    
    $dumper = new OraclePHPDumper();
    
    // Уменьшаем лимит для проблемных таблиц
    $result = $dumper->createCompleteDump(50); // 50 строк максимум на таблицу
    
    echo "\n🎉 ДАМП УСПЕШНО СОЗДАН!\n";
    
    if ($result['tables_with_errors'] > 0) {
        echo "⚠️  Некоторые таблицы содержали ошибки\n";
    }
    
    echo "📁 Проверьте директорию: {$result['dump_dir']}\n";
    
} catch (Exception $e) {
    echo "❌ ОШИБКА: " . $e->getMessage() . "\n";
}
?>
