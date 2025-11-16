<?php
// complete_dump_fixed.php

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
    
    // Функция для обработки LOB данных
    private function getLobValue($lob) {
        if ($lob === null) {
            return null;
        }
        
        if (is_object($lob) && get_class($lob) === 'OCI-Lob') {
            try {
                // Для BLOB данных возвращаем hex строку
                if ($lob->type() === OCI_T_BLOB) {
                    $content = $lob->load();
                    return '0x' . bin2hex($content);
                }
                // Для CLOB данных возвращаем текст
                elseif ($lob->type() === OCI_T_CLOB) {
                    return $lob->read($lob->size());
                }
            } catch (Exception $e) {
                return '[LOB_ERROR: ' . $e->getMessage() . ']';
            }
        }
        
        return $lob;
    }
    
    public function exportTableData($tableName, $limit = null) {
        $sql = "SELECT * FROM " . $tableName;
        if ($limit) {
            $sql .= " WHERE ROWNUM <= " . $limit;
        }
        
        $stmt = oci_parse($this->connection, $sql);
        oci_execute($stmt);
        
        $data = [];
        $numFields = oci_num_fields($stmt);
        
        // Получаем имена полей и их типы
        $fieldNames = [];
        $fieldTypes = [];
        for ($i = 1; $i <= $numFields; $i++) {
            $fieldNames[] = oci_field_name($stmt, $i);
            $fieldTypes[] = oci_field_type($stmt, $i);
        }
        
        while ($row = oci_fetch_array($stmt, OCI_ASSOC+OCI_RETURN_NULLS+OCI_RETURN_LOBS)) {
            // Обрабатываем LOB поля
            foreach ($row as $key => $value) {
                $row[$key] = $this->getLobValue($value);
            }
            $data[] = $row;
        }
        
        oci_free_statement($stmt);
        return ['fields' => $fieldNames, 'data' => $data, 'types' => $fieldTypes];
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
        
        foreach ($tables as $table) {
            $processed_tables++;
            echo "🔄 Обрабатывается таблица {$processed_tables}/{$total_tables}: {$table}\n";
            
            try {
                // Получаем структуру
                $structure = $this->getTableStructure($table);
                $row_count = $this->getTableRowCount($table);
                
                fwrite($stats_handle, "{$table}: {$row_count} строк\n");
                
                // Создаем DROP TABLE
                fwrite($sql_handle, "--\n-- Table: {$table}\n--\n");
                fwrite($sql_handle, "DROP TABLE {$table} CASCADE CONSTRAINTS;\n\n");
                
                // Создаем CREATE TABLE
                fwrite($sql_handle, "CREATE TABLE {$table} (\n");
                
                $column_defs = [];
                foreach ($structure as $col) {
                    $def = "    {$col['COLUMN_NAME']} {$col['DATA_TYPE']}";
                    
                    // Добавляем размер для строковых типов
                    if (in_array($col['DATA_TYPE'], ['VARCHAR2', 'CHAR', 'RAW'])) {
                        $def .= "({$col['DATA_LENGTH']})";
                    }
                    // Для числовых типов
                    elseif ($col['DATA_TYPE'] == 'NUMBER' && $col['DATA_PRECISION']) {
                        if ($col['DATA_SCALE'] > 0) {
                            $def .= "({$col['DATA_PRECISION']},{$col['DATA_SCALE']})";
                        } else {
                            $def .= "({$col['DATA_PRECISION']})";
                        }
                    }
                    // Для BLOB/CLOB не указываем размер
                    elseif (in_array($col['DATA_TYPE'], ['BLOB', 'CLOB'])) {
                        // ничего не добавляем
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
                    
                    $export_data = $this->exportTableData($table, $maxRowsPerTable);
                    $exported_rows = count($export_data['data']);
                    
                    fwrite($sql_handle, "-- Data for {$table} ({$exported_rows} of {$row_count} rows)\n");
                    
                    foreach ($export_data['data'] as $row) {
                        $values = [];
                        foreach ($export_data['fields'] as $field) {
                            $value = $row[$field];
                            if ($value === null) {
                                $values[] = 'NULL';
                            } else {
                                // Экранируем кавычки и обрабатываем специальные символы
                                $value = str_replace("'", "''", $value);
                                // Для BLOB данных уже обработано в getLobValue
                                $values[] = "'" . $value . "'";
                            }
                        }
                        
                        fwrite($sql_handle, "INSERT INTO {$table} (" . 
                              implode(', ', $export_data['fields']) . ") VALUES (" . 
                              implode(', ', $values) . ");\n");
                    }
                    
                    fwrite($sql_handle, "\n");
                    echo "экспортировано {$exported_rows} строк\n";
                    $total_rows += $exported_rows;
                } else {
                    echo "  ℹ️  Таблица пустая\n";
                }
                
                // Создаем индексы (упрощенно)
                fwrite($sql_handle, "-- Indexes for {$table}\n");
                fwrite($sql_handle, "-- (indexes export not implemented)\n\n");
                
            } catch (Exception $e) {
                $error_tables++;
                echo "  ❌ Ошибка: " . $e->getMessage() . "\n";
                fwrite($error_handle, "Таблица {$table}: " . $e->getMessage() . "\n");
                fwrite($sql_handle, "-- ERROR processing table {$table}: " . $e->getMessage() . "\n\n");
            }
        }
        
        fclose($sql_handle);
        fclose($stats_handle);
        fclose($error_handle);
        
        // Создаем архив
        $zip_file = $this->createZipArchive($dump_dir);
        
        echo "\n" . str_repeat("=", 50) . "\n";
        echo "✅ ДАМП ЗАВЕРШЕН!\n";
        echo str_repeat("=", 50) . "\n";
        echo "📊 Таблиц обработано: {$processed_tables}\n";
        echo "📊 Строк экспортировано: {$total_rows}\n";
        echo "❌ Таблиц с ошибками: {$error_tables}\n";
        echo "📁 Файлы:\n";
        echo "   - SQL дамп: {$sql_file}\n";
        echo "   - Статистика: {$stats_file}\n";
        echo "   - Лог ошибок: {$error_file}\n";
        if ($zip_file) {
            echo "   - Архив: {$zip_file}\n";
        }
        
        $this->close();
        
        return [
            'sql_file' => $sql_file,
            'stats_file' => $stats_file,
            'error_file' => $error_file,
            'zip_file' => $zip_file,
            'tables_processed' => $processed_tables,
            'rows_exported' => $total_rows,
            'tables_with_errors' => $error_tables
        ];
    }
    
    private function createZipArchive($directory) {
        $zip_file = $directory . '.zip';
        
        echo "🗜️  Создаем архив... ";
        
        if (!class_exists('ZipArchive')) {
            echo "расширение ZipArchive не доступно\n";
            return null;
        }
        
        $zip = new ZipArchive();
        if ($zip->open($zip_file, ZipArchive::CREATE) === TRUE) {
            $files = new RecursiveIteratorIterator(
                new RecursiveDirectoryIterator($directory),
                RecursiveIteratorIterator::LEAVES_ONLY
            );
            
            foreach ($files as $file) {
                if (!$file->isDir()) {
                    $filePath = $file->getRealPath();
                    $relativePath = substr($filePath, strlen($directory) + 1);
                    $zip->addFile($filePath, $relativePath);
                }
            }
            
            $zip->close();
            echo "готово\n";
            return $zip_file;
        } else {
            echo "ошибка\n";
            return null;
        }
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
    $result = $dumper->createCompleteDump(100); // 100 строк максимум на таблицу
    
    echo "\n🎉 ДАМП УСПЕШНО СОЗДАН!\n";
    
    if ($result['tables_with_errors'] > 0) {
        echo "⚠️  Некоторые таблицы содержали ошибки, проверьте файл: {$result['error_file']}\n";
    }
    
} catch (Exception $e) {
    echo "❌ ОШИБКА: " . $e->getMessage() . "\n";
}
?>
