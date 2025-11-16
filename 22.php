<?php
// test_oci_with_data.php
echo "Проверка OCI8 расширения:\n";
echo "=========================\n";

if (!function_exists('oci_connect')) {
    die("❌ Функция oci_connect не найдена\n");
}

echo "✅ OCI8 расширение доступно\n";

// Параметры подключения
$config = [
    'username' => 'mnp_prod',
    'password' => 's$r$FKjp4t',
    'connection_string' => '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.8.8.16)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=mnp.it.com)))'
];

echo "Попытка подключения...\n";

// Подключаемся к базе
$connection = oci_connect(
    $config['username'],
    $config['password'],
    $config['connection_string']
);

if (!$connection) {
    $error = oci_error();
    echo "❌ Ошибка подключения: " . $error['message'] . "\n";
    exit;
}

echo "✅ Успешное подключение к Oracle!\n\n";

// Получаем список таблиц
$query = "SELECT table_name FROM user_tables ORDER BY table_name";
$statement = oci_parse($connection, $query);

if (!oci_execute($statement)) {
    $error = oci_error($statement);
    echo "❌ Ошибка выполнения запроса: " . $error['message'] . "\n";
    oci_close($connection);
    exit;
}

echo "Таблицы в схеме mnp_prod:\n";
echo "==========================\n";

$tables = [];
$count = 0;
while ($row = oci_fetch_assoc($statement)) {
    $count++;
    $tables[] = $row['TABLE_NAME'];
    echo $count . ". " . $row['TABLE_NAME'] . "\n";
}

echo "\nВсего таблиц: " . $count . "\n";

// Освобождаем ресурсы
oci_free_statement($statement);

echo "\n" . str_repeat("=", 80) . "\n";
echo "ВЫВОД ДАННЫХ ИЗ ТАБЛИЦ (по 5 строк):\n";
echo str_repeat("=", 80) . "\n\n";

// Функция для получения структуры таблицы
function getTableStructure($connection, $tableName) {
    $sql = "
        SELECT 
            column_name,
            data_type,
            data_length
        FROM user_tab_columns 
        WHERE table_name = :table_name 
        ORDER BY column_id
    ";
    
    $stmt = oci_parse($connection, $sql);
    oci_bind_by_name($stmt, ':table_name', $tableName);
    oci_execute($stmt);
    
    $structure = [];
    while ($row = oci_fetch_assoc($stmt)) {
        $structure[] = $row;
    }
    oci_free_statement($stmt);
    return $structure;
}

// Функция для вывода данных таблицы
function displayTableData($connection, $tableName, $limit = 5) {
    echo "📊 ТАБЛИЦА: " . $tableName . "\n";
    echo str_repeat("-", 60) . "\n";
    
    // Получаем структуру таблицы
    $structure = getTableStructure($connection, $tableName);
    
    if (empty($structure)) {
        echo "❌ Не удалось получить структуру таблицы\n\n";
        return;
    }
    
    // Формируем запрос
    $sql = "SELECT * FROM " . $tableName . " WHERE ROWNUM <= " . $limit;
    $stmt = oci_parse($connection, $sql);
    
    if (!oci_execute($stmt)) {
        $error = oci_error($stmt);
        echo "❌ Ошибка выполнения запроса: " . $error['message'] . "\n\n";
        return;
    }
    
    // Выводим заголовки столбцов
    $numColumns = oci_num_fields($stmt);
    echo "Столбцы (" . $numColumns . "): ";
    
    $columnNames = [];
    for ($i = 1; $i <= $numColumns; $i++) {
        $columnName = oci_field_name($stmt, $i);
        $columnNames[] = $columnName;
        echo $columnName;
        if ($i < $numColumns) echo ", ";
    }
    echo "\n";
    
    echo str_repeat("-", 60) . "\n";
    
    // Выводим данные
    $rowCount = 0;
    while ($row = oci_fetch_array($stmt, OCI_ASSOC+OCI_RETURN_NULLS)) {
        $rowCount++;
        echo "Строка " . $rowCount . ":\n";
        
        foreach ($row as $key => $value) {
            // Обрезаем длинные значения для лучшего отображения
            $displayValue = $value;
            if ($value !== null && strlen($value) > 50) {
                $displayValue = substr($value, 0, 47) . '...';
            }
            
            echo "  " . str_pad($key . ":", 25) . " " . 
                 ($value === null ? 'NULL' : $displayValue) . "\n";
        }
        echo "\n";
    }
    
    if ($rowCount == 0) {
        echo "ℹ️  Таблица пустая\n";
    }
    
    echo "Всего строк показано: " . $rowCount . "\n";
    echo str_repeat("=", 60) . "\n\n";
    
    oci_free_statement($stmt);
}

// Выводим данные из каждой таблицы
foreach ($tables as $table) {
    displayTableData($connection, $table, 5);
    
    // Пауза между таблицами (опционально)
    // usleep(100000); // 0.1 секунда
}

// Закрываем соединение
oci_close($connection);

echo "✅ Завершено успешно! Показаны данные из " . count($tables) . " таблиц.\n";
?>
