<?php
// test_itc_connection.php

echo "🔍 ПРОВЕРКА ПОДКЛЮЧЕНИЯ К ITC БАЗЕ\n";
echo "==================================\n";

$connection = oci_connect(
    'ITC',
    'upkV9V32',
    '(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=10.8.8.75)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=dwh.it.com)))'
);

if (!$connection) {
    $error = oci_error();
    echo "❌ Ошибка подключения: " . $error['message'] . "\n";
    exit;
}

echo "✅ Успешное подключение к ITC базе!\n\n";

// Получаем список таблиц
$query = "SELECT table_name FROM user_tables ORDER BY table_name";
$statement = oci_parse($connection, $query);
oci_execute($statement);

echo "📋 ТАБЛИЦЫ В СХЕМЕ ITC:\n";
echo "=======================\n";

$tables = [];
$count = 0;
while ($row = oci_fetch_assoc($statement)) {
    $count++;
    $tables[] = $row['TABLE_NAME'];
    echo $count . ". " . $row['TABLE_NAME'] . "\n";
}

echo "\nВсего таблиц: " . $count . "\n";

oci_free_statement($statement);
oci_close($connection);

echo "\n✅ Проверка завершена успешно!\n";
?>
