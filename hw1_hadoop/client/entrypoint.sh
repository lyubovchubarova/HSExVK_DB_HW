#!/bin/bash
set -e

echo "--- Ожидание готовности Hadoop кластера ---"

# Сделала проверку готовности HDFS вместо просто sleep
until hdfs dfs -ls / &> /dev/null
do
    echo "Ожидание доступности HDFS..."
    sleep 180
done

echo "HDFS доступен!"
echo ""

# Создать директорию /createme
echo "[1/5] Создание директории /createme"
hdfs dfs -mkdir -p /createme
echo "OK"
echo ""

# Удалить директорию /delme
echo "[2/5] Удаление директории /delme"
hdfs dfs -mkdir -p /delme
hdfs dfs -rm -r /delme
echo "OK"
echo ""

# Создать файл /nonnull.txt
echo "[3/5] Создание файла /nonnull.txt с произвольным содержимым"
echo "Some text to write in the file" | hdfs dfs -put - /nonnull.txt
echo "OK"
echo ""

# Задание 4: Выполнить джобу MR wordcount
echo "[4/5] Выполнение джобы MapReduce wordcount для файла /shadow.txt"
hdfs dfs -put /app/shadow.txt /shadow.txt
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar wordcount /shadow.txt /wordcount_output
echo "OK: Джоба завершена, результат в /wordcount_output"
echo ""

# Записать число вхождений "Innsmouth"
echo "[5/5] Запись числа вхождений 'Innsmouth' в файл /whataboutinsmouth.txt"
innsmouth_count=$(hdfs dfs -cat /wordcount_output/part-r-00000 | grep -i 'Innsmouth' | awk '{sum += $2} END {print sum+0}')
echo ${innsmouth_count} | hdfs dfs -put - /whataboutinsmouth.txt
echo "OK: Результат (${innsmouth_count}) записан в /whataboutinsmouth.txt"
echo ""

echo "--- ВСЕ ЗАДАНИЯ ВЫПОЛНЕНЫ ---"