
# Задание 2. Hive и витрины данных.

На основе набора данных о деятельности госпиталя были построены 6 витрин, которые отвечают на различные бизнес-вопросы: от оценки эффективности отделений до выявления перегрузок и анализа демографии пациентов.

**Источник данных:** [Hospital Beds Management на Kaggle](https://www.kaggle.com/datasets/jaderz/hospital-beds-management)

Эта коллекция синтетических больничных наборов данных предназначена для моделирования реальных рабочих процессов больницы среднего размера, с акцентом на укомплектование штата, приём пациентов и распределение коек между отделениями. Эти данные позволяют исследовать и анализировать распределение ресурсов больницы, включая расстановку персонала, спрос со стороны пациентов и показатели работы на уровне отделений.

Набор данных состоит из четырех CSV-файлов:

- `hospital_staff.csv` – Список персонала больницы
- `hospital_patients.csv` – Записи о пациентах
- `hospital_service_weekly.csv` – Еженедельные данные по отделениям
- `hospital_staff_schedule.csv` – Еженедельный график работы

**Руководство по установке Hive через Docker:** [Apache Hive Quickstart](https://hive.apache.org/development/quickstart/#quickstart) - источник части выполнения этого задания

## Структура проекта

```
/
├── data/
│ ├── patients.csv
│ ├── services_weekly.csv
│ ├── staff.csv
│ └── staff_schedule.csv
└── README.md
```

## Развертывание проекта 

То, что делала я пошагово в процессе выполнения

### Шаг 1. Запуск окружения

```bash
# 1. Скачать образ Hive
docker pull apache/hive:4.0.0

# 2. Запустить контейнер Hive 
docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name my-hive-server apache/hive:4.0.0
```

### Шаг 2. Загрузка данных

```bash
# Скопировать файлы данных внутрь контейнера
# (путь hw2_hive/data/ нужно заменить на локальный путь к данным)
docker cp hw2_hive/data/patients.csv my-hive-server:/opt/ 
docker cp hw2_hive/data/services_weekly.csv my-hive-server:/opt/ 
docker cp hw2_hive/data/staff.csv my-hive-server:/opt/  
docker cp hw2_hive/data/staff_schedule.csv my-hive-server:/opt/ 
```

### Шаг 3. Работа в Hive и загрузка данных

```bash
# Подключиться к командной строке Hive (Beeline)
docker exec -it my-hive-server beeline -u 'jdbc:hive2://localhost:10000/'
```

Нужно создать базу данных 

```sql
CREATE DATABASE hospital_analytics;
USE hospital_analytics;
```

И для каждого из сsv файлов нужно создать схему и загрузить данные в таблицу, привожу полный пример. В создании схемы опускаем чтение первой строки-заголовка.

```sql
CREATE TABLE patients_raw (
    patient_id STRING,
    name STRING,
    age INT,
    arrival_date DATE,
    departure_date DATE,
    service STRING,
    satisfaction INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

CREATE TABLE services_weekly_raw (
    week INT,
    month INT,
    service STRING,
    available_beds INT,
    patients_request INT,
    patients_admitted INT,
    patients_refused INT,
    patient_satisfaction INT,
    staff_morale INT,
    event STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

CREATE TABLE staff_raw (
    staff_id STRING,
    staff_name STRING,
    role STRING,
    service STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

CREATE TABLE staff_schedule_raw (
    week INT,
    staff_id STRING,
    staff_name STRING,
    role STRING,
    service STRING,
    present INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


LOAD DATA INPATH '/opt/patients.csv' INTO TABLE patients_raw;
LOAD DATA INPATH '/opt/services_weekly.csv' INTO TABLE services_weekly_raw;
LOAD DATA INPATH '/opt/staff.csv' INTO TABLE staff_raw;
LOAD DATA INPATH '/opt/staff_schedule.csv' INTO TABLE staff_schedule_raw;
```

Здесь можно сделать пару селектов для того, чтобы убедиться, что данные прогрузились

```sql
SELECT * FROM patients_raw LIMIT 5;
SELECT * FROM services_weekly_raw LIMIT 5;
```


## Витрины данных и команды для их создания


### Статистика по отделениям количеству пациентов, удовлетворенности пациентов и длительности пребывания

**Цель**: Сравнить отделения госпиталя по ключевым показателям эффективности.

**Что показывает эта витрина**:

- **Рейтинг удовлетворенности:** Позволяет моментально увидеть, пациенты какого отделения наиболее довольны лечением.
- **Масштаб операций:** Показывает, какое отделение обслужило больше всего пациентов.
- **Эффективность использования коек**: avg_length_of_stay демонстрирует, как долго в среднем пациент занимает койку. Более низкие значения могут говорить о более эффективной работе.

Руководство может использовать эти данные для поощрения отделений-лидеров или для проведения аудита в отделениях с низкими показателями. Витрина дает комплексное представление, а не смотрит на один показатель в отрыве от других.

SQL-запрос:

```sql
CREATE TABLE performance_by_service AS
SELECT
    service,
    COUNT(patient_id) as total_patients,
    ROUND(AVG(satisfaction), 2) as avg_satisfaction,
    ROUND(AVG(DATEDIFF(departure_date, arrival_date)), 2) as avg_length_of_stay
FROM patients_raw
GROUP BY service
ORDER BY avg_satisfaction DESC;
```

Пример результата 
```sql
SELECT * FROM performance_by_service;

+---------------------------------+----------------------------------------+------------------------------------------+--------------------------------------------+
| performance_by_service.service  | performance_by_service.total_patients  | performance_by_service.avg_satisfaction  | performance_by_service.avg_length_of_stay  |
+---------------------------------+----------------------------------------+------------------------------------------+--------------------------------------------+
| surgery                         | 254                                    | 80.31                                    | 7.87                                       |
| ICU                             | 241                                    | 79.92                                    | 7.61                                       |
| emergency                       | 263                                    | 79.55                                    | 7.16                                       |
| general_medicine                | 242                                    | 78.57                                    | 7.0                                        |
+---------------------------------+----------------------------------------+------------------------------------------+--------------------------------------------+
```

### Демографические данные о пациентах по отделению (к сожалению, есть только возраст)

**Цель:** Понять, пациенты какого возраста преобладают в каждом отделении, если бы были другие данные - можно вывести их. Например, пол, наличие/отсутствие страховки и тд.

**Что показывает эта витрина**:

- **Портрет пациента**: Рисует демографический профиль для каждого отделения.
- **Специализация отделений**: Помогает понять, какие отделения фактически являются "детскими", "взрослыми" или "гериатрическими".
- **Маркетинг и коммуникации**: Помогает адаптировать информационные материалы для целевой аудитории каждого отделения.


SQL-запрос:

```sql
CREATE TABLE patient_demographics_by_service AS
SELECT
    service,
    CASE
        WHEN age >= 0 AND age <= 17 THEN 'Children'
        WHEN age >= 18 AND age <= 64 THEN 'Adults'
        ELSE 'Seniors'
    END as age_group,
    COUNT(*) as number_of_patients
FROM patients_raw
GROUP BY
    service,
    CASE
        WHEN age >= 0 AND age <= 17 THEN 'Children'
        WHEN age >= 18 AND age <= 64 THEN 'Adults'
        ELSE 'Seniors'
    END;
```

Пример результата:
```sql
SELECT * FROM patient_demographics_by_service;

+------------------------------------------+--------------------------------------------+----------------------------------------------------+
| patient_demographics_by_service.service  | patient_demographics_by_service.age_group  | patient_demographics_by_service.number_of_patients |
+------------------------------------------+--------------------------------------------+----------------------------------------------------+
| ICU                                      | Adults                                     | 137                                                |
| ICU                                      | Children                                   | 37                                                 |
| ICU                                      | Seniors                                    | 67                                                 |
| emergency                                | Adults                                     | 137                                                |
| emergency                                | Children                                   | 53                                                 |
| emergency                                | Seniors                                    | 73                                                 |
| general_medicine                         | Adults                                     | 134                                                |
| general_medicine                         | Children                                   | 39                                                 |
| general_medicine                         | Seniors                                    | 69                                                 |
| surgery                                  | Adults                                     | 120                                                |
| surgery                                  | Children                                   | 56                                                 |
| surgery                                  | Seniors                                    | 78                                                 |
+------------------------------------------+--------------------------------------------+----------------------------------------------------+
```

### Месячная динамика удовлетворенности пациентов

**Цель:** Отследить, как менялась общая удовлетворенность пациентов на протяжении года.

**Что показывает эта витрина**:

- **Выявление трендов**: Позволяет увидеть общую тенденцию — растет ли удовлетворенность, падает или остается стабильной.
- **Поиск аномалий**: Можно заметить месяцы с аномально низкими или высокими оценками (например, заметное падение в марте до 76.75).
- **Основа для отчетов**: Идеально подходит для построения линейного графика для отчетов.

SQL-запрос:

```sql
CREATE TABLE patients_satisfaction_by_month AS
SELECT
    YEAR(arrival_date) as year,
    MONTH(arrival_date) as month,
    ROUND(AVG(satisfaction), 2) as avg_monthly_satisfaction
FROM patients_raw
GROUP BY YEAR(arrival_date), MONTH(arrival_date)
ORDER BY year, month;
```

Пример результата:
```sql
SELECT * FROM patients_satisfaction_by_month;

+--------------------------------------+---------------------------------------+----------------------------------------------------+
| patients_satisfaction_by_month.year  | patients_satisfaction_by_month.month  | patients_satisfaction_by_month.avg_monthly_satisfaction |
+--------------------------------------+---------------------------------------+----------------------------------------------------+
| 2025                                 | 1                                     | 80.22                                              |
| 2025                                 | 2                                     | 79.28                                              |
| 2025                                 | 3                                     | 76.75                                              |
| 2025                                 | 4                                     | 81.49                                              |
| 2025                                 | 5                                     | 80.81                                              |
| 2025                                 | 6                                     | 79.02                                              |
| 2025                                 | 7                                     | 78.84                                              |
| 2025                                 | 8                                     | 78.41                                              |
| 2025                                 | 9                                     | 79.22                                              |
| 2025                                 | 10                                    | 79.84                                              |
| 2025                                 | 11                                    | 81.42                                              |
| 2025                                 | 12                                    | 79.65                                              |
+--------------------------------------+---------------------------------------+----------------------------------------------------+
```

### Нагрузка на персонал

**Цель**: Оценить операционную нагрузку на персонал в каждом отделении.

**Что показывает эта витрина**:

- **Коэффициент нагрузки**: patients_per_staff — это ключевой показатель, демонстрирующий, сколько пациентов приходится на одного сотрудника.
- **Выявление перегрузок**: Высокие значения этого показателя — прямой сигнал о нехватке персонала на смене.
- **Сравнение отделений**: Позволяет сравнить нагрузку между разными отделениями и понять, где она распределена неравномерно.

SQL-запрос:

```sql
CREATE TABLE staff_to_patient_percentage_by_week AS
WITH WeeklyStaff AS (
    SELECT
        week,
        service,
        SUM(present) as total_staff_present
    FROM
        staff_schedule_raw
    GROUP BY week, service
)
SELECT
    s.week,
    s.service,
    ROUND(s.patients_admitted / ws.total_staff_present, 3) as patients_per_staff
FROM services_weekly_raw s
JOIN WeeklyStaff ws ON s.week = ws.week AND s.service = ws.service
WHERE ws.total_staff_present > 0;
```

Пример результата:
```sql
SELECT * FROM staff_to_patient_percentage_by_week;

+-------------------------------------------+----------------------------------------------+----------------------------------------------------+
| staff_to_patient_percentage_by_week.week  | staff_to_patient_percentage_by_week.service  | staff_to_patient_percentage_by_week.patients_per_staff |
+-------------------------------------------+----------------------------------------------+----------------------------------------------------+
| 1                                         | ICU                                          | 0.71                                               |
| 1                                         | emergency                                    | 0.914                                              |
| 1                                         | general_medicine                             | 1.37                                               |
| 1                                         | surgery                                      | 1.957                                              |
| 2                                         | ICU                                          | 0.233                                              |
| 2                                         | emergency                                    | 0.757                                              |
| 2                                         | general_medicine                             | 1.72                                               |
...
```

### Недели с самым большим количеством отказов пациентам в госпитализации

**Цель**: Найти недели, когда отделения не справлялись с потоком пациентов и процент отказов был критически высоким.

**Что показывает эта витрина**:

- **Сигнал тревоги**: Витрина подсвечивает самые провальные недели, когда система не справилась.
- **Количественная оценка проблемы**: Показывает не просто факт отказов, а их долю от общего спроса, что более информативно.
- **Фокус для расследования**: Руководство может взять конкретную неделю и отделение и детально расследовать причины коллапса.

SQL-запрос:

```sql
CREATE TABLE service_overload_by_week AS
SELECT
    week,
    service,
    SUM(patients_refused) as total_refused_patients,
    SUM(patients_request) as total_requests,
    ROUND((SUM(patients_refused) / SUM(patients_request)), 2) * 100 as refusal_percentage
FROM services_weekly_raw
WHERE patients_refused > 0
GROUP BY week, service
HAVING (SUM(patients_refused) / SUM(patients_request)) * 100 > 50
ORDER BY week, total_refused_patients DESC;
```

Пример результата:
```sql
SELECT * FROM service_overload_by_week;

+--------------------------------+-----------------------------------+--------------------------------------------------+------------------------------------------+----------------------------------------------+
| service_overload_by_week.week  | service_overload_by_week.service  | service_overload_by_week.total_refused_patients  | service_overload_by_week.total_requests  | service_overload_by_week.refusal_percentage  |
+--------------------------------+-----------------------------------+--------------------------------------------------+------------------------------------------+----------------------------------------------+
| 1                              | general_medicine                  | 164                                              | 201                                      | 81.59203980099502                            |
| 1                              | surgery                           | 85                                               | 130                                      | 65.38461538461539                            |
| 1                              | emergency                         | 44                                               | 76                                       | 57.89473684210527                            |
| 2                              | emergency                         | 141                                              | 169                                      | 83.4319526627219                             |
| 2                              | general_medicine                  | 140                                              | 183                                      | 76.50273224043715                            |
| 3                              | emergency                         | 145                                              | 177                                      | 81.92090395480226                            |
| 3                              | surgery                           | 39                                               | 66                                       | 59.09090909090909                            |
| 4                              | emergency                         | 125                                              | 157                                      | 79.61783439490446                            |
| 4                              | general_medicine                  | 109                                              | 152                                      | 71.71052631578947                            |
| 5                              | emergency                         | 363                                              | 388                                      | 93.55670103092784                            |

...
```

### Ранжирование пациентов по длительности пребывания

**Цель**: Проранжировать пациентов внутри каждого отделения по длительности госпитализации.

**Что показывает эта витрина**:

- Моментально находит пациентов, которые занимают койку дольше всех в своем отделении.
- **Поиск аномалий**: Помогает найти случаи, которые сильно выбиваются из средней продолжительности лечения.
- **Оптимизация процессов**: Анализ самых длительных случаев может помочь оптимизировать протоколы лечения и процессы выписки.

SQL-запрос:

```sql
CREATE TABLE patient_stay_rank AS
SELECT
    patient_id,
    name,
    service,
    DATEDIFF(departure_date, arrival_date) as length_of_stay,
    RANK() OVER (
        PARTITION BY service -- Ранжируем внутри каждого отделения
        ORDER BY DATEDIFF(departure_date, arrival_date) DESC -- От самого долгого к самому короткому
    ) as stay_rank_in_service
FROM
    patients_raw
ORDER BY
    service, stay_rank_in_service;
```

Пример результата:
```sql
SELECT * FROM patient_stay_rank LIMIT 10;

+-------------------------------+-------------------------+----------------------------+-----------------------------------+-----------------------------------------+
| patient_stay_rank.patient_id  | patient_stay_rank.name  | patient_stay_rank.service  | patient_stay_rank.length_of_stay  | patient_stay_rank.stay_rank_in_service  |
+-------------------------------+-------------------------+----------------------------+-----------------------------------+-----------------------------------------+
| PAT-a1bec809                  | Catherine Frazier       | ICU                        | 14                                | 1                                       |
| PAT-ac1d3980                  | Paula Brown             | ICU                        | 14                                | 1                                       |
| PAT-367ddbef                  | Lance Simmons           | ICU                        | 14                                | 1                                       |
| PAT-5b61868c                  | Ashley Waller           | ICU                        | 14                                | 1                                       |
| PAT-7aeac4f3                  | Angelica Parker         | ICU                        | 14                                | 1                                       |
| PAT-2343cf78                  | William Jones           | ICU                        | 14                                | 1                                       |
| PAT-d6e424a7                  | Tina Sanders            | ICU                        | 14                                | 1                                       |
| PAT-f3846605                  | Cheyenne Horton         | ICU                        | 14                                | 1                                       |
| PAT-fe962113                  | Beth Cline              | ICU                        | 14                                | 1                                       |
| PAT-b2e7e433                  | Samuel Wong             | ICU                        | 14                                | 1                                       |
+-------------------------------+-------------------------+----------------------------+-----------------------------------+-----------------------------------------+
```