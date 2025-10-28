# Задание по Hadoop

Поскольку публичные Docker-образы для Hadoop версии `3.3.6` отсутствуют, это решение использует подход с созданием собственного базового образа.

## Структура проекта

```
hadoop_task/
├── docker-compose.yml
├── client/
│   ├── Dockerfile
│   ├── entrypoint.sh
├── hadoop.env
└── README.md
```

## Запуск

- Откройте терминал в корневой директории `hw1_hadoop`
- Выполните в терминале:

```bash
docker build -t hadoop-cluster-image:3.3.6 ./client
```

Эта команда создаст локальный образ с именем hadoop-cluster-image:3.3.6

- Запустите кластер с помощью

```bash
docker-compose up
```
