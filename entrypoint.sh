#!/bin/bash
set -e

AIRFLOW_BIN="/home/airflow/.local/bin/airflow"

if [ ! -f "$AIRFLOW_BIN" ]; then
    echo "ERROR: $AIRFLOW_BIN not found!"
    exit 1
fi

# Ожидаем готовности базы данных
until $AIRFLOW_BIN db check; do
  echo "Waiting for Airflow database..."
  sleep 2
done

# Инициализируем базу данных (создаёт таблицы, если их нет)
echo "Initializing Airflow database..."
$AIRFLOW_BIN db init

# Проверяем, существует ли пользователь admin
if ! $AIRFLOW_BIN users list | grep -q "admin"; then
  echo "Creating admin user..."
  $AIRFLOW_BIN users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
else
  echo "Admin user already exists, resetting password..."
  $AIRFLOW_BIN users delete --username admin || true
  $AIRFLOW_BIN users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
fi

# Запускаем оригинальный entrypoint Airflow (standalone)
exec /entrypoint "$@"