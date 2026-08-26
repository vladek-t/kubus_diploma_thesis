FROM apache/airflow:2.7.2-python3.10

# Если нужен провайдер Trino, устанавливаем с фиксацией версии Airflow
RUN pip install --no-cache-dir "apache-airflow==2.7.2" "apache-airflow-providers-trino==5.1.1"

# Гарантируем PATH и пользователя
ENV PATH="/home/airflow/.local/bin:${PATH}"

USER root
COPY --chown=airflow:root entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
USER airflow

COPY --chown=airflow:root ./airflow/dags /opt/airflow/dags

ENTRYPOINT ["/entrypoint.sh"]