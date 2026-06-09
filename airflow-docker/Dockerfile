# استخدم إصدار محدد وثابت لضمان استقرار البيئة
FROM apache/airflow:3.2.1-python3.12

# 1. تثبيت أدوات النظام دفعة واحدة وتنظيف الكاش فوراً لتقليل حجم الصورة
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. إنشاء الـ venv وضبط الملكية فوراً لتجنب مشاكل الصلاحيات في الـ Runtime
USER airflow
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery>=1.11.1

# 3. تثبيت المكتبات الأساسية لـ Airflow
RUN pip install --no-cache-dir \
    requests>=2.34.2 \
    python-dotenv>=0.9.9 \
    astronomer-cosmos>=1.14.2

# 4. نسخ الكود مع ضبط الملكية (Ownership) لمنع الـ init من تعديلها عند كل تشغيل
COPY --chown=airflow:root dbt_earthquake_project/ /opt/airflow/dbt_earthquake_project
COPY --chown=airflow:root src/ /opt/airflow/src
COPY --chown=airflow:root airflow-docker/dags/ /opt/airflow/dags


RUN chmod -R 755 /opt/airflow/dbt_earthquake_project