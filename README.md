# Integrantes

- Marcel Silva
- Aylin Sandoval

# Super-Market

Pipeline de datos batch que procesa transacciones de venta desde múltiples fuentes (Chile, Argentina, Perú), las enriquece y consolida en métricas Gold usando **Apache Beam → PostgreSQL → dbt**, orquestado con **Apache Airflow**.


---

## Arquitectura

```
data/raw/               ← Fuente (CSV)
    sales_data.csv
    status_logs.csv
        │
        ▼
[ Apache Beam ]         ← Capa Bronze → Silver
  Parseo, validación,
  CoGroupByKey,
  escritura Parquet
        │
        ├──► data/silver/sales_enriched.parquet
        │
        ├──► data/audit/rejected_sales.csv
        │
        ▼
[ PostgreSQL ]          ← tabla raw_sales
        │
        ▼
[ dbt ]                 ← Capa Silver → Gold
  stg-sales (view)
  sales_metrics (table)
        │
        ▼
[ Airflow DAG ]         ← Orquestación diaria
```

---

## Requisitos

- Docker Desktop (con Docker Compose)
- Git

No se requiere Python, dbt ni Airflow instalados localmente; todo corre en contenedores.

---

## Inicio rápido

```bash
# 1. Clonar el repositorio
git clone <url-del-repo>
cd bdge-proyecto

# 2. Levantar todos los servicios
docker compose up --build

# 3. Acceder a la interfaz de Airflow
#    URL:      http://localhost:8080
#    Usuario:  admin
#    Contraseña: admin
```

El servicio `beam-pipeline` corre automáticamente al iniciar y genera el Parquet en `data/silver/`. Airflow queda disponible para ejecutar el DAG de forma manual o diaria.

---

## Servicios Docker

| Servicio           | Imagen                    | Puerto | Descripción                              |
|--------------------|---------------------------|--------|------------------------------------------|
| `postgres`         | postgres:15               | 5432   | Base de datos principal (metadata + gold)|
| `beam-pipeline`    | (build local)             | —      | Ejecuta el ETL Beam y carga a Postgres   |
| `airflow-webserver`| apache/airflow:2.9.1      | 8080   | UI de Airflow                            |
| `airflow-scheduler`| apache/airflow:2.9.1      | —      | Scheduler de Airflow                     |

---

## DAG de Airflow — `global_mart_consolidation_pipeline`

**Programación:** diaria (`@daily`)  
**Flujo de tareas:**

```
extract_and_transform_silver  ──►  sensor_silver_data  ──►  load_and_model_gold
       (Beam ETL)                   (FileSensor)                  (dbt)
```

**Tarea 1 — `extract_and_transform_silver`**
Ejecuta `pipeline.py` con Beam: lee los CSV de `data/raw/`, valida registros, hace join por `transaction_id`, y escribe el Parquet en `data/silver/`. Los registros inválidos van a `data/audit/rejected_sales.csv`.

**Tarea 2 — `sensor_silver_data`**
Espera a que `data/silver/sales_enriched.parquet` exista antes de continuar. Timeout de 120 segundos.

**Tarea 3 — `load_and_model_gold`**
Corre `dbt run` y `dbt test` sobre el proyecto `dbt-proyect/`. Genera la vista `stg-sales` y la tabla Gold `sales_metrics` en PostgreSQL.

---

## Modelos dbt

**`staging/stg-sales`** (vista)  
Desanida los campos JSON de `raw_sales` (financials, metadata, status_history) en columnas tipadas planas.

**`marts/sales_metrics`** (tabla)  
Agrega por `store_id` y `currency`:

| Columna              | Descripción                     |
|----------------------|---------------------------------|
| `store_id`           | Identificador de tienda         |
| `currency`           | Moneda (CLP / ARS / PEN)        |
| `total_transactions` | Número de transacciones válidas |
| `total_revenue`      | Suma de montos                  |

---

## Validaciones del pipeline

El pipeline rechaza automáticamente registros con:

- `transaction_id` o `sku` vacíos
- Moneda fuera de `CLP`, `ARS`, `PEN`
- `amount` no numérico o negativo/cero
- Estado de log fuera de `CREATED`, `PENDING`, `COMPLETED`, `REFUNDED`

Los rechazos quedan en `data/audit/rejected_sales.csv` con columnas `source, reason, raw`.

---

## Configuración de conexiones en Airflow (paso requerido)

El `FileSensor` del DAG necesita una conexión configurada manualmente:

1. Ir a **Admin → Connections** en la UI de Airflow.
2. Crear nueva conexión:
   - **Conn Id:** `fs_default`
   - **Conn Type:** `File System`
   - **Extra:** `{"path": "/"}`
3. Guardar.

Sin este paso, la tarea `sensor_silver_data` fallará con un error de conexión no encontrada.

---

## Variables de entorno relevantes

| Variable                              | Valor                                              |
|---------------------------------------|----------------------------------------------------|
| `POSTGRES_USER`                       | `admin`                                            |
| `POSTGRES_PASSWORD`                   | `password123`                                      |
| `POSTGRES_DB`                         | `supermarket`                                      |
| `AIRFLOW__CORE__EXECUTOR`             | `LocalExecutor`                                    |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://admin:password123@postgres/supermarket` |

---

## Estructura del repositorio

```
bdge-proyecto/
├── beam/
│   ├── pipeline.py          # ETL principal (Apache Beam)
│   └── Dockerfile           # Imagen del pipeline
├── dags/
│   └── global_mart_consolidation_pipeline.py  # DAG de Airflow
├── dbt-proyect/
│   ├── models/
│   │   ├── staging/stg-sales.sql
│   │   └── marts/sales_metrics.sql
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/
│   ├── raw/                 # Archivos de entrada
│   ├── silver/              # Parquet generado por Beam
│   └── audit/               # Registros rechazados
└── docker-compose.yml
```
