# MADI — Medellín Apartment Data Integration
> Pipeline de datos de end-to-end. Automatiza la búsqueda de un apartamento muy específico en **Medellín, Colombia**. Ejecutándose totalmente en una máquina local.

---

## 📌 Overview

**MADI** convierte la búsqueda manual en una automatización. Extrae publicaciones de plataformas como Finca Raíz vía **API** (Apify), los procesa mediante una **arquitectura Medallion**, los modela en un **Esquema Estrella** con **dbt**, y produce una única tabla analítica — `mart_ideal_apartments` — que contiene sólo los anuncios que cumplen los criterios objetivos, ordenados por precio (los más baratos primero).

Orquestado por **Airflow**, dentro de **Docker**. Data Warehouse, Orquestador, Spark y dbt se levanta con un único comando.

---

> **Decisiones de soporte:** dbt vive en un **virtualenv aislado** (`/opt/dbt_venv`) para evitar conflictos de dependencias con Airflow; el **driver JDBC de Postgres** está incluido en la imagen para que la escritura Spark→Postgres funcione offline; el **contenedor PostgreSQL aloja dos bases de datos** (metadatos de Airflow + el warehouse `madi_dwh`) para mantener la huella pequeña; y **todos los parámetros y secretos están centralizados** en `config/config.yaml` + `.env`.

---

## 📊 Arquitectura (Medallion)

```mermaid
flowchart LR
    subgraph SRC["🌐 Source"]
        A["API<br/>1. finca-raiz-scraper"]
    end

    subgraph BRONZE["🥉 Bronze — raw"]
        B["data/raw/<br/>finca_raiz_*.json"]
    end

    subgraph SILVER["🥈 Silver — clean & tabular"]
        C["PySpark<br/>cast · flatten · derive allows_pets"]
        D["data/processed/<br/>Parquet"]
        E["Postgres<br/>staging.stg_listings"]
    end

    subgraph GOLD["🥇 Gold — modeled"]
        F["dbt Star Schema<br/>dims + fct_listings"]
        G["mart_ideal_apartments<br/>Ej: 3 hab · pet-friendly · &lt; 2.5M COP"]
    end

    A -->|extract| B
    B -->|read| C
    C -->|write| D
    C -->|JDBC load| E
    E -->|dbt run| F
    F -->|filter & join| G

    classDef bronze fill:#cd7f32,stroke:#000,color:#fff
    classDef silver fill:#9aa0a6,stroke:#000,color:#fff
    classDef gold fill:#d4af37,stroke:#000,color:#000
    class B bronze
    class C,D,E silver
    class F,G gold
```

**Orquestación** — un único DAG de Airflow (`pipeline/` - `madi_finca_raiz_pipeline`) ejecuta las etapas estrictamente en secuencia:

```mermaid
flowchart LR
    extract["extract<br/>(PythonOperator)"] --> transform["pyspark_transform<br/>(PythonOperator)"]
    transform --> dbtrun["dbt_run<br/>(BashOperator · $DBT_BIN)"]
    dbtrun --> dbttest["dbt_test<br/>(BashOperator · $DBT_BIN)"]
```

La ruta del archivo raw es pasada de `extract` a `pyspark_transform` vía XCom*


### ⭐ Modelo dimensional (Gold)

```mermaid
erDiagram
    FCT_LISTINGS }o--|| DIM_LOCATION : location_key
    FCT_LISTINGS }o--|| DIM_PROPERTY_TYPE : property_type_key
    FCT_LISTINGS }o--|| DIM_CONTACT : contact_key
    FCT_LISTINGS }o--|| DIM_DATE : "published/updated_date_key"

    FCT_LISTINGS {
        bigint listing_id PK
        text location_key FK
        text property_type_key FK
        text contact_key FK
        bigint price
        double area_m2
        int bedrooms
        boolean allows_pets
    }
    DIM_LOCATION { text location_key PK }
    DIM_PROPERTY_TYPE { text property_type_key PK }
    DIM_CONTACT { text contact_key PK }
    DIM_DATE { int date_key PK }
```

---

## 📁 Estructura del proyecto

```
MADI/
├── config/
│   └── config.yaml             #   Filtros, conexiones, inputs...
├── data/
│   ├── raw/                    #   Bronze — JSON
│   └── processed/              #   Silver — Parquet
├── docker/
│   └── postgres/
│       └── init/               #   Crea madi_dwh + schemas
├── etl/
│   ├── extract/                #   Ingesta (API)
│   ├── transform/              #   Limpieza (PySpark)
├── pipelines/
│   └── finca_raiz_pipeline.py  #   DAG
├── src/                        #   Lógica compartida y reutilizable (sin duplicar)
│   ├── utils/                  #   config_loader · logger · db · spark · apify_extractor
│   ├── data/                   #   features.py (regex allows_pets — fuente única)
│   └── validation/             #   raw_schema.py (chequeos de sanidad del payload Bronze)
├── Dockerfile                  #   Imagen (Java + PySpark + dbt + JDBC + pytest)
├── docker-compose.yml          #   Postgres + Airflow (init/webserver/scheduler)
├── requirements*.txt           #   dependencias runtime / dbt / dev
└── .env.example                #   plantilla para secretos

Working On
-----------------------------------------------------------------------------------------------
├── dbt/                        #   Gold — dbt (Esquema Estrella + mart)
│   ├── dbt_project.yml         #   Enrutamiento de esquemas
│   ├── profiles.yml            #   Conexión vía variables de entorno
│   ├── macros/                 #   generate_schema_name, null-safe surrogate_key
│   ├── models/
│   │   ├── staging/            #   stg_finca_raiz__listings
│   │   └── marts/
│   │       ├── dimensions/     #   dim_location · dim_property_type · dim_contact · dim_date
│   │       ├── facts/          #   fct_listings
│   │       └── analytics/      #   mart_ideal_apartments  ← Tabla Final
│   └── tests/                  #   Check del mart (cumple el filtro)
├── tests/                      #  suite pytest (+ fixtures/)
└── pytest.ini
-----------------------------------------------------------------------------------------------
```

---

## Stack
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

## Resumen
| Etapa | Capa | Tech | Salida |
|-------|-------|------|--------|
| Extract | Bronze | Apify | `data/raw/finca_raiz_*.json` |
| Transform | Silver | PySpark | `data/processed/*.parquet` + `staging.stg_listings` |
| Model | Gold | dbt | `marts.*` dims/facts + `mart_ideal_apartments` |
| Test | — | — | — |