# prueba-tecnica-data-engineer

## Resumen del Proyecto

Este repositorio contiene un pipeline ETL para el procesamiento incremental de transacciones financieras. El sistema lee lotes de datos crudos (Extracción), los limpia y aplica un modelo de detección de fraude basado en reglas de negocio (Transformación), y persiste el resultado en un Data Warehouse (PostgreSQL) optimizado para análisis (Carga). Toda la infraestructura se gestiona mediante Docker Compose.

## Instrucciones de Setup (Código Ejecutable)

Sigue estos pasos para levantar el entorno y ejecutar el pipeline.

### Prerrequisitos

* **Docker Desktop** (Debe estar instalado y corriendo).
* **Python 3.8+**
* **Git**

### 1. Clonar el Repositorio

bash
git clone [https://docs.github.com/es/repositories/creating-and-managing-repositories/quickstart-for-repositories](https://docs.github.com/es/repositories/creating-and-managing-repositories/quickstart-for-repositories)
cd RetoTecnico-DataEngineer

# Crear y activar el entorno virtual
python -m venv venv
# Windows:
.\venv\Scripts\activate


# Instalar dependencias
pip install -r requirements.txt

# Levantar infraestructura (docker)
docker-compose up -d

# Ejecutar el pipeline
python main.py

# Para confirmar la carga, se accede a la consola de PosrtgreSQL
docker exec -it retotecnico-dataengineer-db-1 psql -U usuarioDB -d data_warehouse

# Una vez dentro de la consola psql:
SELECT COUNT(*) FROM fact_transactions;
\q

## Fases completadas
Fase 1: Setup y estructura

Fase 2: Lógica ETL y detección de fraude

Fase 3: Datawarehouse (extra: se dockerizó PostgreSQL con docker-compose)

## Fases faltantes
Fase 4: Procesamiento en tiempo real

## Tiempo invertido
Setup y logica ETL - 4 horas
Configuracion Docker y PostgreSQL - 3 horas
Pruebas y documentacion - 1 hora
