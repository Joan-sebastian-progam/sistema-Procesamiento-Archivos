# 📂 Sistema de Procesamiento de Archivos

Pipeline de procesamiento de archivos CSV basado en eventos, implementado con una arquitectura de microservicios orquestada con Docker Compose.

---

## 🏗️ Arquitectura

```
Cliente
  │
  ▼ SFTP (puerto 2222)
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ sftp-server │────▶│sftp-watcher │────▶│    Kafka    │
│  atmoz/sftp │     │   Python    │     │  topic:     │
│             │     │  Paramiko   │     │file.received│
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                    ┌──────────────────────────┘
                    ▼
             ┌─────────────┐     ┌─────────────┐
             │file-processor│────▶│  PostgreSQL  │
             │   Python    │     │  file_jobs  │
             │    CSV/DB   │     │    table    │
             └─────────────┘     └──────┬──────┘
                                        │
                    ┌───────────────────┘
                    ▼
             ┌─────────────┐     ┌─────────────┐
             │  FastAPI    │────▶│   Angular   │
             │  REST API   │     │  Dashboard  │
             │  WebSocket  │     │  puerto 4200│
             └─────────────┘     └─────────────┘
```

---

## 🛠️ Tecnologías

| Componente | Tecnología | Puerto |
|---|---|---|
| SFTP Server | atmoz/sftp | 2222 |
| Message Broker | Apache Kafka + Zookeeper | 9092 |
| Kafka UI | Provectus Kafka UI | 8090 |
| Base de Datos | PostgreSQL 15 | 5432 |
| File Watcher | Python 3.11 | — |
| File Processor | Python 3.11 + psycopg2 | — |
| REST API | FastAPI + Uvicorn | 8000 |
| Dashboard | Angular 17 | 4200 |
| Orquestación | Docker Compose | — |

---

## 📁 Estructura del Proyecto

```
sistema-Procesamiento-datos/
├── docker-compose.yml
├── ventas.csv                          # Archivo de prueba
├── services/
│   ├── sftp-watcher/
│   │   ├── Dockerfile
│   │   ├── watcher.py                  # Detecta archivos y publica en Kafka
│   │   └── requirements.txt
│   ├── file-processor/
│   │   ├── Dockerfile
│   │   ├── processor.py                # Consume Kafka, valida y persiste
│   │   └── requirements.txt
│   └── api/
│       ├── Dockerfile
│       ├── main.py                     # FastAPI REST + WebSocket
│       └── requirements.txt
└── dashboard/
    ├── Dockerfile
    ├── angular.json
    ├── proxy.conf.json                 # Proxy hacia API
    └── src/
        ├── main.ts
        ├── index.html
        └── app/
            ├── app.ts
            ├── app.config.ts
            ├── app.routes.ts
            └── pages/
                └── monitor/
                    └── monitor.component.ts
```

---

## 🚀 Cómo Ejecutar

### Prerequisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- Git

### 1. Clonar el repositorio

```bash
git clone https://github.com/Joan-sebastian-progam/sistema-Procesamiento-datos.git
cd sistema-Procesamiento-datos
```

### 2. Levantar todos los servicios

```bash
docker compose up --build -d
```

Espera ~60 segundos para que todos los servicios inicien correctamente.

### 3. Verificar que todo está corriendo

```bash
docker compose ps
```

Todos los contenedores deben estar en estado `Up`.

### 4. Subir un archivo CSV para procesar

```bash
docker cp ventas.csv sftp-server:/home/uploader/upload/ventas.csv
```

### 5. Ver el resultado en el Dashboard

Abre el navegador en:

```
http://localhost:4200
```

---

## 📊 Endpoints de la API

| Método | Endpoint | Descripción |
|---|---|---|
| GET | `/jobs` | Lista todos los jobs |
| GET | `/jobs/{id}` | Detalle de un job |
| GET | `/stats` | Estadísticas por estado |
| WS | `/ws` | WebSocket en tiempo real |

Documentación interactiva disponible en: `http://localhost:8000/docs`

---

## 🗄️ Schema de Base de Datos

```sql
CREATE TABLE file_jobs (
    id           VARCHAR PRIMARY KEY,   -- MD5 checksum del archivo
    filename     VARCHAR NOT NULL,
    status       VARCHAR NOT NULL,      -- received/processing/validated/completed/failed
    path         VARCHAR,
    size         BIGINT,
    checksum     VARCHAR,
    received_at  TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    error_message TEXT,
    rows_processed INTEGER,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 🔄 Flujo de Procesamiento

```
1. Cliente sube archivo CSV vía SFTP → puerto 2222
2. sftp-watcher detecta el archivo cada 5 segundos
3. Lee el contenido, calcula MD5 como ID único
4. Publica evento JSON en Kafka topic: file.received
5. Elimina el archivo del servidor SFTP
6. file-processor consume el evento de Kafka
7. Valida columnas requeridas: id, name, value
8. Transforma y normaliza los datos
9. Persiste el job en PostgreSQL con status: completed
10. Publica evento en Kafka topic: file.processed
11. API hace broadcast por WebSocket al dashboard
12. Dashboard Angular actualiza la tabla en tiempo real
```

---

## 📋 Formato del CSV

El archivo CSV debe tener las siguientes columnas:

```csv
id,name,value
1,Juan Perez,100.5
2,Maria Garcia,200.0
3,Carlos Lopez,300.0
```

---

## 🛑 Detener el Sistema

```bash
docker compose down
```

Para eliminar también los volúmenes de datos:

```bash
docker compose down -v
```

---

## 🧰 Herramientas de Monitoreo

| Herramienta | URL |
|---|---|
| Dashboard Angular | http://localhost:4200 |
| API Docs (Swagger) | http://localhost:8000/docs |
| Kafka UI | http://localhost:8090 |

---

## 👤 Autor

**Joan Sebastian** — [GitHub](https://github.com/Joan-sebastian-progam)