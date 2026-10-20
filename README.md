# SqueezeRadar

Real-time retail sentiment and short squeeze intelligence platform.

---

## Prerequisites

Before starting, make sure you have installed:

* Python 3.11
* Docker Desktop
* Git

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourname/squeezradar.git
cd squeezradar
```

### 2. Create and Activate a Virtual Environment

#### macOS / Linux

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

#### Windows (Command Prompt)

```cmd
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

#### Windows (PowerShell)

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

### 3. Configure Environment Variables

#### macOS / Linux

```bash
cp .env.template .env
```

#### Windows

```cmd
copy .env.template .env
```

Edit the `.env` file and add your credentials.

---

### 4. Start All Databases

```bash
docker-compose up -d
```

Verify all containers are running:

```bash
docker ps
```

---

### 5. Initialize Databases

Wait approximately **60 seconds** for Cassandra to finish starting.

Then run:

```bash
python db/mongo_indexes.py
python db/cassandra_setup.py
python db/elastic_setup.py
```

---

### 6. Apply PostgreSQL Schema

#### macOS / Linux

```bash
docker exec -i squeezradar_postgres \
  psql -U admin -d squeezradar \
  < db/schema.sql
```

#### Windows (Command Prompt)

```cmd
docker exec -i squeezradar_postgres psql -U admin -d squeezradar < db\schema.sql
```

#### Windows (PowerShell)

```powershell
Get-Content db/schema.sql | docker exec -i squeezradar_postgres psql -U admin -d squeezradar
```

---

### 7. Verify Installation

```bash
python tests/test_connections.py
```

Expected result:

```text
✓ PostgreSQL Connected
✓ MongoDB Connected
✓ Redis Connected
✓ Cassandra Connected
✓ Elasticsearch Connected

5/5 tests passing
```

---

## Database Architecture

| Database                 | Port  | Purpose                                |
| ------------------------ | ----- | -------------------------------------- |
| PostgreSQL + TimescaleDB | 5432  | Stock state, scores, OHLCV time-series |
| MongoDB                  | 27017 | Raw Reddit posts and news articles     |
| Redis                    | 6379  | Live score cache and Celery broker     |
| Cassandra                | 9042  | Market ticks and borrow rate history   |
| Elasticsearch            | 9200  | Full-text search on social data        |

---

## Branch Strategy

### Branches

* `main` → Stable, tested production-ready code only
* `scraper/[name]` → One branch per scraper implementation
* `feature/[name]` → One branch per feature development

### Rules

❌ Never push directly to `main`

✅ Create a feature branch

```bash
git checkout -b feature/my-feature
```

✅ Push your branch

```bash
git push origin feature/my-feature
```

✅ Open a Pull Request into `main`

---

## Common Commands

### Start Services

```bash
docker-compose up -d
```

### Stop Services

```bash
docker-compose down
```

### View Running Containers

```bash
docker ps
```

### View Logs

```bash
docker-compose logs -f
```

### Run Tests

```bash
python tests/test_connections.py
```

### Deactivate Virtual Environment

```bash
deactivate
```
