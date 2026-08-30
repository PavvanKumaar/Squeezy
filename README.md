# SqueezeRadar

Real-time retail sentiment and short squeeze intelligence platform.

## Quick Start

### Prerequisites
- Python 3.11
- Docker Desktop
- Git

### Setup (both partners do this)

1. Clone the repo
git clone https://github.com/yourname/squeezradar.git
cd squeezradar

2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

3. Set up environment
cp .env.template .env
# Edit .env and add your credentials

4. Start all databases
docker-compose up -d

5. Wait 60 seconds for Cassandra to fully start, then:
python db/mongo_indexes.py
python db/cassandra_setup.py
python db/elastic_setup.py

6. Apply PostgreSQL schema
docker exec -i squeezradar_postgres psql \
  -U admin -d squeezradar < db/schema.sql

7. Verify everything works
python tests/test_connections.py
# Must show 5/5 passing

## Database Architecture

| Database        | Port  | Used For                              |
|-----------------|-------|---------------------------------------|
| PostgreSQL+Timescale | 5432 | Stock state, scores, OHLCV time-series |
| MongoDB         | 27017 | Raw Reddit posts, news articles       |
| Redis           | 6379  | Live score cache, Celery broker       |
| Cassandra       | 9042  | Market ticks, borrow rate history     |
| Elasticsearch   | 9200  | Full-text search on social data       |

## Branch Strategy

- main — stable, tested code only
- scraper/[name] — one branch per scraper
- feature/[name] — one branch per feature

Never push directly to main.