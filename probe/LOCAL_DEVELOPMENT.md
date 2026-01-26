# Local Development Setup

This guide explains how to set up a local development environment for the probe application.

## Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Firebird client libraries (for connecting to Docker Firebird from host)

## Quick Start

### 1. Start the infrastructure

```bash
# Start Firebird database and mock API server
docker-compose up -d

# Check that services are running
docker-compose ps
```

### 2. Set up your environment

```bash
# Copy the development environment file
cp .env.dev .env

# Or source it directly
source .env.dev
```

### 3. Run the probe

```bash
# With environment variables
python main.py --env

# Or with auto-discovery (if you have a local .fdb file)
python main.py
```

## Services

### Firebird Database

- **Container**: `probe-firebird`
- **Port**: 3050
- **Connection**: `localhost:3050/firebird/data/microsip_test.fdb`
- **User**: sysdba
- **Password**: masterkey

### Mock Cloud API

- **Container**: `probe-mock-api`
- **Port**: 8080
- **Endpoints**:
  - `GET /health` - Health check
  - `GET /api/changes` - List received changes (for debugging)
  - `POST /api/changes` - Submit a change event

## Common Tasks

### View mock API logs

```bash
docker-compose logs -f mock-api
```

### View received changes

```bash
curl http://localhost:8080/api/changes | python -m json.tool
```

### Reset the database

```bash
# Remove the test database
rm -rf test-data/*.fdb

# Restart Firebird
docker-compose restart firebird
```

### Run tests

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=.
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Local Development                                      │
│  ┌─────────────────┐    ┌─────────────────┐            │
│  │ Docker Firebird │    │ Mock Cloud API  │            │
│  │ (port 3050)     │    │ (port 8080)     │            │
│  └────────┬────────┘    └────────┬────────┘            │
│           │                      │                      │
│           └──────────┬───────────┘                      │
│                      │                                  │
│           ┌──────────▼──────────┐                       │
│           │  Probe Application  │                       │
│           │  (Python)           │                       │
│           └─────────────────────┘                       │
└─────────────────────────────────────────────────────────┘
```

## Configuration

All configuration is done via environment variables. See `.env.example` for the full list.

### Key Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PROBE_DB_PATH` | Path to Firebird database | (required) |
| `PROBE_CLOUD_ENDPOINT` | Cloud API endpoint | `http://localhost:8080/api/changes` |
| `PROBE_CLOUD_API_KEY` | API key for authentication | (none) |
| `PROBE_MAX_WORKERS` | Number of worker threads | 10 |

## Troubleshooting

### Cannot connect to Firebird

1. Check if the container is running: `docker-compose ps`
2. Check Firebird logs: `docker-compose logs firebird`
3. Ensure Firebird client libraries are installed on your host

### Mock API not receiving events

1. Check if the mock-api container is running
2. Check the endpoint URL in your config
3. View mock-api logs: `docker-compose logs -f mock-api`

### Buffer file growing

If the `probe_buffer.db` file keeps growing, it means events aren't being sent successfully. Check:
1. Mock API is running
2. Endpoint URL is correct
3. No network issues between probe and API
