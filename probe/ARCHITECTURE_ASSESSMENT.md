# Apis v0.1 Architecture Assessment & Local Development Strategy

## Executive Summary

The **trigger-based CDC (Change Data Capture) approach is solid** and widely used in production systems. The minimal modification strategy (CHANGES_LOG table + triggers) is the right tradeoff given Firebird's limitations. However, there are opportunities to simplify the architecture and improve the local development experience.

---

## Part 1: Is the Probe + Cloud Backup Approach Right?

### What's Working Well

1. **Trigger-based CDC is proven** - Used by Debezium, Maxwell, and enterprise CDC tools
2. **Event-driven architecture** - No polling overhead, real-time change detection
3. **CHANGES_LOG pattern** - Standard approach for tracking mutations
4. **Multi-threaded processing** - Good throughput potential
5. **Comprehensive test suite** - 45+ tests, good coverage

### Concerns with Current Implementation

| Issue | Impact | Recommendation |
|-------|--------|----------------|
| **Kafka on Windows** | Requires JVM + Kafka on each customer's old Windows machine | Replace with direct HTTPS to cloud |
| **Hardcoded `kafka:9093`** | Assumes Docker networking | Make configurable |
| **No retry/backoff** | Failed sends could lose data | Add persistent queue with retry |
| **Single topic** | All tables mixed together | Consider per-table topics or routing |

### Recommended Architecture

```
Current:
  Microsip DB → Triggers → CHANGES_LOG → Probe → Kafka → ??? → Cloud

Recommended:
  Microsip DB → Triggers → CHANGES_LOG → Probe → HTTPS → Cloud API → Database
                                              ↓
                                    (local SQLite buffer for offline/retry)
```

**Why remove Kafka?**
- Kafka requires Java + significant resources on old Windows machines
- For per-customer installs, a simple HTTPS client with retry is sufficient
- Kafka makes sense at scale (central server) but not per-machine

**Suggested cloud stack:**
- **API Layer**: AWS API Gateway + Lambda, or a simple FastAPI/Express server on EC2/Cloud Run
- **Database**: PostgreSQL (RDS/Cloud SQL) or Supabase for managed Postgres
- **Queue (if needed server-side)**: SQS or Cloud Tasks for async processing

---

## Part 2: Local Development Strategy

### The Challenge
- Microsip runs on old Windows machines
- You have a `.fdb` file and access to a production machine
- Need to develop without risking production

### Recommended Setup: Docker + Firebird

**1. Create a local Firebird container:**
```bash
# docker-compose.yml for local dev
version: '3.8'
services:
  firebird:
    image: jacobalberty/firebird:v4.0
    ports:
      - "3050:3050"
    volumes:
      - ./test-data:/firebird/data
    environment:
      - FIREBIRD_DATABASE=microsip_test.fdb
      - FIREBIRD_USER=sysdba
      - FIREBIRD_PASSWORD=masterkey
```

**2. Development workflow:**
```
┌─────────────────────────────────────────────────────────┐
│  Your Mac (Development)                                 │
│  ┌─────────────────┐    ┌─────────────────┐            │
│  │ Docker Firebird │    │ Mock Cloud API  │            │
│  │ (test .fdb)     │    │ (local server)  │            │
│  └────────┬────────┘    └────────┬────────┘            │
│           │                      │                      │
│           └──────────┬───────────┘                      │
│                      │                                  │
│           ┌──────────▼──────────┐                       │
│           │  Probe Application  │                       │
│           │  (Python)           │                       │
│           └─────────────────────┘                       │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│  Windows Machine (E2E Testing Only)                     │
│  - Real Microsip installation                           │
│  - Full integration validation                          │
│  - Use sparingly (production risk)                      │
└─────────────────────────────────────────────────────────┘
```

**3. Test data generation:**
- Your existing `fennec.py` generates test data - expand this
- Create scripts to populate test Firebird DB with realistic Microsip schema

**4. Platform compatibility:**
- The `fdb` Python library works on macOS/Linux/Windows
- Firebird client libraries available for all platforms
- Your code is already cross-platform (see `fdb_helper.py` platform checks)

---

## Part 3: Recommended Next Steps

### Immediate (v0.1 improvements)

1. **Replace Kafka with HTTP client**
   - Create a simple `CloudSyncClient` class
   - Add local SQLite buffer for offline resilience
   - Implement exponential backoff retry

2. **Make configuration external**
   - Move hardcoded values to config file/env vars
   - Database path, cloud API endpoint, credentials

3. **Set up Docker-based local dev**
   - Firebird container
   - Mock cloud API for testing

### Short-term (v0.2)

4. **Build cloud API**
   - Simple REST API to receive change events
   - Store in PostgreSQL
   - Basic authentication (API keys per customer)

5. **Add offline buffering**
   - SQLite queue for changes when cloud is unreachable
   - Sync when connection restored

### Medium-term (v1.0)

6. **Mobile app API**
   - Read-only endpoints for the mobile app
   - Proper authentication (JWT/OAuth)

7. **Multi-tenant architecture**
   - Each customer isolated
   - Dashboard for monitoring probe health

---

## Part 4: Alternative Approaches Considered

| Approach | Pros | Cons | Verdict |
|----------|------|------|---------|
| **Trigger-based CDC (current)** | Real-time, efficient, proven | Modifies DB | Best option |
| **Polling with timestamps** | Truly read-only | Requires timestamp columns on all tables, less efficient | Microsip tables may not have timestamps |
| **Firebird trace API** | Read-only | Complex parsing, may miss data | Not reliable |
| **Full database snapshots** | Simple, read-only | Bandwidth heavy, no real-time | Not scalable |
| **Firebird replication** | Native feature | Limited support, complex setup | Over-engineered |

---

## Part 5: Verification & Testing Strategy

### Local testing (Docker)
```bash
# 1. Start Firebird container
docker-compose up -d firebird

# 2. Run probe against test database
python probe/main.py --config dev.yaml

# 3. Run test suite
python probe/tests/run_tests.py
```

### Integration testing (Windows machine)
- Only for final validation before releases
- Use a separate test database if possible
- Never test on production data

### Cloud integration testing
- Mock cloud API locally for unit tests
- Use staging cloud environment for integration tests

---

## Summary

| Question | Answer |
|----------|--------|
| **Is probe + cloud backup the right approach?** | Yes - trigger-based CDC is the right pattern for real-time sync |
| **Should you keep Kafka?** | No - replace with direct HTTPS + local buffer for per-customer installs |
| **Is minimal DB modification acceptable?** | Yes - CHANGES_LOG + triggers is a reasonable tradeoff |
| **Best local dev approach?** | Docker Firebird + mock cloud API |

The foundation is solid. The main changes needed are simplifying the cloud sync (remove Kafka) and setting up a proper local development environment.
