<p align="center">
  <img src="docs/assets/logo.svg" alt="OpenAdServer" width="400"/>
</p>

<h1 align="center">OpenAdServer</h1>

<p align="center">
  <strong>Open Source Ad Serving Platform with ML-Powered CTR Prediction</strong><br>
  <em>Production-ready ad server for SMBs, startups, and developers</em>
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-documentation">Docs</a> •
  <a href="#-benchmarks">Benchmarks</a> •
  <a href="#-roadmap">Roadmap</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.11+-blue.svg" alt="Python 3.11+"/>
  <img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License"/>
  <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" alt="PRs Welcome"/>
  <a href="https://github.com/pysean/openadserver/stargazers">
    <img src="https://img.shields.io/github/stars/pysean/openadserver?style=social" alt="Stars"/>
  </a>
</p>

<p align="center">
  <sub>
    🌟 If this project helps you, please give it a star! 🌟
  </sub>
</p>

---

## 🤔 Why OpenAdServer?

Most ad servers are either **too simple** (just serving static banners) or **too complex** (requiring Google-scale infrastructure).

**OpenAdServer** is the sweet spot — a **production-ready, self-hosted ad platform** with **machine learning powered CTR prediction**, designed for teams who want full control without the complexity.

### Comparison

| Feature | OpenAdServer | Google Ad Manager | Revive Adserver | AdButler |
|---------|:------------:|:-----------------:|:---------------:|:--------:|
| Self-hosted | ✅ | ❌ | ✅ | ❌ |
| ML CTR Prediction | ✅ DeepFM/LR | ❌ | ❌ | ❌ |
| Real-time eCPM Bidding | ✅ | ✅ | ❌ | ⚠️ |
| Modern Tech Stack | ✅ Python/FastAPI | N/A | ❌ PHP | ❌ |
| One-click Deploy | ✅ Docker | ❌ | ⚠️ | ❌ |
| Free & Open Source | ✅ | ❌ | ✅ | ❌ |
| No Revenue Share | ✅ | ❌ 💰 | ✅ | ❌ 💰 |

### Perfect For

- 🏢 **SMBs** building their own ad network
- 🎮 **Gaming companies** monetizing in-app traffic
- 📱 **App developers** running house ads or direct deals
- 🛒 **E-commerce** platforms with sponsored listings
- 🔬 **Researchers** studying computational advertising
- 🎓 **Students** learning ad-tech systems

---

## ✨ Features

### 🚀 Ad Serving
- **High-Performance API** — <10ms P99 latency with FastAPI
- **Multiple Ad Formats** — Banner, native, video, interstitial
- **Smart Targeting** — Geo, device, OS, demographics, interests
- **Frequency Capping** — Daily/hourly limits per user
- **Budget Pacing** — Smooth delivery throughout the day

### 🤖 Machine Learning
- **CTR Prediction Models** — DeepFM, Logistic Regression, FM
- **Real-time Inference** — <5ms prediction latency
- **Automatic Feature Engineering** — Sparse/dense feature processing
- **Model Hot-swap** — Update models without downtime

### 💰 Monetization
- **eCPM Ranking** — Maximize revenue automatically
- **Multiple Bid Types** — CPM, CPC, CPA, oCPM
- **Real-time Bidding Ready** — OpenRTB compatible (roadmap)

### 📊 Analytics
- **Event Tracking** — Impressions, clicks, conversions
- **Real-time Dashboards** — Grafana integration
- **Prometheus Metrics** — Full observability

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Ad Request Flow                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   📱 Client                                                      │
│      │                                                          │
│      ▼                                                          │
│   ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌─────────┐ │
│   │ FastAPI  │───▶│ Retrieval │───▶│ Ranking  │───▶│Response │ │
│   │  Router  │    │(Targeting)│    │ (eCPM)   │    │         │ │
│   └──────────┘    └───────────┘    └──────────┘    └─────────┘ │
│        │               │                │                       │
│        ▼               ▼                ▼                       │
│   ┌──────────┐    ┌───────────┐    ┌──────────┐                │
│   │PostgreSQL│    │   Redis   │    │ PyTorch  │                │
│   │(Campaigns)│   │  (Cache)  │    │ (Models) │                │
│   └──────────┘    └───────────┘    └──────────┘                │
│                                                                 │
│   Pipeline: Retrieve → Filter → Predict → Rank → Return        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Option 1: Docker Compose (Recommended)

```bash
# Clone the repository
git clone https://github.com/pysean/openadserver.git
cd openadserver

# Start all services (PostgreSQL, Redis, Ad Server)
docker compose up -d

# Initialize sample data
python scripts/init_test_data.py

# Verify it's running
curl http://localhost:8000/health
# {"status":"healthy","version":"1.0.0"}
```

### Option 2: Local Development

```bash
# Prerequisites: Python 3.11+, PostgreSQL 14+, Redis 6+

# Install dependencies
pip install -e ".[dev]"

# Start databases
docker compose up -d postgres redis

# Run the server
OPENADSERVER_ENV=dev python -m openadserver.ad_server.main
```

### 📡 Your First Ad Request

```bash
curl -X POST http://localhost:8000/api/v1/ad/request \
  -H "Content-Type: application/json" \
  -d '{
    "slot_id": "banner_home",
    "user_id": "user_12345",
    "device": {"os": "ios", "os_version": "17.0"},
    "geo": {"country": "US", "city": "new_york"},
    "num_ads": 3
  }'
```

**Response:**
```json
{
  "request_id": "req_a1b2c3d4",
  "ads": [
    {
      "ad_id": "ad_1001_5001",
      "campaign_id": 1001,
      "creative": {
        "title": "Summer Sale - 50% Off!",
        "description": "Limited time offer",
        "video_url": "https://cdn.example.com/video/summer-sale.mp4",
        "landing_url": "https://shop.example.com/sale",
        "duration": 30,
        "width": 1920,
        "height": 1080,
        "mime_type": "video/mp4"
      },
      "tracking": {
        "impression_url": "http://localhost:8000/api/v1/event/track?type=impression&req=req_a1b2c3d4&ad=1001",
        "click_url": "http://localhost:8000/api/v1/event/track?type=click&req=req_a1b2c3d4&ad=1001"
      },
      "metadata": {
        "ecpm": 35.50,
        "pctr": 0.0355
      }
    }
  ],
  "count": 1
}
```

---

## 📖 Documentation

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/ad/request` | POST | Request ads for a placement |
| `/api/v1/event/track` | GET/POST | Track impression/click/conversion |
| `/api/v1/campaign` | CRUD | Manage campaigns |
| `/api/v1/creative` | CRUD | Manage creatives |
| `/api/v1/advertiser` | CRUD | Manage advertisers |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

### Configuration

```yaml
# configs/production.yaml
server:
  host: "0.0.0.0"
  port: 8000
  workers: 4

database:
  host: "postgres"
  port: 5432
  name: "openadserver"
  user: "adserver"
  password: "${DB_PASSWORD}"

redis:
  host: "redis"
  port: 6379
  db: 0

ad_serving:
  enable_ml_prediction: true
  model_path: "models/deepfm_ctr.pt"
  default_pctr: 0.01
  default_pcvr: 0.001
```

### Train Your Own CTR Model

```bash
# Prepare training data from your logs
python scripts/prepare_training_data.py \
  --input logs/events/ \
  --output data/training/

# Train DeepFM model
python -m openadserver.trainer.train_ctr \
  --model deepfm \
  --data data/training/train.parquet \
  --epochs 10 \
  --output models/

# Or train a simpler LR model (faster, good baseline)
python -m openadserver.trainer.train_ctr \
  --model lr \
  --data data/training/train.parquet \
  --output models/

# Evaluate model
python -m openadserver.trainer.evaluate \
  --model models/deepfm_ctr.pt \
  --data data/training/test.parquet
# AUC: 0.72, LogLoss: 0.45
```

---

## 📊 Benchmarks

### Stress Test Results (Simulated 2 vCPU / 6GB)

Full pipeline tested: **Retrieval → Filter → Prediction → Ranking → Rerank**

> **Test Environment:** SQLite in-memory + FakeRedis (zero external dependencies).
> Results reflect core pipeline performance without network I/O overhead.

| Model | QPS | Avg Latency | P95 | P99 | Relative |
|-------|-----|-------------|-----|-----|----------|
| **LR** | 189.7 | 5.24ms | 7.64ms | 10.02ms | 100% (baseline) |
| **FM** | 166.1 | 5.99ms | 8.10ms | 11.54ms | 87.6% |
| **DeepFM** | 151.2 | 6.58ms | 10.30ms | 14.13ms | 79.7% |

### Pipeline Stage Breakdown (LR Model)

```
┌─────────────────┬───────────┬─────────────┐
│     Stage       │  Avg (ms) │  % of Total │
├─────────────────┼───────────┼─────────────┤
│ Retrieval       │   0.97    │    18.5%    │
│ Filter          │   0.20    │     3.8%    │
│ Prediction (ML) │   3.63    │    69.3%    │  ← Bottleneck
│ Ranking         │   0.35    │     6.7%    │
│ Rerank          │   0.10    │     1.9%    │
└─────────────────┴───────────┴─────────────┘
```

### Capacity Estimation (1M DAU)

| Model | Single Server QPS | Peak QPS Needed | Servers Required |
|-------|-------------------|-----------------|------------------|
| **LR** | ~190 | 870 | **5** |
| **FM** | ~166 | 870 | **6** |
| **DeepFM** | ~151 | 870 | **6** |

> **Note:** Calculation assumes 1M DAU × 15 requests/user/day = 15M daily → 174 avg QPS → 870 peak (5x factor).
> Production deployments with PostgreSQL + Redis may have ~10-20% additional I/O overhead.

---

## 🧪 Dataset & Model Training

### Criteo Click Logs Dataset

We use the [Criteo Display Advertising Challenge](https://www.kaggle.com/c/criteo-display-ad-challenge) dataset for CTR model training and evaluation.

**Dataset Characteristics:**
- **Size:** ~45GB (full), 100K samples used for benchmarks
- **Features:** 13 integer features (I1-I13), 26 categorical features (C1-C26)
- **Label:** Click (0/1)
- **Positive Rate:** ~3.4%

### Model Comparison (100K Criteo Samples)

| Model | Test AUC | Model Size | Description |
|-------|----------|------------|-------------|
| **LR** | 0.7577 | 0.49 MB | Logistic Regression — fastest, best AUC |
| **FM** | 0.7472 | 4.34 MB | Factorization Machine — captures feature interactions |
| **DeepFM** | 0.7178 | 8.77 MB | Deep FM — deep learning + FM combined |

> LR achieves highest AUC with fastest inference — recommended for production.

### Feature Engineering

- **Numba JIT acceleration** for feature hashing and encoding
- **Sparse features:** 26 categorical features (user, ad, context)
- **Dense features:** 13 numerical features (normalized)

### Run Stress Test

The stress test uses **SQLite in-memory** for campaign data and **FakeRedis** for frequency capping, enabling zero-dependency testing:

```bash
# Quick test (10 campaigns, 100 requests, no ML)
python scripts/criteo/stress_test.py --campaigns 10 --requests 100 --no-ml

# With ML model (LR recommended)
python scripts/criteo/stress_test.py --campaigns 200 --requests 10000 --model lr

# Compare all models
python scripts/criteo/compare_models.py
```

---

## 📁 Project Structure

```
openadserver/
├── ad_server/              # FastAPI application
│   ├── routers/            # API endpoints (ad, event, campaign)
│   ├── services/           # Business logic
│   └── middleware/         # Logging, metrics, auth
├── rec_engine/             # Recommendation engine
│   ├── retrieval/          # Candidate retrieval & targeting
│   ├── ranking/            # eCPM bidding & ranking
│   ├── filter/             # Budget, frequency, quality filters
│   └── reranking/          # Diversity & exploration
├── ml_engine/              # Machine learning
│   ├── models/             # DeepFM, LR, FM implementations
│   ├── features/           # Feature engineering pipeline
│   └── serving/            # Online prediction server
├── common/                 # Shared utilities
│   ├── config.py           # Configuration management
│   ├── database.py         # PostgreSQL connection
│   ├── cache.py            # Redis client
│   └── logger.py           # Structured logging
├── trainer/                # Model training
├── scripts/                # Utility scripts
├── configs/                # YAML configurations
├── deployment/             # Docker, K8s, Nginx
└── tests/                  # Test suite
```

---

## 🗺️ Roadmap

### ✅ v1.0 (Current)
- [x] Core ad serving API
- [x] eCPM-based ranking (CPM/CPC/CPA)
- [x] Targeting engine (geo, device, demographics)
- [x] DeepFM & LR CTR models
- [x] Event tracking (impression/click/conversion)
- [x] Docker Compose deployment
- [x] Prometheus + Grafana monitoring

### 🚧 v1.1 (Next)
- [ ] Admin dashboard UI (React)
- [ ] Campaign management API
- [ ] Audience segments
- [ ] A/B testing framework

### 🔮 v2.0 (Future)
- [ ] OpenRTB 2.5 support
- [ ] Header bidding
- [ ] Multi-tenant SaaS mode
- [ ] Kubernetes Helm charts
- [ ] Video ad support (VAST)

---

## 🆚 Why Not Just Use...

### Google Ad Manager?
- 💰 Takes 20-30% revenue share
- 🔒 Your data belongs to Google
- 🚫 Limited customization
- **OpenAdServer:** Keep 100% revenue, own your data

### Revive Adserver?
- 👴 Legacy PHP codebase
- 🐌 No ML capabilities
- 📊 Basic reporting only
- **OpenAdServer:** Modern Python, ML-powered, real eCPM

### Building from scratch?
- ⏰ 6-12 months development
- 💸 $100K+ engineering cost
- 🐛 Countless edge cases
- **OpenAdServer:** Production-ready in hours

---

## 🤝 Contributing

We love contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

```bash
# Setup development environment
make setup

# Run tests
make test

# Run linting
make lint

# Format code
make format
```

---

## 📄 License

Apache License 2.0 — See [LICENSE](LICENSE) for details.

Free for commercial use. No attribution required (but appreciated! 🙏)

---

## 💬 Community & Support

- 📖 [Documentation Wiki](https://github.com/pysean/openadserver/wiki)
- 💬 [GitHub Discussions](https://github.com/pysean/openadserver/discussions)
- 🐛 [Issue Tracker](https://github.com/pysean/openadserver/issues)
- 🐦 [Twitter @OpenAdServer](https://twitter.com/openadserver)

---

<p align="center">
  <sub>
    Built with ❤️ by engineers who've scaled ad systems to <b>100M+ daily requests</b><br>
    Extracted from production systems serving <b>billions of ad impressions</b>
  </sub>
</p>

<p align="center">
  <a href="https://github.com/pysean/openadserver">
    <img src="https://img.shields.io/github/stars/pysean/openadserver?style=for-the-badge&logo=github" alt="GitHub stars"/>
  </a>
</p>

<p align="center">
  <sub>
    <b>Keywords:</b> open source ad server, self-hosted ad platform, ad serving,
    programmatic advertising, CTR prediction, DeepFM, ad tech,
    digital advertising platform, ad network software, DSP, SSP,
    advertising API, ad management system, Python ad server
  </sub>
</p>
