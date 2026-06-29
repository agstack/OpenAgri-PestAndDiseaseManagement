# Pest and Disease Management

:eu: *"This service was created in the context of OpenAgri project (https://horizon-openagri.eu/). OpenAgri has received funding from the EU’s Horizon Europe research and innovation programme under Grant Agreement no. 101134083."*
# Description

The Pest and Disease Management(P&DM) service supports farmers by providing multiple ways of calculating a risk associated with
diseases, Growing Degree Days for pests and other mechanisms.

The service now includes a **fuzzy risk engine** (Mamdani fuzzy inference) that replaces the previous rule-engine approach. Crops and their associated threat models (pests/diseases) are managed via dedicated APIs, and risk scores (0–100) with classifications (Low / Moderate / High / Critical) are returned in the OpenAGRI JSON-LD format.

# Requirements

<ul>
    <li>git</li>
    <li>docker</li>
    <li>docker-compose</li>
</ul>

Docker version used during development: 27.0.3

# Installation

There are two ways to install this service, via docker or directly from source.

<h3> Deploying from source </h3>

When deploying from source, use python 3:11.\
Also, you should use a [venv](https://peps.python.org/pep-0405/) when doing this.

A list of libraries that are required for this service is present in the "requirements.txt" file.\
This service uses FastAPI as a web framework to serve APIs, alembic for database migrations sqlalchemy for database ORM mapping.

<h3> Deploying via docker </h3>

After installing <code> docker </code> you can run the following commands to run the application:

```
docker compose build
docker compose up
```

The application will be served on http://127.0.0.1:{SERVICE_PORT}, where SERVICE_PORT is the value read from the .env (default 8003)

# Documentation
You can find working examples for GDD and Risk Index calculation in the following pages:

- [GDD](scripts/gdd.md)
- [Risk Index](scripts/riskindex.md)

A list of APIs can be viewed in the [API.md](https://github.com/openagri-eu/pest-and-disease-management/blob/main/API.md) file, and a full list of APIs can be viewed [here](https://editor.swagger.io/?url=https://gist.githubusercontent.com/vlf-stefan-drobic/71d21b192db0b968278a48d6e5e6d9cb/raw/70bf0e1f05163ef0cf63d5b3950711e98a43c865/gistfile1.txt).

The basic flow for this service is as follows:
1. The user registers and/or logs in;
2. The user creates their parcel/s (historical weather data is seeded automatically on creation)
3. The user creates one or more pest and/or disease models
4. The user queries the system for either risk index or growing degree days (GDD) for pest and disease models respectively

**Fuzzy risk flow (new):**
1. Crops and threat models (with fuzzy rules and biological parameters) are pre-seeded from the reference dataset on first migration
2. The user creates a parcel — one year of historical weather data is fetched automatically from OpenMeteo
3. The user calls `/api/v1/fuzzy-risk/calculate/` with a parcel ID and date range for historical risk, or `/api/v1/fuzzy-risk/forecast/` for a forward-looking risk over the coming days
4. Responses follow the OpenAGRI JSON-LD format with a 0–100 risk score and a risk class per pest per day

New API prefixes:

| Prefix | Purpose |
|--------|---------|
| `/api/v1/crop/` | Manage crop records |
| `/api/v1/threat-model/` | Manage per-pest fuzzy rule sets; bulk import from Excel or JSON |
| `/api/v1/fuzzy-risk/` | Calculate historical or forecast pest/disease risk |

---

# Fuzzy Risk Engine

## How It Works

The engine is a **Mamdani fuzzy inference system** implemented in `app/utils/fuzzy_risk.py`. For each (pest/disease, day) pair it produces a continuous **risk score 0–100** and a discrete **risk class**.

### 1. Feature Engineering (`compute_features`)

Raw daily weather (columns: `date`, `temp_max`, `temp_min`, `humidity`, `rainfall`) is enriched before any rules fire:

| Derived feature | Description |
|----------------|-------------|
| `temp_avg` | `(temp_max + temp_min) / 2` |
| `temp_avg_Xd`, `humidity_Xd`, `rainfall_Xd` | Rolling means/sums over 3, 7, 14 days |
| `wetness_h` | Estimated leaf-wetness hours per day (from RH + rainfall thresholds) |
| `wetness_3d` | 3-day cumulative leaf-wetness hours |
| `vpd`, `vpd_3d`, `vpd_7d` | Vapour pressure deficit and rolling averages |
| `streak_humXX` | Consecutive days with humidity ≥ XX% (thresholds: 60, 70, 80, 85, 90, 95) |
| `gdd_daily_Xb`, `gdd_cum_Xb` | Daily and cumulative Growing Degree Days at base temperatures 0, 5, 10°C (plus any pest-specific base) |
| `gdd_cum_pheno` | Phenological GDD (base 5°C) used for seasonal gating |
| `gdd_annual_ref_Xb` | Annual GDD reference (mean of complete seasons, or extrapolated) |

### 2. Biological Gating

Before fuzzy rules are evaluated, two hard gates are checked:

**Lethal temperature gate** — if `temp_avg` is outside `[t_lethal_min, t_lethal_max]`, score is forced to 0 regardless of rules.

**Phenological gate** — if the pest has `pheno_frac_lo`/`pheno_frac_hi` (fraction of annual GDD) or absolute `pheno_lo`/`pheno_hi` (GDD units), a trapezoidal membership function (`mu_pheno`) is computed. When `mu_pheno = 0` the pest is out of season and score is 0.

### 3. Membership Functions

Three trapezoidal membership functions fuzzify each weather input:

- **`membership_temp(t, lo, hi)`** — gradual ramp over a window proportional to the range width (min transition ±1.5°C)
- **`membership_humidity(h, lo, hi)`** — same approach; if `lo=0, hi=100` returns 1.0 (any humidity matches)
- **`membership_rainfall(r_3d, rain_min)`** — ramp centred around `rain_min` with 3-day cumulative rainfall; if `rain_min=0` returns 1.0

Fungi/bacteria (genus matched against a pathogen keyword list) use **7-day moving averages** for temperature and humidity rather than the daily value, making them smoother and less reactive to single-day spikes.

### 4. Mamdani Inference

For each fuzzy rule in the threat model:

```
mu = min(mu_temp, mu_humidity, mu_rainfall)   # AND aggregation
```

Rules with `mu > FUZZY_MIN_MU` (0.01) are considered active. The defuzzified score is a **weighted mean** of the rule risk scores:

```
score = Σ(mu_i × risk_score_i) / Σ(mu_i)
```

Risk scores per label: `low=10`, `moderate=40`, `high=80`, `critical=100`.

### 5. Post-processing Modifiers

**Humidity streak penalty** — if consecutive wet days < `bio_params.min_streak`, the score is scaled down proportionally.

**Leaf-wetness gate (Mills-type)** — if `bio_params.min_wetness_hours_critical` or `min_wetness_hours_high` are set and the daily wetness hours are below threshold, the score is capped below the corresponding risk class boundary.

**Phenological scaling** — final score is multiplied by `mu_pheno` (0–1), tapering risk at the edges of the active season.

### 6. Risk Classification

| Score range | Class |
|-------------|-------|
| ≥ 75 | Critical |
| ≥ 50 | High |
| ≥ 25 | Moderate |
| < 25 | Low |

---

## ThreatModel Data Structure

Each threat model stored in the `threat_model` table has a `definition` JSONB field with two top-level keys:

```json
{
  "bio_params": {
    "t_base": 5.0,
    "t_lethal_min": -5.0,
    "t_lethal_max": 40.0,
    "min_streak": 3,
    "min_wetness_hours_critical": 10.0,
    "min_wetness_hours_high": 6.0,
    "pheno_frac_lo": 0.1,
    "pheno_frac_hi": 0.9
  },
  "fuzzy_rules": [
    {
      "hum_lo": 70,
      "hum_hi": 100,
      "temp_lo": 10,
      "temp_hi": 25,
      "rain_min": 2.0,
      "risk_level": "high"
    },
    {
      "hum_lo": 60,
      "hum_hi": 100,
      "temp_lo": 5,
      "temp_hi": 30,
      "rain_min": 0.0,
      "risk_level": "moderate"
    }
  ]
}
```

**`bio_params` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `t_base` | float | GDD base temperature (default 5.0°C) |
| `t_lethal_min` | float | Temperature below which organism cannot survive |
| `t_lethal_max` | float | Temperature above which organism cannot survive |
| `min_streak` | int | Minimum consecutive favourable days before full score |
| `min_wetness_hours_critical` | float | Leaf-wetness hours required for Critical class |
| `min_wetness_hours_high` | float | Leaf-wetness hours required for High class |
| `pheno_frac_lo` / `pheno_frac_hi` | float | Season window as fraction of annual GDD (0–1) |
| `pheno_lo` / `pheno_hi` | float | Season window as absolute GDD units (alternative to fractions) |

**`fuzzy_rules` fields:**

| Field | Type | Description |
|-------|------|-------------|
| `hum_lo` / `hum_hi` | float | Humidity range (%) that activates the rule |
| `temp_lo` / `temp_hi` | float | Temperature range (°C) that activates the rule |
| `rain_min` | float | Minimum 3-day cumulative rainfall (mm) required; 0 = no requirement |
| `risk_level` | enum | `low` / `moderate` / `high` / `critical` |

---

## API Reference

All endpoints require a valid JWT (`Authorization: Bearer <token>`).

### Crop endpoints — `/api/v1/crop/`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/crop/` | List all crops |
| `POST` | `/api/v1/crop/` | Create a crop |
| `DELETE` | `/api/v1/crop/{id}/` | Delete a crop (cascades to threat models) |

### Threat Model endpoints — `/api/v1/threat-model/`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/threat-model/` | List all threat models |
| `GET` | `/api/v1/threat-model/{id}/` | Get a single threat model |
| `POST` | `/api/v1/threat-model/` | Create a threat model |
| `PUT` | `/api/v1/threat-model/{id}/` | Update a threat model |
| `DELETE` | `/api/v1/threat-model/{id}/` | Delete a threat model |
| `POST` | `/api/v1/threat-model/import-excel/` | Bulk import from `.xlsx` |
| `POST` | `/api/v1/threat-model/import-json/` | Bulk import from `.json` (supports JSONC comments) |

### Fuzzy Risk endpoints — `/api/v1/fuzzy-risk/`

#### `POST /api/v1/fuzzy-risk/calculate/`

Calculates risk from **weather data already stored in the database** for a parcel.

```json
{
  "parcel_id": 1,
  "from_date": "2024-06-01",
  "to_date": "2024-06-30",
  "threat_model_ids": ["uuid-1", "uuid-2"]
}
```

- `threat_model_ids` is optional — omit to run all threat models.
- Returns 404 if no weather rows exist for the parcel and date range.
- Query param `?format=json-ld` (default) or `?format=json` controls the response shape.

#### `POST /api/v1/fuzzy-risk/forecast/`

Calculates risk from a **live OpenMeteo forecast** for a parcel's location.

```json
{
  "parcel_id": 1,
  "days_ahead": 7,
  "threat_model_ids": null
}
```

- `days_ahead` defaults to 7; clamped to the configured min/max forecast window.
- Weather data is fetched live and not stored.
- Query param `?format=json-ld` (default) or `?format=json` controls the response shape.

#### `POST /api/v1/fuzzy-risk/historical/`

Fetches historical weather from the **OpenMeteo archive API**, stores new rows (deduped), then calculates risk.

```json
{
  "parcel_id": 1,
  "from_date": "2024-01-01",
  "to_date": "2024-12-31",
  "threat_model_ids": null
}
```

- Useful for backfilling parcels that have no stored weather data.
- Query param `?format=json-ld` (default) or `?format=json` controls the response shape.

#### `POST /api/v1/fuzzy-risk/forecast-fetch/`

Fetches weather from the **OpenMeteo forecast API** for a specific date range, stores new rows (deduped), then calculates risk.

```json
{
  "parcel_id": 1,
  "from_date": "2025-04-28",
  "to_date": "2025-05-05",
  "threat_model_ids": null
}
```

Same semantics as `historical/` but uses the forecast API.
Query param `?format=json-ld` (default) or `?format=json` controls the response shape.

### Response Format

All endpoints support two response shapes, selected via the `?format=` query parameter.

**`?format=json-ld`** (default) — OpenAGRI JSON-LD envelope:

```json
{
  "@context": { ... },
  "@graph": [
    {
      "@type": ["ObservationCollection"],
      "description": "Fuzzy risk for Venturia inaequalis (Apple scab)",
      "observedProperty": {
        "name": "Venturia inaequalis",
        "commonName": "Apple scab"
      },
      "hasMember": [
        {
          "@type": ["Observation", "PestInfestationRisk"],
          "phenomenonTime": "2024-06-01",
          "hasSimpleResult": "62.3",
          "riskClass": "High",
          "meta": "best_rule=High mu=0.78 mu_pheno=1.00 streak=4d wh=11h"
        }
      ]
    }
  ]
}
```

**`?format=json`** — flat array of records, one entry per (pest, day) pair:

```json
[
  {
    "date": "2024-06-01",
    "scientific_name": "Venturia inaequalis",
    "common_name": "Apple scab",
    "risk_score": 62.3,
    "risk_class": "High",
    "detail": "best_rule=High mu=0.78 mu_pheno=1.00 streak=4d wh=11h"
  }
]
```

The `meta` / `detail` field contains debug information: best-matched rule, fuzzy activation degree (`mu`), phenological scaling factor (`mu_pheno`), consecutive wet-day streak, and leaf-wetness hours.

# Contribution
Please contact the maintainer of this repository.

The fuzzy risk engine (biological parameters, fuzzy rule sets, and domain expertise on pest/disease modelling) was developed in collaboration with **Agrobit S.r.l.**, whose agronomic knowledge underpins the threat model definitions used in this service.

# License
This project code is licensed under the Apache License 2.0, see the [LICENSE](LICENSE) file for more details.
