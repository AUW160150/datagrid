# datagrid

**South Asian clinical data marketplace — hospital data to pharma research, end-to-end.**

[![Demo](https://img.shields.io/badge/Demo-YouTube-red?logo=youtube)](https://www.youtube.com/watch?v=aufWPcHjLrk)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Demo

[![datagrid demo](https://img.youtube.com/vi/aufWPcHjLrk/maxresdefault.jpg)](https://www.youtube.com/watch?v=aufWPcHjLrk)

> Watch the full pipeline — hospital submission → multi-agent harmonization → pharma matching — in action.

---

## What is datagrid?

datagrid connects South Asian hospitals to US pharma research buyers. Hospitals upload raw clinical records (Bengali, Hindi, English — PDF, CSV, VCF). A 5-agent pipeline automatically harmonizes them into OMOP CDM v5.4-compliant Parquet datasets. Pharma buyers browse, search, and purchase matched datasets through a marketplace UI.

**The problem:** South Asian populations are massively underrepresented in clinical research. Hospitals have the data but no way to package or monetize it. Pharma needs this data for trial design and pharmacogenomics research but can't access it.

**The solution:** datagrid handles the full stack — ingestion, standardization, compliance, and distribution.

---

## Architecture

```
Hospital Upload
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│                    datagrid Pipeline                     │
│                                                         │
│  Airbyte        Auth0         Ghost DB      OverClaw    │
│  Source    ─►   M2M      ─►   Ephemeral ─►  LLM        │
│  Connector      Tokens        Postgres      Tracing     │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │Ingestion │→ │Modality  │→ │Harmonize │             │
│  │  Agent   │  │  Agent   │  │  Agent   │             │
│  └──────────┘  └──────────┘  └──────────┘             │
│                               ┌──────────┐  ┌────────┐ │
│                               │Validation│→ │Output  │ │
│                               │  Agent   │  │ Agent  │ │
│                               └──────────┘  └────────┘ │
└─────────────────────────────────────────────────────────┘
      │
      ▼
OMOP CDM v5.4 Output
person.parquet · condition_occurrence.parquet
drug_exposure.parquet · measurement.parquet
      │
      ▼
Pharma Marketplace
```

---

## Sponsor Integrations

| Sponsor | Role |
|---------|------|
| **Ghost DB** | Spins up an ephemeral Postgres DB per pipeline run — stores cache + provenance — discarded on completion. Zero persistent storage risk for patient data. |
| **Auth0** | Issues a scoped M2M token to each agent before execution. Harmonization-agent can write; validation-agent can only read. No agent can exceed its permissions. |
| **Airbyte** | Custom Python CDK source connector reads patient records (TXT, CSV, VCF) from hospital directories and emits structured RECORD streams. |
| **OverClaw** | Wraps every LLM call with a trace so Overmind can automatically optimize harmonization and validation prompts over time. |

---

## Agent Pipeline

### 1 · Ingestion Agent
Reads raw patient files via the Airbyte source connector. Handles `.txt` clinical notes (Bengali/Hindi/English), `.csv` lab results, and `.vcf` genomic variant files. Groups them by patient ID.

### 2 · Missing Modality Agent
Scores each patient record for data completeness across three modalities (clinical note, lab results, genomic variants). Flags gaps and infers compensating evidence from present data before harmonization.

### 3 · Harmonization Agent
Maps all clinical entities to ICD-10 codes and OMOP concept IDs using `llama-3.3-70b` via Groq. Handles multilingual extraction (Bengali, Hindi, English mixed). Assigns confidence scores and flags uncertain mappings.

### 4 · Validation Agent
Second-pass review on all entities with confidence < 0.85 or any flag set. Independently confirms, corrects, or escalates each mapping. Produces a full audit trail.

### 5 · Output Agent
Writes OMOP CDM v5.4 Parquet tables (`person`, `condition_occurrence`, `drug_exposure`, `measurement`) plus per-patient provenance JSON to Ghost DB and local output.

---

## Performance

| Metric | Result |
|--------|--------|
| ICD-10 mapping accuracy | 94.4% |
| Validation improvement | +12.3% on low-confidence entities |
| Languages | Bengali · Hindi · English (mixed) |
| Formats | TXT · CSV · VCF |
| Output standard | OMOP CDM v5.4 |
| Validated against | KDIGO 2022 · WHO ICD-10 2023 · OMOP Athena |

---

## Quickstart

### Prerequisites
- Python 3.12
- [Groq API key](https://console.groq.com) (free tier)
- [Ghost CLI](https://ghost.build) — `curl -fsSL https://install.ghost.build | sh`
- Auth0 account (free tier)

### Setup

```bash
git clone https://github.com/AUW160150/datagrid
cd datagrid
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your keys
```

### Environment variables

```bash
GROQ_API_KEY=             # Groq free-tier LLM inference
GHOST_TOKEN=              # ghost.build CLI token (ghost login)
AUTH0_DOMAIN=             # e.g. dev-xxx.us.auth0.com
AUTH0_CLIENT_ID=          # M2M application client ID
AUTH0_CLIENT_SECRET=      # M2M application client secret
AUTH0_AUDIENCE=           # e.g. https://dev-xxx.us.auth0.com/api/v2/
OVERCLAW_API_KEY=         # console.overmindlab.ai (optional — falls back to direct Groq)
```

### Run the pipeline

```bash
python run_pipeline.py
```

### Run the API + frontend

```bash
# Terminal 1 — API
uvicorn api:app --port 8001

# Terminal 2 — Frontend
cd frontend && python3 -m http.server 8080

# Open browser
open http://localhost:8080/screen0_landing.html
```

---

## Frontend Screens

| Screen | Description |
|--------|-------------|
| `screen0_landing.html` | Role picker — Hospital or Pharma |
| `screen1a_hospital.html` | Hospital data submission |
| `screen2_pipeline.html` | Live pipeline with sponsor dashboards |
| `screen1b_pharma.html` | Pharma dataset browser + Hospital → US Pharma recommendations |
| `screen3_results.html` | OMOP output results |
| `screen4_earnings.html` | Hospital earnings dashboard |

---

## Project Structure

```
datagrid/
├── agents/
│   ├── ingestion_agent.py       # Airbyte connector → patient records
│   ├── modality_agent.py        # completeness scoring + gap detection
│   ├── harmonization_agent.py   # ICD-10 / OMOP mapping (OverClaw)
│   ├── validation_agent.py      # second-pass validation
│   └── output_agent.py          # OMOP Parquet + Ghost DB write
├── auth/
│   └── m2m.py                   # Auth0 M2M token manager
├── connectors/
│   └── airbyte_source.py        # Airbyte Python CDK source connector
├── db/
│   └── ghost_client.py          # Ghost DB fork/connect/discard
├── frontend/                    # React single-page screens
├── harmonization/
│   └── omop_reference.py        # OMOP concept ID reference table
├── pipeline/
│   └── orchestrator.py          # pipeline coordinator
├── data/synthetic/              # 10 synthetic South Asian patients
├── output/                      # OMOP Parquet tables + provenance
├── api.py                       # FastAPI backend
└── run_pipeline.py              # CLI entry point
```

---

## Data

Includes 10 synthetic South Asian patients with:
- Clinical notes in Bengali and Hindi (mixed with English medical terms)
- Lab results (glucose, HbA1c, lipids, renal panel, CBC)
- Genomic VCF files (TCF7L2, PCSK9, APOE4, rs IDs)
- Diagnoses: T2DM, hypertension, dyslipidemia, CKD, CVD

All data is fully synthetic — no real patient information.

---

## Built at

Hackathon project · 2025
**Author:** Mahtabin Rodela — [@AUW160150](https://github.com/AUW160150)

---

## References

- [OMOP CDM v5.4](https://ohdsi.github.io/CommonDataModel/)
- [WHO ICD-10 2023](https://www.who.int/standards/classifications/classification-of-diseases)
- [KDIGO 2022 Clinical Guidelines](https://kdigo.org/guidelines/)
- [Airbyte Source Protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol)
- [Ghost agent-native database](https://ghost.build)
- [Auth0 M2M](https://auth0.com/docs/get-started/authentication-and-authorization-flow/client-credentials-flow)
- [OverClaw / Overmind](https://overmindlab.ai)
- [Groq](https://console.groq.com)
