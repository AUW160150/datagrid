# clingrid

**Connecting LMIC hospitals to pharmaceutical research buyers.**

Hospitals upload raw clinical records in Bengali, Hindi, or English. A 5-agent AI pipeline automatically harmonizes them into OMOP CDM v5.4-compliant Parquet datasets. Pharma buyers browse, search, and purchase matched datasets through a marketplace UI — with a queryable audit trail retained for every harmonization decision.

---

## The Problem

South Asian populations are massively underrepresented in clinical research. LMIC hospitals hold valuable patient data but have no infrastructure to standardize, package, or monetize it. Pharma buyers can't access it at scale. clingrid closes this gap.

---

## Sponsor Tracks

### GMI Cloud + Kimi K2.5
The inference backbone for all 5 agents. Kimi K2.5's native multimodal and multilingual capabilities handle Bengali/Hindi/English mixed clinical text without a separate translation layer — critical for South Asian hospital records. GMI's GPU infrastructure keeps the full harmonization pipeline under 60 seconds per patient record.

### Dify
Orchestrates the entire 5-agent workflow (ingest → modality gap detection → harmonization → validation → output) as a visual, observable pipeline. Replaces custom orchestration code with built-in retry logic, LLM call tracing, and RAG pipeline support for concept lookups.

### HydraDB
Persists provenance, confidence scores, and OMOP outputs across pipeline sessions. Unlike ephemeral per-run databases, HydraDB gives pharma buyers a queryable audit trail: every harmonization decision, every flagged mapping, every confidence score — retained and attributable across hospital submissions.

### Photon
Surfaces the pharma marketplace and hospital submission UI into interfaces buyers and hospitals already use, removing the adoption friction of a standalone web app.

---

## Pipeline Architecture

```
Hospital Upload (Bengali / Hindi / English)
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│                    Dify Workflow                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐  │
│  │ Ingest   │→ │ Modality │→ │ Harmonization Agent  │  │
│  │ Agent    │  │ Gap      │  │ Kimi K2.5 (GMI Cloud)│  │
│  │ (Photon) │  │ Detect   │  │ ICD-10 + OMOP CDM    │  │
│  └──────────┘  └──────────┘  └──────────────────────┘  │
│                                          │               │
│  ┌──────────────────────┐  ┌─────────────────────────┐  │
│  │ Output Agent         │← │ Validation Agent        │  │
│  │ HydraDB provenance   │  │ Kimi K2.5 (GMI Cloud)   │  │
│  │ Parquet OMOP tables  │  │ KDIGO 2022 · ICD-10 2024│  │
│  └──────────────────────┘  └─────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
         │
         ▼
HydraDB — persistent audit trail (confidence scores, flags, mappings)
         │
         ▼
Pharma Marketplace — OMOP Parquet datasets, queryable provenance
```

**Output tables:** `person` · `condition_occurrence` · `drug_exposure` · `measurement`
**Standard:** OMOP CDM v5.4 · ICD-10-CM 2024 · KDIGO 2022 · GA4GH DUO

---

## Project Structure

```
clingrid/
├── api.py                          # FastAPI backend
├── agents/
│   ├── harmonization_agent.py      # GMI Cloud Kimi K2.5 — ICD-10/OMOP mapping
│   └── validation_agent.py         # GMI Cloud Kimi K2.5 — second-pass review
├── db/
│   └── hydradb_client.py           # HydraDB — persistent provenance store
├── workflow/
│   ├── dify_client.py              # Dify streaming workflow client
│   └── DIFY_SETUP.md               # Step-by-step Dify workflow build guide
├── skills/
│   └── photon_skills.py            # Photon agent skill definitions
├── auth/
│   └── m2m.py                      # Auth0 M2M scoped agent tokens
├── frontend/
│   ├── screen0_landing.html        # Role picker — Hospital / Pharma
│   ├── screen1a_hospital.html      # Hospital submission dashboard
│   ├── screen1b_pharma.html        # Pharma dataset marketplace + search
│   ├── screen2_pipeline.html       # Live 5-agent pipeline visualization
│   ├── screen3_results.html        # OMOP output + buyer recommendations
│   └── screen4_earnings.html       # Hospital royalty dashboard
├── .env.example
└── requirements.txt
```

---

## Running the Demo

**Frontend only (no backend needed):**
```bash
cd frontend
python3 -m http.server 3000
# Open http://localhost:3000/screen0_landing.html
```

The pipeline screen (`screen2`) runs a full animated demo showing all 5 sponsor integrations in real time — no API keys required.

**With live backend:**
```bash
cp .env.example .env
# Fill in HYDRADB_API_KEY, GMI_API_KEY, DIFY_API_KEY
pip install -r requirements.txt
uvicorn api:app --port 8001 --reload
```

---

## Environment Variables

```bash
# GMI Cloud — Kimi K2.5 inference
GMI_API_KEY=<your_jwt_token>
GMI_BASE_URL=https://api.gmi-serving.com/v1
GMI_MODEL=moonshotai/Kimi-K2-Instruct

# Dify — workflow orchestration
DIFY_API_KEY=<your_app_key>
DIFY_BASE_URL=https://api.dify.ai/v1

# HydraDB — persistent provenance
HYDRADB_API_KEY=<your_api_key>

# Photon — UI surfacing + notifications
PHOTON_API_KEY=<your_api_key>

# Auth0 — agent M2M tokens
AUTH0_DOMAIN=<your_tenant>.us.auth0.com
AUTH0_CLIENT_ID=<client_id>
AUTH0_CLIENT_SECRET=<client_secret>
```

---

## Dify Workflow Setup

See [`workflow/DIFY_SETUP.md`](workflow/DIFY_SETUP.md) for the complete guide to building the 7-node clinical harmonization workflow in the Dify UI, including how to add GMI Cloud as an OpenAI-compatible model provider.

---

## What the Demo Shows

**screen0** — Landing: Hospital vs Pharma role picker with animated genomic/molecular SVGs and sponsor badges.

**screen1a** — Hospital dashboard: drag-and-drop clinical file upload (TXT/CSV/VCF), data usage terms, submit to pipeline.

**screen1b** — Pharma marketplace: natural language dataset search (powered by HydraDB), OMOP compliance scores, pricing, dataset cards.

**screen2** — Pipeline visualization (the main demo screen):
- Left: 7 agent cards updating live — each shows its active sponsor skill
- Center: streaming log with per-sponsor attribution (`[Kimi K2.5]`, `[HydraDB]`, `[Dify]`, `[Photon]`)
- Right: 5 live sponsor dashboards animating in real time

| Sponsor panel | What it shows |
|--------------|---------------|
| **HydraDB** | Session active · `persisting` · audit trail retained |
| **Auth0** | Token counter 0→5 as each agent starts |
| **Photon** | Record counter 0→10 · zero-friction UI |
| **Dify** | Node counter 0→7 · retry + tracing |
| **GMI Cloud** | Inference call counter 0→10 · Kimi K2.5 |

**screen3** — Results: OMOP completeness score, buyer matches, export Parquet + provenance.

**screen4** — Hospital earnings: royalty dashboard, transaction ledger, payout schedule.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Inference | GMI Cloud — Kimi K2.5 (native multilingual) |
| Orchestration | Dify — 7-node visual workflow |
| Provenance DB | HydraDB — persistent audit trail |
| UI surfacing | Photon — zero-friction interfaces |
| Auth | Auth0 M2M — scoped per agent |
| Data standard | OMOP CDM v5.4 · ICD-10-CM 2024 |
| Output format | Apache Parquet |
| Backend | FastAPI + Python 3.12 |
| Frontend | React 18 (CDN) · DM Sans · no build step |

---

*Built for the clingrid hackathon sponsor track — GMI Cloud · Dify · HydraDB · Photon*
