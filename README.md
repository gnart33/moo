# MOO - CowSwap Data Pipeline

A data pipeline for extracting, transforming, and loading CowSwap DEX trading data.

## Architecture
Extract → Transform → Load
↓ ↓ ↓
TheGraph → Polars → PostgreSQL


### Components

- **Extract**: Fetches swap data from CowSwap's TheGraph subgraph
- **Transform**: Processes raw data using Polars with validations
- **Load**: Stores transformed data in PostgreSQL with versioning support

## Key Features

- Incremental data loading
- Data validation and quality checks
- Schema versioning
- Audit trail tracking
- Configurable pipeline parameters

## Setup

1. Create `.env` file and set the environment variables
```bash
cp .env.example .env
```

2. Configure pipeline in `pipeline_config.yaml`:
```yaml
extract:
  subgraph_base_url: "https://gateway.thegraph.com/api"
  cowswap_subgraph_id: "..."
  raw_data_dir: "data/raw"

transform:
  decimal_places: 18
  timestamp_format: "%Y-%m-%d %H:%M:%S"

load:
  batch_size: 5000
  timeout: 300
  table_name: "swaps"
  if_exists: "append"
```

3. Run the pipeline
- Set up docker compose for postgres
```bash
docker compose up -d
```
- Set up python environment with poetry, after cloning the repo and in the root directory
```bash
poetry install
```
- Or install using pip git repo
```bash
pip install git+https://github.com/gnart33/moo.git
```

- Orchestrate the pipeline
```bash
poetry run python src/moo/pipeline/orchestrator.py
```
## Data Model

### Swaps Table
- blockTimestamp
- transactionHash
- tokenAmountIn
- tokenAmountOut
- tokenIn
- tokenInSymbol
- tokenOutSymbol
- tokenOut
