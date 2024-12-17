# Moo

Moo is a tool for collecting and processing data from the CowSwap protocol.

## ETL

API → Polars → Postgres

- Data is collected from the The Graph API
- Then transformed, validated, and normalized into a dataframe
- Finally, loaded into Postgres

### Running the data pipeline
- Environment variables:
```bash
cp .env.example .env
```
Edit the `.env` file with your credentials

- Pipeline config:
Edit the `config/pipeline_config.yaml` file

- Run the pipeline:

```bash
python src/moo/run.py
```

### Running the analytics

