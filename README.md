# Prices Pipeline

## Introduction

POC to test `dlt` to create a ELT pipeline to load prices from EODHistoricalData.com to a duckdb database.

## Secrets

Secrets are stored in .dlt/secrets.toml

```toml
[sources.eod_source]
eodhd_api_key = "your_eodhd_api_key"
```