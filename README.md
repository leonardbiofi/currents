# Currents

API server to store data colleted by sensor for the Currents Art Project. 


## Get Started

To run the server:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Write Data from Local Sensor

Ingest endpoint will write data to parquet file with a single file per day
| Purpose                  | URL                |
| ------------------------ | ------------------ |
| Ingest data from `local` | `POST /api/ingest` |
|                          |


## 🧪 Example API Usage

| Purpose                   | URL                                                             |
| ------------------------- | --------------------------------------------------------------- |
| Get all data from `local` | `GET /api/data/local`                                           |
| With date range           | `GET /api/data/local?start_date=2025-10-15&end_date=2025-10-17` |
| Latest point only         | `GET /api/data/local?latest=true`                               |
