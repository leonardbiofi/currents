from fastapi import FastAPI, Path, HTTPException, Query
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import os
import duckdb
import glob

from typing import Optional

app = FastAPI()


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

# Base Data dir
BASE_DATA_DIR = os.path.join(SCRIPT_DIR, "data")
os.makedirs(BASE_DATA_DIR, exist_ok=True)

# Local sensor
LOCAL_DATA_DIR = os.path.join(BASE_DATA_DIR, "local")
os.makedirs(LOCAL_DATA_DIR, exist_ok=True)


class SensorData(BaseModel):
    co2: int
    temperature: float
    humidity: float
    timestamp: str  # ISO8601 string


def get_parquet_filename(date: datetime) -> str:
    return os.path.join(BASE_DATA_DIR, "local", f"{date.strftime('%Y-%m-%d')}.parquet")


@app.post("/api/ingest")
async def receive_sensor_data(data: SensorData):
    try:
        # Parse timestamp to datetime object
        dt = datetime.fromisoformat(data.timestamp.replace("Z", "+00:00"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid timestamp format")

    # Prepare dataframe row
    df = pd.DataFrame(
        [
            {
                "co2": data.co2,
                "temperature": data.temperature,
                "humidity": data.humidity,
                "timestamp": dt,
            }
        ]
    )

    file_path = get_parquet_filename(dt)

    # Append or create parquet file
    if os.path.exists(file_path):
        # Load existing data and append
        existing_df = pd.read_parquet(file_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_parquet(file_path, index=False)
    else:
        df.to_parquet(file_path, index=False)

    return {"status": "success", "file": file_path}


@app.get("/data/{source}")
def get_data(
    source: str = Path(
        ..., description="Subfolder under /data, e.g., lab1 or sensor01"
    ),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    latest: Optional[bool] = Query(False, description="Get latest data point only"),
):
    try:
        # Path to the source subfolder
        source_dir = os.path.join(BASE_DATA_DIR, source)
        if not os.path.isdir(source_dir):
            raise HTTPException(
                status_code=404, detail=f"Source folder '{source}' not found."
            )

        # Get list of .parquet files
        parquet_files = sorted(glob.glob(os.path.join(source_dir, "*.parquet")))
        if not parquet_files:
            return {"data": [], "message": f"No data files found in {source_dir}"}

        # Filter files by start/end date (using filenames like YYYY-MM-DD.parquet)
        if start_date or end_date:
            start = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
            end = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

            parquet_files = [
                f
                for f in parquet_files
                if (not start or os.path.basename(f) >= f"{start:%Y-%m-%d}.parquet")
                and (not end or os.path.basename(f) <= f"{end:%Y-%m-%d}.parquet")
            ]

        if not parquet_files:
            return {"data": [], "message": "No matching data in given date range."}

        # DuckDB query
        files_sql = ", ".join(f"'{f}'" for f in parquet_files)
        query = f"""
            SELECT * FROM read_parquet([{files_sql}])
            {"ORDER BY timestamp DESC LIMIT 1" if latest else "ORDER BY timestamp ASC"}
        """

        con = duckdb.connect()
        result = con.execute(query).fetchdf()
        return result.to_dict(orient="records")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
