import logging
import json
from pathlib import Path
import time
from datetime import datetime, timezone, timedelta
import psycopg2
import schedule

import config
import aqi
from loader import BigQueryLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

STATE_FILE = Path(config.INGESTION_STATE_FILE)


def _floor_to_hour(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _read_last_window_end() -> datetime | None:
    if not STATE_FILE.exists():
        return None

    try:
        state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        raw_value = state.get("last_window_end") or state.get("last_ingested_timestamp")
        if not raw_value:
            return None
        parsed = datetime.fromisoformat(raw_value)
        return _floor_to_hour(parsed)
    except Exception as e:
        log.warning("Failed to read ingestion state file '%s': %s", STATE_FILE, e)
        return None


def _write_last_window_end(window_end: datetime) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {"last_window_end": _floor_to_hour(window_end).isoformat()}
    STATE_FILE.write_text(json.dumps(payload), encoding="utf-8")


def _discover_initial_window_start() -> datetime | None:
    min_timestamps: list[datetime] = []
    for city in config.CITIES:
        db_name = f"{city.lower()}_db"
        try:
            conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                dbname=db_name,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
            )
            cursor = conn.cursor()
            cursor.execute('SELECT MIN("timestamp") FROM readings')
            min_ts = cursor.fetchone()[0]
            if min_ts is not None:
                min_timestamps.append(min_ts)
            cursor.close()
            conn.close()
        except Exception as e:
            log.error("Failed discovering initial timestamp for city %s: %s", city, e)

    if not min_timestamps:
        return None
    return _floor_to_hour(min(min_timestamps))

def fetch_and_load(bq_loader):
    log.info("Starting scheduled ingestion from TimescaleDB to BigQuery...")

    now = datetime.now(timezone.utc)
    end_time = _floor_to_hour(now)

    last_window_end = _read_last_window_end()
    if last_window_end is None:
        start_time = _discover_initial_window_start()
        if start_time is None:
            log.info("No source data found in TimescaleDB; skipping this cycle.")
            return
    else:
        # Watermark stores the latest ingested bucket timestamp, so start
        # from the next hour to avoid re-ingesting the same bucket.
        start_time = last_window_end + timedelta(hours=1)

    if start_time >= end_time:
        log.info("No new complete hourly window yet. last_window_end=%s current_end=%s", start_time, end_time)
        return
    
    log.info("Fetching data for time window: %s to %s", start_time, end_time)
    
    all_rows = []
    had_errors = False
    latest_ingested_ts = None
    
    for city in config.CITIES:
        db_name = f"{city.lower()}_db"
        try:
            conn = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                dbname=db_name,
                user=config.DB_USER,
                password=config.DB_PASSWORD
            )
            cursor = conn.cursor()
            
            query = """
            SELECT 
                city,
                hotspot,
                time_bucket('1 hour', "timestamp") AS ts,

                AVG(pm25) AS pm25,
                AVG(pm10) AS pm10,
                AVG(no) AS no,
                AVG(no2) AS no2,
                AVG(nox) AS nox,
                AVG(nh3) AS nh3,
                AVG(so2) AS so2,
                AVG(co) AS co,
                AVG(ozone) AS ozone,

                AVG(benzene) AS benzene,
                AVG(toluene) AS toluene,
                AVG(xylene) AS xylene,
                AVG("oxylene") AS oxylene,
                AVG("ethbenzene") AS ethbenzene,
                AVG("mpxylene") AS mpxylene,

                AVG(temperature) AS temperature,
                AVG(humidity) AS humidity,
                AVG(windspeed) AS windspeed,
                AVG(winddirection) AS winddirection,

                AVG(rainfall) AS rainfall,
                AVG(totalrainfall) AS totalrainfall,
                AVG(solarradiation) AS solarradiation,
                AVG(pressure) AS pressure,
                AVG(verticalwindspeed) AS verticalwindspeed,

                MAX(latitude) AS latitude,
                MAX(longitude) AS longitude

            FROM readings
            WHERE "timestamp" >= %s 
            AND "timestamp" < %s

            GROUP BY city, hotspot, ts
            ORDER BY ts;
            """
            
            cursor.execute(query, (start_time, end_time))
            rows = cursor.fetchall()
            
            columns = [desc[0] for desc in cursor.description]
            
            for row in rows:
                raw_record = dict(zip(columns, row))
                
                # Note: Postgres returns unquoted column aliases in lowercase.
                bq_row = {
                    "city": raw_record.get("city"),
                    "hotspot": raw_record.get("hotspot"),
                    "timestamp": raw_record.get("ts").isoformat() if raw_record.get("ts") else None,
                    "pm25": raw_record.get("pm25"),
                    "pm10": raw_record.get("pm10"),
                    "no": raw_record.get("no"),
                    "no2": raw_record.get("no2"),
                    "nox": raw_record.get("nox"),
                    "nh3": raw_record.get("nh3"),
                    "so2": raw_record.get("so2"),
                    "co": raw_record.get("co"),
                    "ozone": raw_record.get("ozone"),
                    "benzene": raw_record.get("benzene"),
                    "toluene": raw_record.get("toluene"),
                    "xylene": raw_record.get("xylene"),
                    "o_xylene": raw_record.get("oxylene"),
                    "eth_benzene": raw_record.get("ethbenzene"),
                    "mp_xylene": raw_record.get("mpxylene"),
                    "temperature": raw_record.get("temperature"),
                    "humidity": raw_record.get("humidity"),
                    "wind_speed": raw_record.get("windspeed"),
                    "wind_direction": raw_record.get("winddirection"),
                    "rainfall": raw_record.get("rainfall"),
                    "total_rainfall": raw_record.get("totalrainfall"),
                    "solar_radiation": raw_record.get("solarradiation"),
                    "pressure": raw_record.get("pressure"),
                    "vertical_wind_speed": raw_record.get("verticalwindspeed"),
                    "latitude": raw_record.get("latitude"),
                    "longitude": raw_record.get("longitude"),
                }
                
                computed_aqi = aqi.compute_aqi(bq_row)
                if computed_aqi is not None:
                    bq_row['aqi'] = computed_aqi
                else:
                    bq_row['aqi'] = None
                    
                bq_row['ingested_at'] = datetime.now(timezone.utc).isoformat()
                
                all_rows.append(bq_row)

                current_ts = raw_record.get("ts")
                if current_ts is not None:
                    if latest_ingested_ts is None or current_ts > latest_ingested_ts:
                        latest_ingested_ts = current_ts

            cursor.close()
            conn.close()
            
        except Exception as e:
            log.error("Failed to fetch data for city %s: %s", city, e)
            had_errors = True
            
    if all_rows:
        try:
            log.info("Loading %d aggregated records to BigQuery...", len(all_rows))
            bq_loader.load(all_rows)
            log.info("Batch load complete.")
        except Exception as e:
            log.error("Failed to load batch to BigQuery: %s", e)
            had_errors = True
    else:
        log.info("No data found for the given time window.")

    if had_errors:
        log.warning("Skipping watermark update due to errors in this cycle.")
        return

    if latest_ingested_ts is None:
        log.info("No records ingested in this cycle; watermark unchanged.")
        return

    _write_last_window_end(latest_ingested_ts)
    log.info("Updated ingestion watermark to latest ingested timestamp %s", latest_ingested_ts)

def main():
    log.info("Starting BigQuery Scheduled Ingestion Pipeline...")
    bq_loader = BigQueryLoader()
    
    # Run once immediately on startup
    fetch_and_load(bq_loader)
    
    # Run every one hour to push newly completed windows.
    schedule.every().hour.do(fetch_and_load, bq_loader=bq_loader)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutdown requested. Pipeline stopped.")

if __name__ == "__main__":
    main()
