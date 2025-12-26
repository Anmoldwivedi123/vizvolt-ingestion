
import requests
import psycopg2
import time
import os
from datetime import datetime
import threading
from fastapi import FastAPI
import uvicorn

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok", "service": "vizvolt-ingestion"}
    
def start_web():
    uvicorn.run(app, host="0.0.0.0", port=8000)

# =========================================================
# DATABASE CONFIG (RENDER)
# =========================================================

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", 5432))


# =========================================================
# API CONFIG
# =========================================================

API_URL = "https://analytics.ursaaenergy.com/api/service/getlastknownlocation"
API_SECRET = os.getenv("API_SECRET")


# =========================================================
# HELPERS
# =========================================================

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        sslmode="require"
    )

def safe(value, default=0):
    if value in [None, "", "null", "NULL", "NA"]:
        return default
    return value

def safe_timestamp(value):
    if value in [None, "", "null", "NULL", "NA"]:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    except Exception:
        return None

def fetch_api_data():
    payload = {
        "secretkey": API_SECRET,
        "imeino": "all",
        "pageindex": "1"
    }
    headers = {"Content-Type": "application/json"}
    res = requests.post(API_URL, json=payload, headers=headers, timeout=30)
    res.raise_for_status()
    return res.json().get("data", [])

# =========================================================
# INSERT / UPSERT
# =========================================================

def upsert_device(conn, device):
    cursor = conn.cursor()

    try:
        data = {k: safe(v) for k, v in device.items()}

        # ✅ FIX timestamps properly
        data["gpsiat"] = safe_timestamp(device.get("gpsiat"))
        data["bmsiat"] = safe_timestamp(device.get("bmsiat"))

        # ✅ Always add local created_at
        data["created_at"] = datetime.now()
        
        cursor.execute("""
            INSERT INTO device_data (
                imei, assetname, gpsiat, latitude, longitude, direction, speed,
                disttravelled_all, disttravelled_today, bmsiat, cc, voltage, current, soc,
                maxvoltagecellvalue, maxvltagecellnumber,
                minvoltagecellvalue, minvoltagecellnumber,
                ChargeDischargeStatus, ChargingCurrent, dischargingcurrent, DeviceStatus,
                serial, barcode,

                cellVolt1, cellVolt2, cellVolt3, cellVolt4, cellVolt5, cellVolt6,
                cellVolt7, cellVolt8, cellVolt9, cellVolt10, cellVolt11, cellVolt12,
                cellVolt13, cellVolt14, cellVolt15, cellVolt16,

                cellTemp1, cellTemp2, cellTemp3, cellTemp4, cellTemp5, cellTemp6,
                cellTemp7, cellTemp8, cellTemp9, cellTemp10, cellTemp11, cellTemp12,
                cellTemp13, cellTemp14, cellTemp15, cellTemp16,

                charging, avgrangekm, maxrangekm, minrangekm,
                created_at
            ) VALUES (
                %(imei)s, %(assetname)s, %(gpsiat)s, %(latitude)s, %(longitude)s, %(direction)s, %(speed)s,
                %(disttravelled_all)s, %(disttravelled_today)s, %(bmsiat)s, %(cc)s, %(voltage)s, %(current)s, %(soc)s,
                %(maxvoltagecellvalue)s, %(maxvltagecellnumber)s,
                %(minvoltagecellvalue)s, %(minvoltagecellnumber)s,
                %(ChargeDischargeStatus)s, %(ChargingCurrent)s, %(dischargingcurrent)s, %(DeviceStatus)s,
                %(serial)s, %(barcode)s,

                %(cellVolt1)s, %(cellVolt2)s, %(cellVolt3)s, %(cellVolt4)s, %(cellVolt5)s, %(cellVolt6)s,
                %(cellVolt7)s, %(cellVolt8)s, %(cellVolt9)s, %(cellVolt10)s, %(cellVolt11)s, %(cellVolt12)s,
                %(cellVolt13)s, %(cellVolt14)s, %(cellVolt15)s, %(cellVolt16)s,

                %(cellTemp1)s, %(cellTemp2)s, %(cellTemp3)s, %(cellTemp4)s, %(cellTemp5)s, %(cellTemp6)s,
                %(cellTemp7)s, %(cellTemp8)s, %(cellTemp9)s, %(cellTemp10)s, %(cellTemp11)s, %(cellTemp12)s,
                %(cellTemp13)s, %(cellTemp14)s, %(cellTemp15)s, %(cellTemp16)s,

                %(charging)s, %(avgrangekm)s, %(maxrangekm)s, %(minrangekm)s,
                %(created_at)s
            )
        """, data)

        conn.commit()
        print(f"Inserted IMEI {device.get('imei')} at {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print("DB ERROR:", e)

    finally:
        cursor.close()


# =========================================================
# MAIN LOOP
# =========================================================

def main():
    print("Vizvolt RAW ingestion started...")

    # 🔹 start web server in background
    threading.Thread(target=start_web, daemon=True).start()

    while True:
        try:
            conn = get_db_connection()
            devices = fetch_api_data()

            for device in devices:
                upsert_device(conn, device)

            conn.close()
        except Exception as e:
            print("MAIN LOOP ERROR:", e)

        time.sleep(10)


if __name__ == "__main__":
    main()
