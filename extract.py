import requests

API_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest"
    "/services/Animal_Services/FeatureServer/0/query"
)
BATCH_SIZE = 1000


def extract() -> list[dict]:
    all_data, offset = [], 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultRecordCount": BATCH_SIZE,
            "resultOffset": offset,
        }
        r = requests.get(API_URL, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        features = data.get("features", [])
        if not features:
            break
        all_data.extend(f["attributes"] for f in features)
        if not data.get("exceededTransferLimit", False):
            break
        offset += len(features)
    print(f"  Extracted {len(all_data):,} records")
    return all_data
