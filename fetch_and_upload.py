import requests
import json
import boto3
from datetime import datetime

def fetch_and_upload():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": False
    }

    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print("❌ Failed to fetch data:", response.status_code)
        print("Response:", response.text)
        return

    try:
        data = response.json()
    except Exception as e:
        print("⚠️ Error parsing JSON:", e)
        print("Raw response:", response.text)
        return

    s3 = boto3.client('s3',
        aws_access_key_id='AKIA6GSNGXEGBTD3JRF4',
        aws_secret_access_key='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'
    )

    filename = f"crypto_{datetime.now(timezone.utc).isoformat()}.json"
    s3.put_object(
        Bucket="crypto-dataeng-project",
        Key=f"raw/{filename}",
        Body=json.dumps(data)
    )

    print("✅ Uploaded to S3:", filename)

# Run the function
fetch_and_upload()
