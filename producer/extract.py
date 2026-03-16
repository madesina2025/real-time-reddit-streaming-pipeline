import requests
import os
from dotenv import load_dotenv

load_dotenv()


URL = os.getenv('API_URL')
if not URL or not URL.startswith(("http://", "https://")):
        raise ValueError("API_URL missing/invalid in .env (must start with http(s)://)")

def extract_data():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    # #return response.json()['PublicAssistanceFundedProjectsDetails']
    # data = response.json()['PublicAssistanceFundedProjectsDetails']
    payload = response.json()
    key = "PublicAssistanceFundedProjectsDetails"
    items = payload.get(key, [])
    #print(f"Fetched {len(items)} records")
    #print("Sample:", items[:3])
    return items

    #print(data[:3])
if __name__ == "__main__":
    extract_data()
    
