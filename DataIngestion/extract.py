import requests

url = "https://isibaloweb.statssa.gov.za/data/ETS/Monthly/Export%20and%20Import%20Unit%20Value%20IndicesP0142_7/P0142_7p.json"
    
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json"
}

def Fetch_Data():
    
    response = requests.get(url, headers=HEADERS, timeout=10)
    response.raise_for_status()
    # data = response.json()
    return response.json()

if __name__ == "__main__":
    print(Fetch_Data())