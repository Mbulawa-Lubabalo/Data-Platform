import requests

url = "https://isibaloweb.statssa.gov.za/data/ETS/Monthly/Export%20and%20Import%20Unit%20Value%20IndicesP0142_7/P0142_7p.json"
    
def Fetch_Data():
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data

if __name__ == "__main__":
    print(Fetch_Data())