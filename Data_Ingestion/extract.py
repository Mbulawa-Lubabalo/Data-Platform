import requests

url = "https://isibaloweb.statssa.gov.za/data/ETS/Monthly/Export%20and%20Import%20Unit%20Value%20IndicesP0142_7/P0142_7p.json"
    
def Fetch_Data():

    # url = "https://isibaloweb.statssa.gov.za/data/ETS/Monthly/Export%20and%20Import%20Unit%20Value%20IndicesP0142_7/P0142_7p.json"
    
    # headers = {
    # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    #               "AppleWebKit/537.36 (KHTML, like Gecko) "
    #               "Chrome/120.0.0.0 Safari/537.36",
    # "Accept": "application/json, text/plain, */*",
    # "Referer": "https://isibaloweb.statssa.gov.za/",
    # "Origin": "https://isibaloweb.statssa.gov.za",
    # }
    
    response = requests.get(url, timeout=10)
    # print("Status code:", response)
    # print("Raw response text:", response.text)
    response.raise_for_status()
    data = response.json()
    return data

print(Fetch_Data())