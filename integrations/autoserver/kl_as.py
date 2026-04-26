import requests
import os
import csv
import datetime
import json 
from integrations.autoserver import gdocs_as as gdocs
keyKL = os.getenv("keyKL")
headers_KL = {
    "Authorization": f"Bearer {keyKL}",
    "Content-Type": "application/json"
}
geos = ['ae', 'at' , 'au', 'be', 'br', 'ca', 'ch', 'cz', 'de', 'dk', 'es', 'fi', 'fr',
    'gr', 'hk', 'hu', 'id', 'ie', 'in', 'it', 'jp', 'kr', 'mx', 'my', 'nb', 'nl',
    'no', 'nz', 'ph', 'pl', 'pt', 'ro', 'se', 'sg', 'sk', 'tr', 'uk', 'us', 'vn'
]

#1. 12.06 - recives a url and geo and checks if the url is monetized in the geo using the KL api and returns the response json
def check_monetization(url,geo):
    endpoint = f'https://api.kelkoogroup.net/publisher/shopping/v2/search/link?&country={geo}&merchantUrl={url}'
    r = requests.get(endpoint, headers=headers_KL)
    match r.status_code:
        case 200:
            return r.json()
        case 404:
            print(r.json())
            return False
        case _:
            print(r.json())
            return 'error occured'
#1b. 17.06 - recives a url and geo and checks if the url is monetized in the geo using the KL api and returns the raw response
def check_monetizationWithResp(url,geo):
    endpoint = f'https://api.kelkoogroup.net/publisher/shopping/v2/search/link?&country={geo}&merchantUrl={url}'
    r = requests.get(endpoint, headers=headers_KL)
    return r

#2. 12.06 - downloads all merchants from KLapi and saves them to a csv file 
def get_kelkoo_adv(headers_KL, geo):
    # Get response from API
    print(f"Fetching data for {geo}...")
    response = requests.get(
        f"https://api.kelkoogroup.net/publisher/shopping/v2/feeds/merchants?country={geo}&format=JSON", 
        headers=headers_KL
    )
    print(f"Response status code: {response.status_code}")
    if {response.status_code} : 
        return response.json()
    else: 
        return False

def merchants2csvw(merc,fname,fields_to_extract):
    gdocs.create_or_update_sheet_from_dicts(fname, merc)
    # Define the fields to extract - these should match the keys     in your merchant dictionaries
    # Create and write to CSV
    csv_filename = f"{fname}.csv"
    with open(csv_filename, mode="w", newline="", encoding="utf-8")     as file:
        writer = csv.DictWriter(file, fieldnames=fields_to_extract)
        # Write header
        writer.writeheader()
        written_count = 0
        for item in merc:
            writer.writerow(item)
            written_count += 1
        print(f"Total written: {written_count}")
        print(f"CSV file saved to {csv_filename}")
    return True



def getall_merchantsFIX(fname):
    merchants = []
    for geo in geos :
        resp = get_kelkoo_adv(headers_KL, geo)
        if resp == False:
            continue
        else:
            for merch in resp:
                try:
                    if merch['merchantTier'] ==  'Static' and merch['supportsLinks'] :
                        item = {
                            "name": merch['name'],
                            "url": merch['url'],
                            "deliveryCountries": geo,
                            "supportsLinks": merch["supportsLinks"],
                            'merchantEstimatedCpc': merch.get('merchantEstimatedCpc', 'N/A'),
                            "merchantMobileEstimatedCpc": merch.get('merchantMobileEstimatedCpc', 'N/A'),
                            "currency": merch.get('currency', 'N/A'),
                            'Restrictions': merch.get('forbiddenTrafficTypes',None),
                            "spotlight" : merch.get('spotlight',None),
                        }
                        merchants.append(item)
                except: print(f'error occured in {merch}')
    merchants2csvw(merchants, f"{fname}",merchants[0].keys())
    gdocs.create_or_update_sheet_from_dicts(fname,merchants)
    
    return merchants




