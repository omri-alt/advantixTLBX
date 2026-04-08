import os
import csv
import requests
from datetime import datetime, timedelta
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

import gdocs as gdocs

# here we keep the ids of all wlkl1 campaigns that have converted already
WLcampaigns = {
    325562: ['s3ed3a7177c013e2', 's3e371c141d436a6']
}  # campID : [list of sources]

# Prefer KEYSK in .env; legacy name keySK still supported (via config or direct env).
try:
    from config import SOURCEKNOWLEDGE_API_KEY as keySK
except ImportError:
    keySK = ""
if not keySK:
    keySK = (os.getenv("KEYSK") or os.getenv("keySK") or "").strip()
headers_sk = {
    "accept": "application/json",
    "X-API-KEY": f"{keySK}"  # Replace with your actual API key
}


#1. 12.06 - reicves a list of dictionaries,required file name and list of values for headers and saves them to a csv file
def save_response_to_csv(response, filename, field_names):
    """Save the response content to a CSV file"""
    csv_filename = f"{filename}.csv"
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        # Write header
        writer.writeheader()
        written_count = 0
        for item in response:
            writer.writerow(item)
            written_count += 1
        print(f"CSV file saved to {csv_filename} with {written_count} rows")


#2. 12.06 - using the SK api creates a list of all advertisers cleaned from bl names and returns it as list of dictionaries (for every 1k advertisers add another iteration)
def get_advertisers():
    field_names = ['id', 'name', 'businessUrl', 'categoryId', 'categoryName']
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/advertisers?page=1"
    r = requests.get(endpoint, headers=headers_sk)
    bl = ['Mistake', 'test', 'Test', 'mistake']
    set = r.json()['items']
    cleanset = []
    for item in set:
        if item['name'] not in bl:
            cleanset.append(item)
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/advertisers?page=2"
    r = requests.get(endpoint, headers=headers_sk)
    set = r.json()['items']
    for item in set:
        if item['name'] not in bl:
            cleanset.append(item)
    save_response_to_csv(cleanset, "sk_advertisers", field_names)
    return cleanset


#2b. 22.06 - recoves an advertiser id and returns it's data using the sk api
def get_advertiser(id):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/advertisers/{id}"
    r = requests.get(endpoint, headers=headers_sk)
    return r.json()


#2c. 22.06 - recoves an advertiser id and returns it's data using the sk api
def update_advertiser(id, data):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/advertisers/{id}"
    r = requests.put(endpoint, headers=headers_sk, json=data)
    match r.status_code:
        case 200:
            return r.json()
        case _:
            print(f"cooldown in advertiser {id}")
            time.sleep(60)
            r = requests.put(endpoint, headers=headers_sk, json=data)
    return r.json()


#3.12.06 - using SK api requests a list of all campaigns and sorts them into active and paused and saves the two lists as csv files
def get_campaigns():
    field_names = [
        'id', 'name', 'active', 'start', 'end', 'updated', 'advertiser'
    ]
    active = []
    paused = []
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/campaigns?page=1"
    r = requests.get(endpoint, headers=headers_sk)
    try:
        set = r.json()['items']
    except:
        print("pause for 60 sec")
        time.sleep(60)
        r = requests.get(endpoint, headers=headers_sk)
        set = r.json()['items']
    for i in range(len(set)):
        if set[i]['active']:
            active.append(set[i])
        else:
            paused.append(set[i])
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/campaigns?page=2"
    r = requests.get(endpoint, headers=headers_sk)
    try:
        set = r.json()['items']
    except:
        print("pause for 60 sec")
        time.sleep(60)
        r = requests.get(endpoint, headers=headers_sk)
        set = r.json()['items']
    for i in range(len(set)):
        if set[i]['active']:
            active.append(set[i])
        else:
            paused.append(set[i])
    save_response_to_csv(active, "sk_campaignsACT", field_names)
    save_response_to_csv(paused, "sk_campaignsPaused", field_names)
    return set


#3b 12.06 - gets campaign id and returns campaign data
def get_campaignById(id):
    endpoint = f"https://api.sourceknowledge.com//affiliate/v2/campaigns/{id}"
    r = requests.get(endpoint, headers=headers_sk)
    return r.json()


#4. 12.06 - recives an advertiser id and using the SK api returns a list of all active campaigns, when exceeding the rate limit it waits for 60 sec and tries again
def get_campaignsByAdvid(advid):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns?advertiserId={advid}"
    r = requests.get(endpoint, headers=headers_sk)
    try:
        set = r.json()['items']
    except:
        print("pause for 60 sec")
        time.sleep(60)
        try:
            r = requests.get(endpoint, headers=headers_sk)
            set = r.json()['items']
        except:
            set = r.json()
            print(set)
    return set


#5.12.06 - using get_campaigns updates the list  list of all active and paused campagns and then opens the updated active csv file and returns it as a list of dictionaries
def get_activeCampaigns():
    get_campaigns()
    with open("sk_campaignsACT.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        active = [row for row in reader]  # Convert to list of dicts

    return active


#6.12.06 - recives campaign id and using the SK api pauses it and returns the response json
def pause_campaign(id):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns/{id}"
    body = {"active": False}
    r = requests.post(endpoint, headers=headers_sk, json=body)
    match r.status_code:
        case 200:
            print(f"campaign {id} paused")
        case _:
            print(f"error in pausing campaign {id}")
    return r.json()


#11. 15.06 recives advertisers parameters - [advertiser name , merch url with https:// , category ID ] and creates a new advertiser
def new_advertiser(name, merch_url, catego):
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/advertisers"
    body = {"name": name, "businessUrl": merch_url, "categoryId": catego}
    r = requests.post(endpoint, headers=headers_sk, json=body)
    print(r.json())
    adv_id = r.json()['id']
    return adv_id


#12. 12.06 recives campaign settings and creates a new campaign
def new_campaign(settings):
    endpoint = "https://api.sourceknowledge.com/affiliate/v2/campaigns"
    r = requests.post(endpoint, headers=headers_sk, json=settings)
    print(r.json())
    return r


#16. 12.06 recives campaign id and report start and end dates. using the SK api getting auction source level data and returns it as a list of dictionaries
def campaign_buying_stats(id, start, end):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{id}/by-publisher?from={start}&to={end}"
    data = {
        "from": start,
        "to": end,
    }
    r = requests.get(endpoint, headers=headers_sk)
    return r


#17. 12.06 uses the get_activeCampaigns to update the csv files of campaigns and get fixadv to get KLFIX advertisers and returns a list of all active campaigns that are KLFIX
def get_fixcamp():
    field_names = [
        'id', 'name', 'active', 'start', 'end', 'updated', 'advertiser'
    ]
    actives = get_activeCampaigns()
    
    fixed_advid, fixedadv = get_fixadv()
    fix = []
    for act in actives:
        for id in fixed_advid:
            if act['advertiser'].find(id) != -1:
                fix.append(act)
    save_response_to_csv(fix, "Fix-sk_campaignsACT", field_names)
    return fix


#18. 12.06 using get_advertisers updates advertisers list and returns a touple of ([list of advertiser ids] , [list all fixed advertisers data] )
def get_fixadv():
    get_advertisers()
    with open("sk_advertisers.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        adv = [row for row in reader]  # Convert to list of dicts
    fix_ids = []
    fix_adv = []
    for i in range(len(adv)):
        if adv[i]['name'].find('KLFIX') != -1:
            fix_ids.append(adv[i]['id'])
            fix_adv.append(adv[i])
    return fix_ids, fix_adv


#18b. 22.06 using get_advertisers updates advertisers list and returns a touple of ([list of advertiser ids] , [list all fixed advertisers data] )
def get_KLWLadv():
    get_advertisers()
    with open("sk_advertisers.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        adv = [row for row in reader]  # Convert to list of dicts
    fix_ids = []
    fix_adv = []
    for i in range(len(adv)):
        if adv[i]['name'].split('-')[-1].find('KLWL') != -1:
            fix_ids.append(adv[i]['id'])
            fix_adv.append(adv[i])
    return fix_ids, fix_adv


#19. 12.06  recives SK campaign ID , using the campaign_buyings_stats gets detailed source level information on the auctiona and makes a list of all subid in campaign with bidfactor 1
def find_new_subs7days(camp_id):
    today = datetime.today().strftime('%Y-%m-%d')
    d = datetime.today() - timedelta(days=7)
    date = d.strftime('%Y-%m-%d')
    micro = campaign_buying_stats(camp_id, date, today).json()
    micro_data = micro.get('items', [])
    new_subs = []
    for i in range(len(micro_data)):
        if micro_data[i]['bidFactor'] == 1:
            print(micro_data[i]['subId'],
                  f"new source found in campaign {camp_id}")
            new_subs.append(micro_data[i]['subId'])
    return new_subs


#20. 12.06 function recives campaign id, subid and bidfactor and updates it's bid_factor
def update_bid_factor(camp_id, sub_id, bid):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns/{camp_id}/bid-factor"
    data = {"subId": sub_id, "bidFactor": bid}
    r = requests.post(endpoint, headers=headers_sk, json=data)
    print(r.json())
    return


#21. 12.06 function uses get_fixcamp to get all active KLFIX campaigns and find_new_subs7days to find all new subs in the last 7 days and updates their bid_factor to 0.205
def optimize_newsource_fix7days():
    fix = get_fixcamp()
    for i in range(len(fix)):
        newsub = find_new_subs7days(fix[i]['id'])
        time.sleep(1)
        print(f"campaign {fix[i]['id']} found ", len(newsub), "new subs")
        if len(newsub) != 0:
            for k in range(len(newsub)):
                time.sleep(1)
                print(f"updating sub {newsub[k]} to 0.205")
                b = update_bid_factor(fix[i]['id'], newsub[k], 0.205)
                time.sleep(2)
    return


#19b. 20.06  recives SK campaign ID , using the campaign_buyings_stats gets detailed source level information on the auctiona and makes a list of all subid in campaign with bidfactor 1
def find_new_subs7days2(camp_id):
    today = datetime.today().strftime('%Y-%m-%d')
    d = datetime.today() - timedelta(days=7)
    date = d.strftime('%Y-%m-%d')
    micro = campaign_buying_stats(camp_id, date, today).json()
    micro_data = micro.get('items', [])
    new_subs = []
    for i in range(len(micro_data)):
        if micro_data[i]['bidFactor'] == 1:
            print(micro_data[i]['subId'],
                  f"new source found in campaign {camp_id}")
            new_subs.append(micro_data[i]['subId'])
    return new_subs


#20b. 20.06 function recives campaign id, and list of new subids and updates it's bid_factor to 1
def update_bid_factor2(camp_id, list):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns/{camp_id}/bid-factors"
    data = {"subIds": list}
    print(data)
    r = requests.post(endpoint, headers=headers_sk, json=data)
    if r.status_code == 429:
        print(f"pause for 60 sec for rate limit exceeded and status code is {r.status_code}")
        time.sleep(60)
        r = requests.post(endpoint, headers=headers_sk, json=data)
    elif r.status_code != 200:
        print(f"error in updating bid factor for campaign {camp_id} and status code is {r.status_code} and response is {r.json()}")
    print(r.json())
    return


#21b. 20.06 function uses get_fixcamp to get all active KLFIX campaigns and find_new_subs7days to find all new subs in the last 7 days and updates their bid_factor to 0.205
def optimize_newsource_fix7days2():
    updated = []
    fix = get_fixcamp()
    for camp in fix:
        newsub = find_new_subs7days(camp['id'])
        print(f"campaign {camp['id']} found ", len(newsub), "new subs")
        if len(newsub) != 0:
            toupdate = []
            for k in newsub:
                toupdate.append({"subId": k, "bidFactor": 0.205})
            updated.append(update_bid_factor2(camp['id'], toupdate))
            print(toupdate)
    return updated


# auxilary function finds campaigns bid
def campaign_cpc(camp_id):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns/{camp_id}"
    try:
        r = requests.get(endpoint, headers=headers_sk)
        return r.json()['cpc']
    except:
        return 0


def winRate(camp_id, data):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp_id}/by-publisher"
    r = requests.get(endpoint, headers=headers_sk, params=data)
    match r.status_code:
        case 200:
            if r.json()['itemsCount'] == 0:
                return 'N/A', 'N/A'
            return r.json()['items'][0]['winRate'], r.json(
            )['items'][0]['bidFactor']
        case _:
            print(f"error in getting winrate for campaign {camp_id}")
            time.sleep(60)
            r = requests.get(endpoint, headers=headers_sk, params=data)
            if r.status_code == 200:
                try:
                    return r.json()['items'][0]['winRate'], r.json(
                    )['items'][0]['bidFactor']
                except:
                    print(r.json())
                    return 'N/A', 'N/A'
            else:
                return 'N/A', 'N/A'


#22.a 26.06 function recives source id and campaign Id and returns it's winrate30 , winrate7 and winrateYest and winrateToday and current bid factor bidfactor
def findSourceinCampaign(source, camp):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    days30 = (datetime.now() - timedelta(days=31)).strftime('%Y-%m-%d')
    days7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp}/by-publisher"
    data = {
        "from": days30,
        "to": yesterday,
        "subid": source,
    }
    winrate30, bid4 = winRate(camp, data)
    data['from'] = days7
    winrate7, bid3 = winRate(camp, data)
    data['from'] = yesterday
    winrateYest, bid2 = winRate(camp, data)
    data['from'] = today
    data['to'] = today
    winrateToday, bid1 = winRate(camp, data)
    if bid1 == 'N/A':
        if bid2 == 'N/A':
            if bid3 == 'N/A':
                if bid4 == 'N/A':
                    bid1 = 1
                else:
                    bid1 = bid4
            else:
                bid1 = bid3
        else:
            bid1 = bid2
    campaign = get_campaignById(camp)
    if (camp == '322908'):
        print(campaign)
    try:
        skStatus = campaign['active']
    except:
        skStatus = 'could not obtain status'
    try:
        cpc = campaign['cpc'] * bid1
        if bid1 == 1:
            cpc = f"Src Bfactor N/A, campaign cpc is {campaign['cpc']}"
    except:
        cpc = 'could not obtain campaign cpc'

    return {
        'winrate30': winrate30,
        'winrate7': winrate7,
        'winrateYest': winrateYest,
        'winrateToday': winrateToday,
        'bid': cpc,
        'SKstatus': skStatus
    }


#22b. 21.06 function recives source id and returns a list of all campaigns it is in with winrate 30 , 7 and today and current bid factor bidfactor also prints it to a csv file
def findSourceInCampaigns(source):
    output = []
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    days30 = (datetime.now() - timedelta(days=31)).strftime('%Y-%m-%d')
    days7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    campaigns = get_activeCampaigns()
    for camp in campaigns:
        endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp['id']}/by-publisher"
        data = {
            "from": days30,
            "to": today,
            "subid": source,
        }
        stats = requests.get(endpoint, headers=headers_sk, params=data)
        if camp['name'] == 'BestbuyFIX-US-all':
            print(stats.json())  #testpoint
        if stats.status_code != 200 and stats.json(
        )['error'] == "Too Many Requests":
            print(f"cooldown in campaign {camp['name']}")
            time.sleep(60)
            stats = requests.get(endpoint, headers=headers_sk, params=data)
            print(stats.status_code)
        stats = stats.json()
        if stats['itemsCount'] == 0:
            continue
        else:
            for item in stats['items']:
                if item['subId'] == source:
                    data = {
                        "from": today,
                        "to": today,
                        "subid": source,
                    }
                    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp['id']}/by-publisher"
                    r = requests.get(endpoint, headers=headers_sk, params=data)
                    try:
                        winrate = r.json()['items'][0]['winRate']
                    except:
                        winrate = "couldn't obtain today's winrate"
                    data['from'] = yesterday
                    data['to'] = yesterday
                    r = requests.get(endpoint, headers=headers_sk, params=data)
                    try:
                        winrateYest = r.json()['items'][0]['winRate']
                    except:
                        winrateYest = "couldn't obtain yesterday's winrate"

                    data['from'] = days7
                    r = requests.get(endpoint, headers=headers_sk, params=data)
                    try:
                        winrate7 = r.json()['items'][0]['winRate']
                    except:
                        winrate7 = "couldn't obtain 7 days winrate"
                    cpc = campaign_cpc(camp['id'])
                    output.append({
                        "campaignName":
                        camp['name'],
                        "campaign":
                        camp['id'],
                        "winrate30":
                        item['winRate'],
                        "winrate7":
                        winrate7,
                        "winrateYest":
                        winrateYest,
                        "winrateToday":
                        winrate,
                        "bid":
                        cpc * item['bidFactor'],
                        'url':
                        f"https://app.sourceknowledge.com/agency/campaigns/{camp['id']}/by-channel"
                    })
                break
    match source:
        case 's3ed3a7177c013e2':
            sname = source + 'KLWL1'
        case _:
            sname = source
    gdocs.create_or_update_sheet_from_dicts(f"{sname}", output)
    return output


#22b. 24.06 function recives list of source ids and returns a list of all campaigns it is in with winrate 30 , 7 and today and current bid factor bidfactor also prints it to a csv file
def findSourceListInCampaigns(sourceList):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    days30 = (datetime.now() - timedelta(days=31)).strftime('%Y-%m-%d')
    days7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    outputs = []
    campaigns = get_activeCampaigns()
    for source in sourceList:
        output = []
        for camp in campaigns:
            endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp['id']}/by-publisher"
            data = {
                "from": days30,
                "to": today,
                "subid": source,
            }
            stats = requests.get(endpoint, headers=headers_sk, params=data)
            if camp['name'] == 'BestbuyFIX-US-all':
                print(stats.json())  #testpoint
            if stats.status_code != 200 and stats.json(
            )['error'] == "Too Many Requests":
                print(f"cooldown in campaign {camp['name']}")
                time.sleep(60)
                stats = requests.get(endpoint, headers=headers_sk, params=data)
                print(stats.status_code)
            stats = stats.json()
            try:
                if stats['itemsCount'] == 0:
                    continue
            except:
                print(stats)
            else:
                for item in stats['items']:
                    if item['subId'] == source:
                        data = {
                            "from": today,
                            "to": today,
                            "subid": source,
                        }
                        endpoint = f"https://api.sourceknowledge.com/affiliate/v2/stats/campaigns/{camp['id']}/by-publisher"
                        r = requests.get(endpoint,
                                         headers=headers_sk,
                                         params=data)
                        try:
                            winrate = r.json()['items'][0]['winRate']
                        except:
                            winrate = "couldn't obtain today's winrate"
                        data['from'] = yesterday
                        data['to'] = yesterday
                        r = requests.get(endpoint,
                                         headers=headers_sk,
                                         params=data)
                        try:
                            winrateYest = r.json()['items'][0]['winRate']
                        except:
                            winrateYest = "couldn't obtain yesterday's winrate"

                        data['from'] = days7
                        r = requests.get(endpoint,
                                         headers=headers_sk,
                                         params=data)
                        try:
                            winrate7 = r.json()['items'][0]['winRate']
                        except:
                            winrate7 = "couldn't obtain 7 days winrate"
                        cpc = campaign_cpc(camp['id'])
                        output.append({
                            "campaignName":
                            camp['name'],
                            "campaign":
                            camp['id'],
                            "winrate30":
                            item['winRate'],
                            "winrate7":
                            winrate7,
                            "winrateYest":
                            winrateYest,
                            "winrateToday":
                            winrate,
                            "bid":
                            cpc * item['bidFactor'],
                            'url':
                            f"https://app.sourceknowledge.com/agency/campaigns/{camp['id']}/by-channel"
                        })
                    break
        match source:
            case 's3ed3a7177c013e2':
                sname = source + '_KLWL1'
            case 's6edc9136846d915':
                sname = source + '_KLWL2'
            case 'sfb01bfc6ac1cbe3':
                sname = source + '_KLWL3'
            case 's06bc48fe7a74470':
                sname = source + '_KLWL4'
            case 's27b58e2b6548902':
                sname = source + '_KLWL5'
            case 's2599879d2841979':
                sname = source + '_KLWL6'
            case 's1bf84bf08ddb9e4':
                sname = source + '_KLWL7'
            case 'sab80d384b9e33bf':
                sname = source + '_KLWL8'
            case 's329c543a1b75b0b':
                sname = source + '_KLWL9'
            case _:
                sname = source
        gdocs.create_or_update_sheet_from_dicts(f"{sname}", output)
        outputs.append(output)
    return outputs


#23. 22.06 function optimizes the KLWL1 campaigns
def optimize_KLWL1():
    today = datetime.now().strftime('%Y-%m-%d')
    updated = []
    raiseBid = []
    klwlIDS, klwlObj = get_KLWLadv()
    for adv in klwlObj:
        campaigns = get_campaignsByAdvid(adv['id'])
        campId = campaigns[0]['id']
        start = campaigns[0]['start'][0:10]
        stats = campaign_buying_stats(campId, start, today)
        stats = stats.json()
        try:
            if stats['itemsCount'] == 0:
                cpc = campaign_cpc(campId)
                raiseBid.append({
                    'campId':
                    campId,
                    'campName':
                    campaigns[0]['name'],
                    'bid':
                    cpc,
                    'url':
                    f"https://app.sourceknowledge.com/agency/campaigns/{campId}/by-channel"
                })
        except:
            print("didn't manage to analyze campaign")
    gdocs.create_or_update_sheet_from_dicts("raiseBid", raiseBid)


#24 26.06 function goes over the "QualityWL" sheet and collects winrate data and KL monetization status
def collect_QualityWL():
    return


def update_campaign(id, data):
    endpoint = f"https://api.sourceknowledge.com/affiliate/v2/campaigns/{id}"
    r = requests.put(endpoint, headers=headers_sk, json=data)
    match r.status_code:
        case 200:
            print(f"campaign {id} updated")
        case _:
            print(f"cooldown in campaign {id}")
            time.sleep(60)
            r = requests.put(endpoint, headers=headers_sk, json=data)
    return r.json()


#find campaigns that started today and print them to newcamps sheet
def firstOfJuly_fixBulkErrors():
    camps = get_campaigns()
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    newcamps = []
    print(camps[0])
    for camp in camps:
        if camp['start'][0:10] == yesterday:
            newcamps.append(camp)
    print(f"there are {len(newcamps)} new campaigns today")
    gdocs.create_or_update_sheet_from_dicts('newcamps', newcamps)


#find campaigns that started today and have the name KLFIX and change it to KLWL5
def campsOfTodayRename():
    camps = get_campaigns()
    print(len(camps))
    today = datetime.now().strftime('%Y-%m-%d')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    newcamps = []
    print(camps[0])
    for camp in camps:
        if camp['start'][0:10] == yesterday and camp['advertiser']['name'].split(
                '-')[-1] not in [
                    'KLWL1', 'KLWL2', 'KLWL3', 'KLWL4', 'KLWL5', 'KLWL6',
                    'KLWL7', 'KLWL8'
                ]:
            print(camp['advertiser']['id'])
            if int(camp['advertiser']['id']) >= 118074:
                newcamps.append(camp)
                data = camp['advertiser']
                data['name'] = data['name'].replace('KLFIX', 'KLWL9')
                print(data['name'])
                r = update_advertiser(camp['advertiser']['id'], data)
            else:
                print(
                    f"campaign {camp['advertiser']['id']} is not a new campaign"
                )
    print(f"there are {len(newcamps)} new campaigns today")
    firstOfJuly_fixBulkErrors()


def campsOfTodayRenameBrandPrefix():
    camps = get_campaigns()
    today = datetime.now().strftime('%Y-%m-%d')
    newcamps = []
    print(camps[0])
    for camp in camps:
        if camp['start'][0:10] == today and camp['advertiser']['name'].split(
                '-')[-1] == 'KLWL8':
            newcamps.append(camp)
            data = get_campaignById(camp['id'])
            data['trackingUrl'] = data['trackingUrl'].replace(
                'KLFIX', 'KLFLEX')
            r = update_campaign(camp['id'], data)
