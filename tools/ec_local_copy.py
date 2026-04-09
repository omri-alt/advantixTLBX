import os
import requests
import hashlib
from datetime import datetime, timedelta
import csv
import time
import json
import kl as kl
import gdocs as gd

headers = {"Content-Type": "application/json"}
feedid = '22974eb2-a9b8-4eb8-a0cf-735538fff4ea_self'
sheetid = '1-kclsSvR7LUtpi-Ymrd9wRYbbmkraP2tGLTrvSnih9c'
sources_SheetId = '1-kclsSvR7LUtpi-Ymrd9wRYbbmkraP2tGLTrvSnih9c'
advkey = os.getenv("ECadvKey")
authkey = os.getenv("ECauthKey")
secretkey = os.getenv("ECsecretKey")


#1. 12.06 - functon open the JSON DB and returns it as a list of dictionaries
def load_campaignsJson(filepath="ecopti.json"):
    with open(filepath, "r") as f:
        campaigns = json.load(f)
    return campaigns


#2a. 12.06 - function generates EC auth token with time sign of now
def generate_authtoken(secret_key):
    # Get the current UTC time in the specified format
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    # Concatenate the timestamp and secret key
    input_string = current_time + secret_key

    # Generate MD5 hash
    md5_hash = hashlib.md5(input_string.encode('utf-8')).hexdigest().upper()

    return md5_hash


#2b. 12.06 - function generates EC auth token with time sign of date range
def generate_authtokenNew(secret_key, start, end):
    # Get the current UTC time in the specified format
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M")

    # Concatenate timestamp, start, end, and secret key
    input_string = f"{timestamp}{start}{end}{secret_key}"

    # Generate MD5 hash and return Base16 encoded (hex) in uppercase
    authtoken = hashlib.md5(input_string.encode('utf-8')).hexdigest().upper()

    return authtoken


#3a. 12.06 - function gets all campaigns from EC and returns them as a list of dictionaries
def get_campaigns():
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    '''print(campaigns[0].keys())
  for campaign in campaigns:
    print(f"{campaign['name']} and id is {campaign['id']}") '''
    return campaigns


#3b. 12.06 - function gets all campaigns from EC and sorts all campaigns with '-klfix' in their campaign name and returns them as a list of dictionaries
def get_campaignsKLFIX():
    klfix = []
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    for campaign in campaigns:
        if campaign['name'].find('-klfix') != -1:
            klfix.append(campaign)
    return klfix


def get_campaignsKL():
    kl = []
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    for campaign in campaigns:
        if campaign['name'].split('-')[-1] == 'kl':
            kl.append(campaign)
    return kl


#3c. 15.06 - function gets all campaigns from EC and sorts all campaigns with review status approved and returns them as a list of dictionaries
def get_campaignsApproved():
    approved = []
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    for campaign in campaigns:
        if campaign['reviewstatus'] == 'approved':
            approved.append(campaign)
    return approved


#3d. 15.06 - function gets all campaigns from EC and sorts all campaigns with '-klfix' in their campaign name and returns them as a list of dictionaries
def get_campaignS24():
    s24 = []
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    for campaign in campaigns:
        if campaign['name'].find('-sh') != -1:
            s24.append(campaign)
    return s24


def get_campaignsKLFIXactiveAproved():
    klfix = []
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    campaigns = r.json()['campaigns']
    for campaign in campaigns:
        if campaign['name'].find('-klfix') != -1 and campaign[
                'reviewstatus'] == 'approved' and campaign[
                    'status'] == 'active':
            klfix.append(campaign)
    return klfix


def get_campaignById(id):
    endpoint = f"https://advertiser.ecomnia.com/get-advertiser-campaigns?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}&campaign_id={id}"
    r = requests.get(endpoint, headers=headers)
    return r.json()


#4. 12.06 - function gets all campaigns klfix and check kl api for their monetization status and prints those who are not monetized for operator to pause
def unmonetized_EC():
    campaigns = get_campaignsKLFIX()
    for campaign in campaigns:
        if campaign['status'] == 'active' and campaign[
                'reviewstatus'] == 'approved':
            id = campaign['id']
            geo = campaign['name'].split('-')[-2].lower()
            hps = campaign['whitelistdomains']
            for hp in hps:
                if hp.find('www') != -1:
                    match kl.check_monetization(f'https://{hp}', geo):
                        case False:
                            print(
                                f"hp {hp} is not monetized in {geo} and campaign {campaign['name']} was paused"
                            )
                            #pause_campaign(id)
                        case 'error occured':
                            print(
                                f"error occured in {hp}, geo {geo} and campaign {id} was paused"
                            )
                            #pause_campaign(id)
                        case _:
                            continue


#5. 12.06 - download updated report of the merchants and save it to a csv file
def get_merchants():
    endpoint = f"https://advertiser.ecomnia.com/get-merchants?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}"
    r = requests.get(endpoint, headers=headers)
    merchants = r.json()['merchants']
    firstline = merchants[0].keys()
    with open('ECmerchantslist.csv', mode='w', newline='',
              encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(firstline)
        for merchant in merchants:
            writer.writerow(merchant.values())
    return merchants


#6. 12.06 - function gets merchant name and looks for it mid in the csv file
def find_merchant_id_by_name(name):
    with open('ECmerchantslist.csv', mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['mname'] == name:
                return row['mid']
        return "not found"


#7. 12.06 - function gets campaign id and the updated campaign settings looks for it name in the csv file
def update_campaign(campaign_id, data):
    endpoint = f"https://advertiser.ecomnia.com/update-advertiser-campaign?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}&id={campaign_id}"
    r = requests.post(endpoint, headers=headers, json=data)
    print(r.json())
    return r.json()


#8a 12.06 - function gets campaign name and pauses it
def pause_campaignWithName(campaign_name):
    campaigns = get_campaigns()
    for campaign in campaigns:
        if campaign['name'] == campaign_name:
            campaign['status'] = 'paused'
            update_campaign(campaign['id'], campaign)
            print(f"campaign {campaign['name']} was paused")
            return


#8b 12.06 - function gets campaign id and pauses it
def pause_campaignWitId(campaign_id):
    campaigns = get_campaigns()
    for campaign in campaigns:
        if campaign['id'] == campaign_id:
            campaign['status'] = 'paused'
            update_campaign(campaign['id'], campaign)
            print(f"campaign {campaign['name']} was paused")
            return


#9. 12.06 - function recives brandname with no '-' and geo and homepage url with 'www' and fallback homepage url of the same formaT , then generates tracking url for klfix campaigns
def generate_tracking_url(brandName, geo, hp, fbhp):
    if geo in ['gb', 'GB']:
        gep = 'uk'
    track = 'https%3A%2F%2Fshopli.city/raini?rain=https%3A%2F%2Ftrck.shopli.city/7FDKRK?external_id={CLICKID}&cost={CPC}&sub_id_5={SOURCEID}&sub_id_3={url}'+f'sub_id_2={geo}'+f'&sub_id_6={brandName}-{geo}-KL-EC'+f'&sub_id_1=https%3A%2F%2F{fbhp}'
    
    return (track)


#10. 12.06 - function creates klfix campaign in EC
def create_campaignKLfix(brandName, geo, hp, hpfb):
    endpoint = f'https://advertiser.ecomnia.com/create-advertiser-campaign?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}'
    track = generate_tracking_url(brandName, geo, hp, hpfb)
    if geo in ['uk', 'UK']:
        geo = 'GB'
    mid = find_merchant_id_by_name(brandName.replace('-', ''))
    print(f'found mid {mid}')
    print(f'found tracking {track}')
    #domainWL = [f"{hp}", f"{hp[4:]}"]
    domainWL = []
    campaign_settings = {
        "traffictype": "branded",
        "excludecoupon": 'false',
        "ishomepageonly": 'true',
        "name": f"{brandName}-{geo}-kl",
        "url": f"{track}",
        "geo": f"{geo.lower()}",
        #by default all when not sent "os": "android,ios,others,macintosh",
        #by default all when not sent "browser": "chrome,safari,firefox,samsunginternet",
        "dailybudget": 5,
        "dailyclicks": 300,
        "totalbudget": "nolimit",
        "bid": 0.05,
        "status": "active",
        "mid": f"{mid}",
        #"whitelistsources": [],
        "whitelistdomains": domainWL,
        #"cpcbysource": {source_5: 0.008, source_3: 0.002},
        "id": f"{mid}"
    }
    r = requests.post(endpoint, headers=headers, json=campaign_settings)
    print(f"brand {brandName} status code {r.status_code} and geo {geo}")
    return r.json(), {'name': {brandName}, 'status_code': r.status_code}


#11. 15.06 - function recives fname.csv  and bulk creates ec klfix campaigns for it not properly working yet
def bulk_create_campaignsKLfix(fname):
    created = []
    with open(fname, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            brand = row['brandName'].split('.')[0].lower()
            if row['hp'][0:3] == 'www.':
                try:
                    resp = create_campaignKLfix(brand, row['geo'], row['hp'],
                                                row['fbhp'])
                    if resp.status_code == 200:
                        created.append(row)
                        print(
                            f"created {row['brand']} {row['geo']} {row['hp']} {row['fbhp']}"
                        )
                except:
                    print(
                        f"error in {row['brand']} {row['geo']} {row['hp']} {row['fbhp']}"
                    )
            else:
                hp = f"www.{row['hp']}"
                try:
                    resp = create_campaignKLfix(brand, row['geo'], hp,
                                                row['fbhp'])
                    if resp.status_code == 200:
                        created.append(row)
                        print(
                            f"created {row['brandName']} {row['geo']} {row['hp']} {row['fbhp']}"
                        )
                except:
                    print(
                        f"error in {row['brandName']} {row['geo']} {row['hp']} {row['fbhp']}"
                    )
    print(f"created {len(created)} campaigns")

    return created


#11. b 02.01.2026 - function opens a bulk campaign from the sheet and creates them in EC
def bulk_create_campaignsKLfixFromSheet():
    created = []
    sheet = gd.read_sheet_withID(sources_SheetId, 'bulk')
    for row in sheet:
        track = generate_tracking_url(row['brand'].lower(), row['geo'], row['url'], row['hpfb'])
        resp1,resp2 = create_campaignKLfix(row['brand'].lower(), row['geo'], row['url'], row['hpfb'])
        if resp2['status_code'] == 200:
            created.append(row)
            print(f"created {row['brand']} {row['geo']} {row['url']} {row['hpfb']}")
        time.sleep(20)
    track_sheet = gd.read_sheet_withID(sources_SheetId, 'trackExploration')
    updated = []
    for row in track_sheet:
        updated.append(row)
    for campaign in created : 
        updated.append({
                          'campName': f"{campaign['brand'].lower()}-{campaign['geo']}-kl",
                          'campId':'',
                          'status':'',
                          'startBudget':'5',
                          'maxBudget':'5',
                          'potential30days': '0',
                          'explored30': '0',
                          'verify': '[]',
                          'wl': '[]',
                          'CpcLvlUp': 'x',
                          'cpcUpdate': '',
                          'geo': campaign['geo'],
                          'monNetwork': 'kl',  # Note: This appears to be "monNetworl" in the image
                          'monUrl': campaign['url']})
        
    gd.create_or_update_sheet_from_dicts_withId(sources_SheetId, 'trackExploration', updated)
#12. 17.06 - function collects eccomnia cost for affiliations [klfix , sh ]
def get_affiliations_yesterday_cost(aff='all'):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    match aff:
        case 'all':
            campaigns = get_campaigns()
        case 'klfix':
            campaigns = get_campaignsKLFIX()
        case 'sh':
            campaigns = get_campaignS24()
        case 'kl':
            campaigns = get_campaignsKL()
        case _:
            campaigns = []
            print('wrong affiliation')
            return
    cost = 0
    for campaign in campaigns:
        try:
            data = get_campaigns_stats(campaign['id'], yesterday, yesterday)
            if len(data['stats']) > 0:
                cost += data['stats'][0]['spend']
                continue
        except:
            print(f"error in campaign {campaign['name']}")
    return cost


def get_campaigns_stats(campaign_id, start, end):
    endpoint = f"https://report.ecomnia.com/adv-stats-by-date?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtokenNew(secretkey,start,end)}&startdate={start}&enddate={end}&campaignid={campaign_id}"
    r = requests.get(endpoint, headers=headers)
    return r.json()


def get_campaigns_statsBySource(campaign_id, start, end):
    endpoint = f"https://report.ecomnia.com/adv-stats-by-source?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtokenNew(secretkey,start,end)}&startdate={start}&enddate={end}&campaignid={campaign_id}"
    r = requests.get(endpoint, headers=headers)
    return r.json()


#def optimize_30clicks(campaign):
#print(get_affiliations_yesterday_cost('all'))

#print(f"there are {len(get_campaignsKLFIXactiveAproved())} campaigns active and approved")
#bulk_create_campaignsKLfix("bulk1506.csv")
#create_campaignKLfix('agrieuro','de','www.agrieuro.de','www.agrieuro.de')


########################################################################################
######################3 this section is for the EC dashboard ######################
########################################################################################
#23.09 potential sources - recives campaign id and finds all sources that we didn't sample in a campaign DEMO / V@1.0
def potentialSources(id):
    potential_sources = []
    blacklist = get_campaignById('639dcda3-b021-4ecd-9751-1723c9410c3d'
                                 )['campaigns'][0]['blacklistsources']
    print(blacklist)
    data = get_campaigns_statsBySource('639dcda3-b021-4ecd-9751-1723c9410c3d',
                                       '2025-10-21', '2025-10-28')['stats']
    for source in data:
        if source['source'] not in blacklist and source['clicks'] < 30:
            potential_sources.append(source)
    print(f'there are {len(potential_sources)} potential sources')
    return potential_sources


#29.10 potential sources - recives campaign id and finds all sources that we didn't sample in a campaign  in last 7 days DEMO / V@1.0
def potentialSources7days(campId):
    today = datetime.now().strftime('%Y-%m-%d')
    #yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    days7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    potential_sources = []
    blacklist = get_campaignById(campId)['campaigns'][0]['blacklistsources']
    print(blacklist)
    data = get_campaigns_statsBySource(campId, days7, today)['stats']
    for source in data:
        if source['source'] not in blacklist and source['clicks'] < 5:
            potential_sources.append(source)
    print(f'there are {len(potential_sources)} potential sources')
    return potential_sources


#29.10 potential sources 30 days - recives campaign id and finds all sources that we didn't sample in a campaign  in last 30 days returns a list of all sources that we didn't sample 30 clicks from and a list of all sources that we did sample 30 clicks from and didn't get a costume bid
def potentialSources30days(campId, wl):
    today = datetime.now().strftime('%Y-%m-%d')
    #yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    days30 = (datetime.now() - timedelta(days=31)).strftime('%Y-%m-%d')
    potential_sources = []
    need_verification = []
    campData = get_campaignById(campId)['campaigns'][0]
    blacklist = campData['blacklistsources']
    whitelist = wl
    data = get_campaigns_statsBySource(campId, days30, today)['stats']
    for source in data:
        if source['source'] not in blacklist and source[
                'source'] not in whitelist:
            if source['clicks'] < 30:
                potential_sources.append(source)
            else:
                need_verification.append({
                    'source': source['source'],
                    'spend': source['spend'],
                    'clicks': source['clicks']
                })

    print(
        f'there are {len(potential_sources)} potential sources and {len(need_verification)} need verification .'
    )
    if len(data) != 0:
        explored = 100 * (len(blacklist) + len(need_verification) +
                          len(whitelist)) / len(data)
        print(
            f'explored {explored}% of the sources, len of data is {len(data)} len of blacklist is {len(blacklist)} len of need verification is {len(need_verification)} len of whitelist is {len(whitelist)}'
        )

    else:
        explored = 'error'
    return potential_sources, need_verification, explored


#potential = potentialSources('639dcda3-b021-4ecd-9751-1723c9410c3d')
#gd.create_or_update_sheet_from_dicts_withId(sheetid,'potentialSources',potential)


#29.10 function opens the track sheet finds the campaign for each campaign name and updates the track sheet for it. for every campaign it finds the potential sources and calculates % status of the test . the function also blacklists sources with over 30 clicks that didn't get a cpcbysource and updates the track sheet
def update_track_sheet():
    updated = []
    # Load the track sheet
    track_sheet = gd.read_sheet_withID(sheetid, 'trackExploration')
    # Load the campaigns
    campaigns = get_campaigns()
    # Update the track sheet
    for row in track_sheet:
        for campaign in campaigns:
            if campaign['name'] == row['campName']:
                row['campId'] = campaign['id']
                row['status'] = campaign['status']
                while row['wl'].find("'") != -1:
                    row['wl'] = row['wl'].replace("'", '"')
                row['wl'] = json.loads(row['wl'])
                potential = potentialSources30days(campaign['id'], row['wl'])
                row['potential30days'] = len(potential[0])
                row['verify'] = potential[1]
                #if there are sources to verify, blacklist them and update the track sheet
                if len(potential[1]) > 0:
                    logsSheet = gd.read_sheet_withID(sheetid, 'logs')
                    sources = []
                    for item in potential[1]:
                        sources.append(item['source'])
                    response = blackListSources(campaign['id'], sources)
                    logsSheet.append({
                        'campId':
                        campaign['id'],
                        'campName':
                        campaign['name'],
                        'verify':
                        potential[1],
                        'date':
                        datetime.now().strftime('%Y-%m-%d'),
                        'response':
                        response
                    })
                    gd.create_or_update_sheet_from_dicts_withId(
                        sheetid, 'logs', logsSheet)
                campData = get_campaignById(campaign['id'])
                row['explored30'] = potential[2]
                updated.append(row)
                break
    gd.create_or_update_sheet_from_dicts_withId(sheetid, 'trackExploration',
                                                updated)
    return


# 08.11.25 - function gets a campId and a list of sources to blacklist and updates the campaigns settings
def blackListSources(campId, sourcesList):
    campData = get_campaignById(campId)['campaigns'][0]
    blacklist = campData['blacklistsources']
    for source in sourcesList:
        blacklist.append(source)
    campData['blacklistsources'] = blacklist
    response = update_campaign(campId, campData)
    return response

################################################################
################################################################
#those functions are for the increase cpc of the exploration sources

#10.11.25 function gets campaign id and extracts all sources that we didn't buy from in last 7 days and gives them a cpcbysource of 0.05
def exploration_increaseCPCBySource(campId, wl):
    campData = get_campaignById(campId)['campaigns'][0]
    potential7 = potentialSources7days(campId)
    potential30 = potentialSources30days(campId, wl)[0]
    listCPCbySourceCurrent = list(campData['cpcbysource'].keys())
    listCPCbySourceNew = campData['cpcbysource']
    for source in potential30:
        if source['source'] not in listCPCbySourceCurrent:
            listCPCbySourceNew[source['source']] = 0.10
        elif listCPCbySourceNew[source['source']] < 0.25:
            current = listCPCbySourceNew[source['source']]
            listCPCbySourceNew[source['source']] = 0.05 + current
    print(listCPCbySourceNew)
    campData['cpcbysource'] = listCPCbySourceNew
    response = update_campaign(campId, campData)
    logsSheet = gd.read_sheet_withID(sheetid, 'logs')
    logsSheet.append({
        'campId': campId,
        'campName': campData['name'],
        'verify': 'increased cpc for exploration sources',
        'date': datetime.now().strftime('%Y-%m-%d'),
        'response': response
    })
    gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    return

def exploration_IncreaseCPC_fromTrackSheet():
    today = datetime.now().strftime('%Y-%m-%d')
    track_sheet = gd.read_sheet_withID(sheetid, 'trackExploration')
    for row in track_sheet:
        if row['CpcLvlUp'] in ['v', 'V', 'yes', 'Yes', 'YES', 'y', 'Y']:
            if row['cpcUpdate'] != today:
                exploration_increaseCPCBySource(row['campId'], row['wl'])
                row['cpcUpdate'] = today
                gd.create_or_update_sheet_from_dicts_withId(
                    sheetid, 'trackExploration', track_sheet)


################################################################
################################################################
# those functions are for the daily budget management *increasing and resetting
def checkDailySpend(campId,startBudget,max):
    today = datetime.now().strftime('%Y-%m-%d')
    campData = get_campaignById(campId)['campaigns'][0]
    if campData['status'] != 'active':
        return
    try:
      spend = get_campaigns_stats(campId, today,today)['stats'][0]['spend']
    except:
      spend = 0
    daily_budget = campData['daily_budget']
    print(f'{campData["name"]} daily budget {daily_budget}')
    if spend > daily_budget and spend < int(max):
        campData['daily_budget'] = daily_budget + 3
        print(campData)
        response = update_campaign(campId, campData)
        print(response)
        logsSheet = gd.read_sheet_withID(sheetid, 'logs')
        logsSheet.append({
            'campId': campId,
            'campName': campData['name'],
            'verify': f"increased campaign budget from {daily_budget} to {campData['daily_budget']}",
            'date': datetime.now().strftime('%Y-%m-%d'),
            'response': response
        })
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    elif daily_budget != int(startBudget) and spend > int(max) :
        campData['daily_budget'] = int(startBudget)
        response = update_campaign(campId, campData)
        print(response)
        logsSheet = gd.read_sheet_withID(sheetid, 'logs')
        logsSheet.append({
            'campId': campId,
            'campName': campData['name'],
            'verify': f"reset campaign budget after spending {spend} to starting daily {campData['daily_budget']}",
            'date': datetime.now().strftime('%Y-%m-%d'),
            'response': response
        })
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    else :
        pass
    return

def trackSheetDailySpend():
    today = datetime.now().strftime('%Y-%m-%d')
    track_sheet = gd.read_sheet_withID(sheetid, 'trackExploration')
    update_track_sheet()
    for row in track_sheet:
        checkDailySpend(row['campId'],row['startBudget'],row['maxBudget'])

################################################################
################################################################
def checkUnmonExploration():
    track_sheet = gd.read_sheet_withID(sheetid, 'trackExploration')
    for row in track_sheet:
        if row['status'] == 'active':
            monUrl = row['monUrl']
            geo = row['geo']
            if row['monNetwork'] in ['kl','KL','Kl']:
                response = kl.check_monetization(monUrl,geo)
            elif row['monNetwork'] in ['adexa','Adexa','ADEXA','ADEX','adex','Adex']:
                response = True
            else:
                response = True

            if not response:
                pause_campaignWitId(row['campId'])
                print(f"campaign {row['campId']} was paused")
                logsSheet = gd.read_sheet_withID(sheetid, 'logs')
                logsSheet.append({ 'campId': row['campId'], 'campName': row['campName'], 'verify': f"campaign was paused due to unmonetization and monNetwork {row['monNetwork']}", 'date': datetime.now().strftime('%Y-%m-%d'), 'response': response})
                gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    return

def checkUnmonWL():
    track_sheet = gd.read_sheet_withID(sheetid, 'trackWL')
    for row in track_sheet:
        if row['status'] == 'active':
            monUrl = row['monUrl']
            geo = row['geo']
            if row['monNetwork'] in ['kl','KL','Kl']:
                response = kl.check_monetization(monUrl,geo)
            elif row['monNetwork'] in ['adexa','Adexa','ADEXA','ADEX','adex','Adex']:
                response = True
            else:
                response = True
            
            if not response:
                pause_campaignWitId(row['campId'])
                print(f"campaign {row['campId']} was paused")
                logsSheet = gd.read_sheet_withID(sheetid, 'logs')
                logsSheet.append({ 'campId': row['campId'], 'campName': row['campName'], 'verify': f"campaign was paused due to unmonetization and monNetwork {row['monNetwork']}", 'date': datetime.now().strftime('%Y-%m-%d'), 'response': response})
                gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    return




#############################################################################################
############### Those are testing points for trackEXploration sheet functions ###############

#print(update_track_sheet())
#print(blackListSources('639dcda3-b021-4ecd-9751-1723c9410c3d',['source_1','source_2','source_3']))
#print(potentialSources30days('6a8bea4f-f2f1-41d3-a49b-371d9d5a15fb',[]))
#exploration_increaseCPCBySource('b1d98673-45bf-48e0-acd2-69d07cf6a68e',['ryz5h2iisweswovd6zz6bandhu', 'fjyasyjllypzpjsthzt5zstedu'])
#exploration_IncreaseCPC_fromTrackSheet()
#checkDailySpend('db4c3242-ef24-47ea-aa1d-0ea1e0a8eb5c',5,5)
#trackSheetDailySpend()
#checkUnmon()


##################################################################
##################################################################
#functions for track WL sheet
def average_clicks_from_data_list(data):
    today = datetime.now().strftime('%Y-%m-%d')
    sum = 0
    count = 0
    todayClicks = 0
    for day in data :
        if day['date'] != today:
            count += 1
            sum += day['clicks']
        else:
            try:
                todayClicks = day['clicks']
            except:
                todayClicks = 0
    average = sum / count
    return average , todayClicks
    
def average_clicks(campId):
    today = datetime.now().strftime('%Y-%m-%d')
    days30 = (datetime.now() - timedelta(days=31)).strftime('%Y-%m-%d')
    data = get_campaigns_stats(campId, days30, today)['stats']
    average30 , todayClicks = average_clicks_from_data_list(data)
    day7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    data = get_campaigns_stats(campId, day7, today)['stats']
    try:
        yesterday = data[-2]
        yesterdayClicks = yesterday['clicks'] if yesterday['date'] == (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d') else 0
    except:
        yesterdayClicks = 0
    print(yesterdayClicks)
    average7 , todayClicks = average_clicks_from_data_list(data)
    #print(f'average clicks are {average} and today clicks are {todayClicks}')
    return average30 , average7 ,yesterdayClicks, todayClicks
    
def update_trackWLsheet():
    updated = []
    # Load the track sheet
    track_sheet = gd.read_sheet_withID(sheetid, 'trackWL')
    # Load the campaigns
    campaigns = get_campaigns()
    # Update the track sheet
    for row in track_sheet:
        for campaign in campaigns:
            if campaign['name'] == row['campName']:
                row['campId'] = campaign['id']
                row['status'] = campaign['status']
                row['reviewstatus'] = campaign['reviewstatus']
                average30 , average7 ,yesterdayClicks , todayClicks = average_clicks(campaign['id'])
                row['average30'] = average30
                row['average7'] = average7
                row['yesterdayClicks'] = yesterdayClicks
                row['todayClicks'] = todayClicks
                row['lastUpdate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                updated.append(row)
    gd.create_or_update_sheet_from_dicts_withId(sheetid,'trackWL', updated)
    checkUnmonWL()
    return

####################################################################################
############### Those are testing points for trackWL sheet functions ###############
#update_trackWLsheet()
#average_clicks('42cf2936-e692-4efd-b38c-e6e161da1338')



#################################################################################
############# Functions for synching global Black/White lists ####################
#################################################################################
def get_blacklist_sources(geo):
    # Load the track sheet
    track_sheet = gd.read_sheet_withID(sheetid, 'globaList')
    # Load the campaigns
    campaigns = get_campaigns()
    # Update the track sheet
    for row in track_sheet:
        if row['geo'] == geo:
            return row['blacklist']
    return []
    
def get_whitelist_sources(geo):
    # Load the track sheet
    track_sheet = gd.read_sheet_withID(sheetid, 'globaList')
    # Load the campaigns
    campaigns = get_campaigns()
    # Update the track sheet
    for row in track_sheet:
        if row['geo'] == geo:
            return row['whitelist']
    return []
# dictionary black lists has the keys which are source names , each of them is a dictionary with the keys which are geo names and the values are the number of times the source is blacklisted in that geo            
def explorations_blacklist_synch():
    campaigns = get_campaigns()
    blackLists = {}
    for campaign in campaigns:
        print(campaign['name'])
        black = campaign['blacklistsources']
        for source in black:
            if source not in blackLists:
                blackLists[source] = {campaign['geo']: 1}
            else:
                if campaign['geo'] not in blackLists[source].keys():
                    val = blackLists[source]
                    val[campaign['geo']] = 1
                    blackLists[source] = val
                else:
                    blackLists[source][campaign['geo']] += 1
    
    print(f'len of BlackLists is {len(blackLists)}')
    finalBlackLists = []
    finalByGeoBlacklist = {}
    for item in blackLists.keys():
        sum = 0 
        for geo in blackLists[item].keys():
            sum += blackLists[item][geo]
            if blackLists[item][geo] > 3:
                temp = finalByGeoBlacklist[geo] if geo in finalByGeoBlacklist.keys() else []
                temp.append(item)
                finalByGeoBlacklist[geo] = temp
        if sum > 5:
            finalBlackLists.append(item)
    print(f'len of finalBlackLists is {len(finalBlackLists)}')
    print(f'len of finalByGeoBlacklist is {len(finalByGeoBlacklist)}')
    print(finalByGeoBlacklist)

    for campaign in campaigns:
        if campaign['geo'] in finalByGeoBlacklist.keys() and campaign['name'].split('-')[-1] != 'wl':
            addToBlacklist = []
            for source in finalByGeoBlacklist[campaign['geo']]:
                if source not in campaign['blacklistsources']:
                    addToBlacklist.append(source)
            blackListSources(campaign['id'], addToBlacklist)
            print(f'added {len(addToBlacklist)} sources to {campaign["name"]}')
    return finalBlackLists

#explorations_blacklist_synch()
#checkUnmonExploration()

#exploration_IncreaseCPC_fromTrackSheet()