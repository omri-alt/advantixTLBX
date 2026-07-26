import logging
import os
import requests
import hashlib
from datetime import datetime, timedelta
import csv
import time
import json
from typing import Any, Dict, List, Optional, Tuple
from integrations.autoserver import kl_as as kl
from integrations.autoserver import gdocs_as as gd

logger = logging.getLogger(__name__)

headers = {"Content-Type": "application/json"}
feedid = '22974eb2-a9b8-4eb8-a0cf-735538fff4ea_self'
sheetid = (
    os.getenv("EC_SHEETS_SPREADSHEET_ID")
    or "1-kclsSvR7LUtpi-Ymrd9wRYbbmkraP2tGLTrvSnih9c"
).strip()
sources_SheetId = sheetid
advkey = os.getenv("ECadvKey")
authkey = os.getenv("ECauthKey")
secretkey = os.getenv("ECsecretKey")

TAB_EXPLORATION = "trackExploration"
TAB_WL = "trackWL"

# Required / bootstrap columns for EC exploration (legacy sheet may have more).
HEADERS_EXPLORATION = [
    "campName",
    "campId",
    "status",
    "wl",
    "potential30days",
    "verify",
    "explored30",
    "budgetReachedYesterday",
    "monUrl",
    "monNetwork",
    "geo",
    "CpcLvlUp",
    "cpcUpdate",
    "startBudget",
    "maxBudget",
    "skipUnmon",
]
HEADERS_WL = [
    "campName",
    "campId",
    "status",
    "reviewstatus",
    "average30",
    "average7",
    "yesterdayClicks",
    "todayClicks",
    "lastUpdate",
    "budgetReachedYesterday",
    "monUrl",
    "monNetwork",
    "geo",
    "skipUnmon",
]


def _truthy_skip_unmon(raw: Any) -> bool:
    s = str(raw or "").strip().lower()
    return s in ("1", "true", "yes", "y", "v", "skip", "x")


def _extract_campaign_id_from_create(body: Any) -> str:
    """Best-effort campaign id from create-advertiser-campaign JSON."""
    if not isinstance(body, dict):
        return ""
    for key in ("id", "campaign_id", "campaignId", "campId"):
        v = body.get(key)
        if v is not None and str(v).strip() and str(v).strip().lower() not in ("none", "null"):
            # create payload also echoes merchant mid as ``id`` — prefer UUID-like / longer ids
            s = str(v).strip()
            if len(s) >= 20 or "-" in s:
                return s
    camps = body.get("campaigns")
    if isinstance(camps, list) and camps:
        c0 = camps[0]
        if isinstance(c0, dict):
            for key in ("id", "campaign_id", "campaignId"):
                v = c0.get(key)
                if v is not None and str(v).strip():
                    return str(v).strip()
    data = body.get("data")
    if isinstance(data, dict):
        return _extract_campaign_id_from_create(data)
    return ""


def exploration_row_from_bulk_sheet_row(
    item: Dict[str, str],
    *,
    camp_name: str,
    camp_id: str = "",
    mon_network: str = "kl",
    start_budget: str = "5",
    max_budget: str = "5",
) -> Dict[str, str]:
    """
    Build one ``trackExploration`` row after an EC bulk-open create.

    ``item`` is a bulk input row (``brand``, ``geo``, ``url``, ``hpfb``).
    ``camp_name`` must match the EC campaign name created by the bulk opener.
    """
    geo = str(item.get("geo") or "").strip().lower()[:2]
    url = str(item.get("url") or "").strip()
    if url and not url.lower().startswith("http"):
        url = f"https://{url.lstrip('/')}"
    return {
        "campName": str(camp_name or "").strip(),
        "campId": str(camp_id or "").strip(),
        "status": "active",
        "wl": "[]",
        "potential30days": "0",
        "verify": "[]",
        "explored30": "0",
        "budgetReachedYesterday": "",
        "monUrl": url,
        "monNetwork": (mon_network or "kl").strip().lower(),
        "geo": geo,
        "CpcLvlUp": "x",
        "cpcUpdate": "",
        "startBudget": str(start_budget),
        "maxBudget": str(max_budget),
        "skipUnmon": "",
    }


def append_ec_exploration_tracking_rows(rows: List[Dict[str, Any]]) -> Tuple[int, str]:
    """
    Append rows to ``trackExploration`` for campaign names / ids not already present.

    Preserves existing sheet columns; ensures ``HEADERS_EXPLORATION`` exist.
    Returns ``(added_count, error_message)``.
    """
    sid = (sheetid or "").strip()
    if not sid:
        return 0, "EC_SHEETS_SPREADSHEET_ID is not set"
    if not rows:
        return 0, ""
    try:
        gd.append_missing_headers_row1(sid, TAB_EXPLORATION, HEADERS_EXPLORATION, create_if_missing=False)
        data = gd.read_sheet_withID(sid, TAB_EXPLORATION) or []
    except Exception as e:
        logger.exception("append_ec_exploration_tracking_rows: read failed")
        return 0, str(e)

    all_keys: List[str] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        for k in r.keys():
            if k not in all_keys:
                all_keys.append(str(k))
    for h in HEADERS_EXPLORATION:
        if h not in all_keys:
            all_keys.append(h)

    existing_names = {
        str(r.get("campName") or "").strip().lower()
        for r in data
        if isinstance(r, dict) and str(r.get("campName") or "").strip()
    }
    existing_ids = {
        str(r.get("campId") or "").strip()
        for r in data
        if isinstance(r, dict) and str(r.get("campId") or "").strip()
    }

    added = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("campName") or "").strip()
        cid = str(raw.get("campId") or "").strip()
        if not name:
            continue
        if name.lower() in existing_names:
            continue
        if cid and cid in existing_ids:
            continue
        row_out = {k: "" for k in all_keys}
        for k, v in raw.items():
            if k not in row_out:
                all_keys.append(k)
                for prev in data:
                    if isinstance(prev, dict) and k not in prev:
                        prev[k] = ""
                row_out[k] = ""
            row_out[k] = "" if v is None else str(v)
        data.append(row_out)
        existing_names.add(name.lower())
        if cid:
            existing_ids.add(cid)
        added += 1

    if not added:
        return 0, ""

    normalized: List[Dict[str, Any]] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        nr = {k: ("" if r.get(k) is None else r.get(k)) for k in all_keys}
        # Keep wl / verify JSON-ish strings; lists from prior hourlies become str later.
        for listish in ("wl", "verify"):
            v = nr.get(listish)
            if isinstance(v, (list, dict)):
                nr[listish] = json.dumps(v, ensure_ascii=False)
        normalized.append(nr)

    try:
        gd.create_or_update_sheet_from_dicts_withId(sid, TAB_EXPLORATION, normalized)
    except Exception as e:
        logger.exception("append_ec_exploration_tracking_rows: write failed")
        return 0, str(e)
    return added, ""


def _ec_monetization_check(mon_network: str, mon_url: str, geo: str) -> Tuple[Optional[bool], Optional[str]]:
    """Same multi-feed unmon probe as SK exploration (kl / feeds / yadore / adexa / new / skip)."""
    from integrations.autoserver.sk_optimizer import _monetization_for_network

    return _monetization_for_network(mon_network, mon_url, geo)


def _resolve_ec_mon_url(row: Dict[str, Any], camp_url: str = "") -> str:
    u = (row.get("monUrl") or row.get("monURL") or "").strip()
    if u:
        return u
    from integrations.autoserver.sk_optimizer import _hp_from_tracking_url

    hp = _hp_from_tracking_url(str(camp_url or ""))
    return (hp or "").strip()


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
    base = 'https://shopli.city/raini?rain=https%3A%2F%2Fdighlyconsive.com/0039c33f-0b64-41f0-8b5f-fa948ece4d4a?&click_id={clickid}&adv_price={cpc}&sub_id={sourceid}&traffic_type=07&' + f'geo={geo.lower()}'
    brand = f'{brandName}-{geo}-KLFIX-EC'
    hp = f'https%3A%2F%2F{fbhp}&oadest=https%3A%2F%2F{hp}'
    tracking_url = f'{base}&brand={brand}&hp={hp}'
    return (tracking_url)


#10. 12.06 - function creates klfix campaign in EC
def create_campaignKLfix(brandName, geo, hp, hpfb):
    endpoint = f'https://advertiser.ecomnia.com/create-advertiser-campaign?advertiserkey={advkey}&authkey={authkey}&authtoken={generate_authtoken(secretkey)}'
    track = generate_tracking_url(brandName, geo, hp, hpfb)
    if geo in ['uk', 'UK']:
        geo = 'GB'
    mid = find_merchant_id_by_name(brandName.replace('-', ''))
    domainWL = [f"{hp}", f"{hp[4:]}"]
    campaign_settings = {
        "traffictype": "branded",
        "excludecoupon": 'false',
        "ishomepageonly": 'true',
        "name": f"{brandName}-{geo}-KLFIX",
        "url": f"{track}",
        "geo": f"{geo.lower()}",
        #by default all when not sent "os": "android,ios,others,macintosh",
        #by default all when not sent "browser": "chrome,safari,firefox,samsunginternet",
        "dailybudget": 5,
        "dailyclicks": 200,
        "totalbudget": "nolimit",
        "bid": 0.01,
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
    #print(blacklist)
    data = get_campaigns_statsBySource('639dcda3-b021-4ecd-9751-1723c9410c3d',
                                       '2025-10-21', '2025-10-28')['stats']
    for source in data:
        if source['source'] not in blacklist and source['clicks'] < 30:
            potential_sources.append(source)
    #print(f'there are {len(potential_sources)} potential sources')
    return potential_sources


#29.10 potential sources - recives campaign id and finds all sources that we didn't sample in a campaign  in last 7 days DEMO / V@1.0
def potentialSources7days(campId):
    today = datetime.now().strftime('%Y-%m-%d')
    #yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    days7 = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
    potential_sources = []
    blacklist = get_campaignById(campId)['campaigns'][0]['blacklistsources']
    #print(blacklist)
    data = get_campaigns_statsBySource(campId, days7, today)['stats']
    for source in data:
        if source['source'] not in blacklist and source['clicks'] < 25:
            potential_sources.append(source)
    #print(f'there are {len(potential_sources)} potential sources')
    return potential_sources


#29.10 potential sources 30 days - recives campaign id and finds all sources that we didn't sample in a campaign  in last 30 days returns a list of all sources that we didn't sample 30 clicks from and a list of all sources that we did sample 30 clicks from and didn't get a costume bid
def potentialSources30days(campId , wl):
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
        if source['source'] not in blacklist and source['source'] not in whitelist:
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
        print(f'explored {explored}% of the sources, len of data is {len(data)} len of blacklist is {len(blacklist)} len of need verification is {len(need_verification)} len of whitelist is {len(whitelist)}')

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
        matched = False
        for campaign in campaigns:
            if campaign['name'] == row['campName']:
                row['campId'] = campaign['id']
                prev_status = str(row.get('status') or '').strip().lower()
                api_status = str(campaign.get('status') or '').strip()
                # Keep operator-facing paused-unmon when EC API still reports paused.
                if prev_status == 'paused-unmon' and api_status.lower() == 'paused':
                    row['status'] = 'paused-unmon'
                else:
                    row['status'] = api_status
                while row['wl'].find("'") != -1 :
                    row['wl'] = row['wl'].replace("'", '"')
                row['wl'] = json.loads(row['wl'])
                potential = potentialSources30days(campaign['id'],row['wl'])
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
                try:
                    row["budgetReachedYesterday"] = check_budget_reached_yesterday_EC(campaign["id"])
                except Exception:
                    row["budgetReachedYesterday"] = "No"
                updated.append(row)
                matched = True
                break
        if not matched:
            # Keep bulk-registered rows until campName matches an EC campaign.
            updated.append(row)
    gd.create_or_update_sheet_from_dicts_withId(sheetid, 'trackExploration',
                                                updated)
    return updated


def update_trackWLsheet():
    updated = []
    # Load the track sheet
    track_sheet = gd.read_sheet_withID(sheetid, 'trackWL')
    # Load the campaigns
    campaigns = get_campaigns()
    # Update the track sheet
    for row in track_sheet:
        matched = False
        for campaign in campaigns:
            if campaign['name'] == row['campName']:
                row['campId'] = campaign['id']
                prev_status = str(row.get('status') or '').strip().lower()
                api_status = str(campaign.get('status') or '').strip()
                if prev_status == 'paused-unmon' and api_status.lower() == 'paused':
                    row['status'] = 'paused-unmon'
                else:
                    row['status'] = api_status
                row['reviewstatus'] = campaign['reviewstatus']
                average30 , average7 ,yesterdayClicks , todayClicks = average_clicks(campaign['id'])
                row['average30'] = average30
                row['average7'] = average7
                row['yesterdayClicks'] = yesterdayClicks
                row['todayClicks'] = todayClicks
                row['lastUpdate'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                try:
                    row["budgetReachedYesterday"] = check_budget_reached_yesterday_EC(campaign["id"])
                except Exception:
                    row["budgetReachedYesterday"] = "No"
                updated.append(row)
                matched = True
                break
        if not matched:
            updated.append(row)
    gd.create_or_update_sheet_from_dicts_withId(sheetid,'trackWL', updated)
    checkUnmonWL()
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


def whiteListSources(campId, sourcesList):
    """Append sources to campaign ``whitelistsources`` (deduped), then PUT full campaign."""
    campData = get_campaignById(campId)["campaigns"][0]
    wl = list(campData.get("whitelistsources") or [])
    existing = {str(x).strip() for x in wl if str(x).strip()}
    for source in sourcesList:
        s = str(source or "").strip()
        if s and s not in existing:
            wl.append(s)
            existing.add(s)
    campData["whitelistsources"] = wl
    return update_campaign(campId, campData)


def reactivate_sources_ec(
    campId,
    sourcesList,
    *,
    target_bid: float = 0.10,
    also_whitelist: bool = True,
):
    """
    Reactivate EC sources after blacklist / zero CPC.

    Unlike SK (bidFactor), EC uses absolute ``cpcbysource`` bids and an explicit
    ``blacklistsources`` list. This removes each source from the blacklist and sets
    ``cpcbysource[source] = target_bid``. Optionally appends to ``whitelistsources``.
    """
    campData = get_campaignById(campId)["campaigns"][0]
    blacklist = [str(x).strip() for x in (campData.get("blacklistsources") or []) if str(x).strip()]
    bl_set = set(blacklist)
    cpc = dict(campData.get("cpcbysource") or {})
    wl = list(campData.get("whitelistsources") or [])
    wl_set = {str(x).strip() for x in wl if str(x).strip()}
    bid = float(target_bid) if target_bid and float(target_bid) > 0 else 0.10
    touched: List[str] = []
    for source in sourcesList:
        s = str(source or "").strip()
        if not s:
            continue
        if s in bl_set:
            bl_set.discard(s)
        cpc[s] = bid
        if also_whitelist and s not in wl_set:
            wl.append(s)
            wl_set.add(s)
        touched.append(s)
    if not touched:
        return {"ok": True, "reactivated": [], "response": None}
    campData["blacklistsources"] = [x for x in blacklist if x in bl_set]
    campData["cpcbysource"] = cpc
    if also_whitelist:
        campData["whitelistsources"] = wl
    response = update_campaign(campId, campData)
    return {"ok": True, "reactivated": touched, "target_bid": bid, "response": response}


def activate_campaignWitId(campaign_id):
    """Set campaign ``status`` to ``active`` (inverse of ``pause_campaignWitId``)."""
    campaigns = get_campaigns()
    for campaign in campaigns:
        if campaign["id"] == campaign_id:
            campaign["status"] = "active"
            update_campaign(campaign["id"], campaign)
            print(f"campaign {campaign['name']} was activated")
            return
    logger.warning("activate_campaignWitId: campaign %s not found", campaign_id)

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
def check_budget_reached_yesterday_EC(campaign_id):
    """
    EC daily cap vs yesterday spend (field names from existing EC module usage;
    align with ``get_campaignById`` + ``get_campaigns_stats``):

    - Daily cap: ``daily_budget`` on each campaign object (``get_campaignById`` → ``campaigns[0]['daily_budget']``;
      creation payloads sometimes use ``dailybudget`` — we read both).
    - Yesterday spend: ``get_campaigns_stats(campaign_id, y, y)`` → ``stats[0]['spend']`` when a row exists,
      else spend treated as ``0``.

    Returns ``Yes``, ``No``, or ``No limit`` (no cap / zero / missing).
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        camp_block = get_campaignById(campaign_id)
        camp_data = (camp_block.get("campaigns") or [{}])[0]
    except Exception:
        return "No"
    raw = camp_data.get("daily_budget", camp_data.get("dailybudget"))
    try:
        cap = float(raw) if raw is not None and str(raw).strip() != "" else 0.0
    except (TypeError, ValueError):
        cap = 0.0
    if cap is None or cap <= 0:
        return "No limit"
    try:
        st = get_campaigns_stats(campaign_id, yesterday, yesterday).get("stats") or []
        spend = float(st[0]["spend"]) if st else 0.0
    except Exception:
        spend = 0.0
    if spend >= cap:
        return "Yes"
    return "No"


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
def _campaign_tracking_url(camp_id: Any) -> str:
    """Best-effort tracking URL from EC get-advertiser-campaign(s) for hp= fallback."""
    if not camp_id:
        return ""
    try:
        data = get_campaignById(camp_id)
    except Exception:
        return ""
    if isinstance(data, list) and data:
        data = data[0]
    if not isinstance(data, dict):
        return ""
    camps = data.get("campaigns")
    if isinstance(camps, list) and camps and isinstance(camps[0], dict):
        data = camps[0]
    for key in ("trackingurl", "tracking_url", "trackingUrl", "url"):
        v = data.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def checkUnmonExploration():
    """
    Pause active exploration campaigns that fail monetization for their ``monNetwork``.

    Uses the same multi-feed probes as SK (``kl``, ``feed1``/``2``/``5``, Yadore, Adexa).
    ``new`` / ``skip`` / ``skipUnmon`` skip the pause. Sets sheet status ``paused-unmon``.
    """
    track_sheet = gd.read_sheet_withID(sheetid, 'trackExploration')
    changed = False
    for row in track_sheet:
        status = str(row.get('status') or '').strip().lower()
        if status != 'active':
            continue
        if _truthy_skip_unmon(row.get('skipUnmon')):
            continue
        camp_id = row.get('campId')
        mon_url = _resolve_ec_mon_url(row, _campaign_tracking_url(camp_id) if not (row.get('monUrl') or row.get('monURL')) else "")
        geo = str(row.get('geo') or '').strip()
        net = str(row.get('monNetwork') or 'kl').strip()
        if not mon_url:
            logger.warning(
                "EC unmon skip %s: empty monUrl",
                row.get('campName') or row.get('campId'),
            )
            continue
        mon_ok, err_tag = _ec_monetization_check(net, mon_url, geo)
        if err_tag in ('error', 'skip_unmon'):
            continue
        if mon_ok is not False:
            continue
        if not camp_id:
            continue
        pause_campaignWitId(camp_id)
        row['status'] = 'paused-unmon'
        changed = True
        print(f"campaign {camp_id} was paused (unmon / monNetwork={net})")
        logsSheet = gd.read_sheet_withID(sheetid, 'logs')
        logsSheet.append({
            'campId': camp_id,
            'campName': row.get('campName'),
            'verify': f"campaign was paused due to unmonetization and monNetwork {net}",
            'date': datetime.now().strftime('%Y-%m-%d'),
            'response': mon_ok,
        })
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    if changed:
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'trackExploration', track_sheet)
    return


def checkUnmonWL():
    """Pause active WL campaigns that fail monetization (same probe rules as exploration)."""
    track_sheet = gd.read_sheet_withID(sheetid, 'trackWL')
    changed = False
    for row in track_sheet:
        status = str(row.get('status') or '').strip().lower()
        if status != 'active':
            continue
        if _truthy_skip_unmon(row.get('skipUnmon')):
            continue
        camp_id = row.get('campId')
        mon_url = _resolve_ec_mon_url(row, _campaign_tracking_url(camp_id) if not (row.get('monUrl') or row.get('monURL')) else "")
        geo = str(row.get('geo') or '').strip()
        net = str(row.get('monNetwork') or 'kl').strip()
        if not mon_url:
            continue
        mon_ok, err_tag = _ec_monetization_check(net, mon_url, geo)
        if err_tag in ('error', 'skip_unmon'):
            continue
        if mon_ok is not False:
            continue
        if not camp_id:
            continue
        pause_campaignWitId(camp_id)
        row['status'] = 'paused-unmon'
        changed = True
        print(f"campaign {camp_id} was paused (unmon / monNetwork={net})")
        logsSheet = gd.read_sheet_withID(sheetid, 'logs')
        logsSheet.append({
            'campId': camp_id,
            'campName': row.get('campName'),
            'verify': f"campaign was paused due to unmonetization and monNetwork {net}",
            'date': datetime.now().strftime('%Y-%m-%d'),
            'response': mon_ok,
        })
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    if changed:
        gd.create_or_update_sheet_from_dicts_withId(sheetid, 'trackWL', track_sheet)
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
    try:
        average = sum / count
    except:
        average = 0
    return average , todayClicks
    track_sheet = gd.read_sheet_withID(sheetid, 'trackWL')
    for row in track_sheet:
        if row['status'] == 'active':
            monUrl = row['monUrl']
            geo = row['geo']
            response = kl.check_monetization(monUrl,geo)
            if kl.check_monetization(monUrl,geo) == False:
                pause_campaignWitId(row['campId'])
                print(f"campaign {row['campId']} was paused")
                logsSheet = gd.read_sheet_withID(sheetid, 'logs')
                logsSheet.append({ 'campId': row['campId'], 'campName': row['campName'], 'verify': 'campaign was paused due to unmonetization', 'date': datetime.now().strftime('%Y-%m-%d'), 'response': response})
                gd.create_or_update_sheet_from_dicts_withId(sheetid, 'logs', logsSheet)
    return

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