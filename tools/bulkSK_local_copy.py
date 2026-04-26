# to use the bulk open campaign add the file name without .csv to the last line of the file and run it, the inputs file should contain the following columns: brand, geo, homepage url for adv creation , hp fallback link that will be inserted in tracking url , category
import sk as sk
import csv
import datetime
import urllib.parse
import time
import gdocs as gd

KLcampset = {
    "name": "name",
    "start": "today",
    "dailyBudget": "25.0",
    "cpc": "0.05",
    "trackingUrl": "trackingUrl",
    "advertiserId": "advID",
    "allowDeepLink": "true",
    "geoTargeting": [""],
    "partnerChannels": [
        "1",
        "2",
        "3",
        "5",
        "6",
        "8",
        "9",
        "12",
        "13",
        "14",
        "15",
        "16",
    ],  # no push , native , adnetwork
    "strategyId": 3,
}
# https://shopli.city/raini?rain=https://trck.shopli.city/7FDKRK?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}&sub_id_2=XgeoX&sub_id_6=XbrandX&sub_id_1=XhpX&sub_id_3={oadest}


# 1. 15.06 - function recives brand name , geo and homepage url and generates tracking url for SK klfix campaigns
def create_KLlinkSKglobal(brand, geo, hp):
    base = "https://shopli.city/raini?rain=https://trck.shopli.city/7FDKRK?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}&sub_id_3={oadest}"
    macros = (
        f"sub_id_2={geo.lower()}&sub_id_6={brand}-{geo.upper()}-KLFIX-SK&sub_id_1={hp}"
    )
    link = f"{base}&{macros}"
    return link


# 1. 17.06 - function recives brand name , geo and homepage url and the affiliation prefix to use in the brand macro, and generates tracking url for SK klfix campaigns
def create_KLlinkSKglobalDynamicPrefix(brand, geo, hp, prefix):
    base = "hhttps://shopli.city/raini?rain=https://trck.shopli.city/7FDKRK?external_id={clickid}&cost={adv_price}&sub_id_4={traffic_type}&sub_id_5={sub_id}&sub_id_3={oadest}"
    macros = f"sub_id_2={geo.lower()}&sub_id_6={brand}-{geo.upper()}-{prefix}-SK&sub_id_1={hp}"
    link = f"{base}&{macros}"
    return link


def encode_url(url):
    encoded_url = url.replace(" ", "%20")
    return encoded_url


# 3. 15.06 - function recives brand name and strips it from spaces and '-' and .registrar , 'www' and returns the brand name
def strip_brandName(brandName):
    striped = brandName.replace(" ", "")
    striped = striped.replace("-", "")
    match len(striped.split(".")):
        case 1:
            return striped
        case 2:
            return striped.split(".")[0]
        case _:
            return striped.split(".")[-2]


# 4. 15.06 - function recives fname.csv and bulk creates sk klfix campaigns for it and returns a list of all created campaigns
def bulk_open_fixcampaignsGlob(fname, klfixSettings=KLcampset):
    completed = []
    with open(f"{fname}.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        bulk = [row for row in reader]  # Convert to list of dicts
        for item in bulk:
            campset = klfixSettings
            if item["geo"] in ["uk", "UK"]:
                campset["geoTargeting"] = ["GB"]
            else:
                campset["geoTargeting"] = [item["geo"].upper()]
            brand = strip_brandName(item["brand"])
            advID = sk.new_advertiser(
                f"{brand}-{item['geo'].upper()}-KLFIX", item["url"], item["category"]
            )
            time.sleep(4)
            campset["advertiserId"] = advID
            campset["name"] = f"{brand}FIX-{item['geo'].upper()}-all"
            campset["start"] = datetime.datetime.now().strftime("%Y-%m-%d")  # today
            camp_url = create_KLlinkSKglobal(brand, item["geo"].lower(), item["hp"])
            campset["trackingUrl"] = encode_url(camp_url)
            camp = sk.new_campaign(campset)
            completed.append(camp.json())
            time.sleep(4)
            completed.append(camp)
    return completed


# 4b. 17.06 - function recives fname.csv and bulk creates sk prefix campaigns for it and returns a list of all created campaigns
def bulk_open_KLWLcampaignsGlob(fname, klfixSettings=KLcampset):
    completed = []
    with open(f"{fname}.csv", "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        bulk = [row for row in reader]  # Convert to list of dicts
        for item in bulk:
            campset = klfixSettings
            campset["geoTargeting"] = [item["geo"].upper()]
            brand = strip_brandName(item["brand"])
            advID = sk.new_advertiser(
                f"{brand}-{item['geo'].upper()}-{prefix}", item["url"], item["category"]
            )
            time.sleep(4)
            campset["advertiserId"] = advID
            campset["name"] = f"{brand}FIX-{item['geo'].upper()}-all"
            campset["start"] = datetime.datetime.now().strftime("%Y-%m-%d")  # today
            camp_url = create_KLlinkSKglobalDynamicPrefix(
                brand, item["geo"].lower(), item["hp"], "KLFLEX"
            )
            campset["trackingUrl"] = encode_url(camp_url)
            if item["costumeCpc"] != "":
                campset["cpc"] = item["costumeCpc"]
            camp = sk.new_campaign(campset)
            completed.append(camp.json())
            time.sleep(4)
            completed.append(camp)
    return completed


# 5 30.06 - new bulk opener for KLfix global using the gdocs api
def bulk_open_fixcampaignsGlob2(fname, klfixSettings=KLcampset):
    completed = []
    bulk = gd.read_sheet(fname)
    for item in bulk:
        campset = klfixSettings
        if item["geo"] in ["uk", "UK"]:
            campset["geoTargeting"] = ["GB"]
        else:
            campset["geoTargeting"] = [item["geo"].upper()]
        brand = strip_brandName(item["brand"])
        advID = sk.new_advertiser(
            f"{brand}-{item['geo'].upper()}-KLFIX", item["url"], item["category"]
        )
        time.sleep(4)
        campset["advertiserId"] = advID
        campset["name"] = f"{brand}FIX-{item['geo'].upper()}-all"
        campset["start"] = datetime.datetime.now().strftime("%Y-%m-%d")  # today
        camp_url = create_KLlinkSKglobal(brand, item["geo"].lower(), item["hpfb"])
        campset["trackingUrl"] = encode_url(camp_url)
        camp = sk.new_campaign(campset)
        completed.append(camp.json())
        time.sleep(4)
    return


# 5b 30.06 - new bulk opener for prefix KL campaigns global using the gdocs api
def bulk_open_KLprefixCampaignsGlob2(fname, prefix, klfixSettings=KLcampset):
    completed = []
    bulk = gd.read_sheet(fname)
    for item in bulk:
        campset = klfixSettings
        if item["geo"] in ["uk", "UK"]:
            campset["geoTargeting"] = ["GB"]
        else:
            campset["geoTargeting"] = [item["geo"].upper()]
        brand = strip_brandName(item["brand"])
        advID = sk.new_advertiser(
            f"{brand}-{item['geo'].upper()}-{prefix}", item["url"], item["category"]
        )
        time.sleep(4)
        campset["advertiserId"] = advID
        campset["name"] = f"{brand}{prefix}-{item['geo'].upper()}-all"
        campset["start"] = datetime.datetime.now().strftime("%Y-%m-%d")  # today
        camp_url = create_KLlinkSKglobalDynamicPrefix(
            brand, item["geo"].lower(), item["hpfb"], prefix
        )
        campset["trackingUrl"] = encode_url(camp_url)
        camp = sk.new_campaign(campset)
        completed.append(camp.json())
        time.sleep(4)
    return


print("---------------------------------------")
# insert the file name without .csv to start running the bulk open campaigns
# bulk_open_fixcampaignsGlob2('sk1506', KLcampset )

# bulk_open_TrackCampaignsGlob('2009-test.csv', KLcampset , 'KLFLEX')
# bulk_open_fixcampaignsGlob('18.06_bulkup', KLcampset)
# print(create_KLlinkSKglobalDynamicPrefix('keskisenkello','fi','https://www.keskisenkello.fi','KLWL'))

bulk_open_fixcampaignsGlob2("bulkSK-KLFIX", klfixSettings=KLcampset)
