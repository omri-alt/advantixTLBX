import os
import urllib.parse
import requests
from integrations.autoserver import gdocs_as as gd
from integrations.autoserver import kl_as as kl
from datetime import datetime, timedelta
import time

ZPkey = (os.environ.get("keyZP") or os.getenv("KEYZP") or "").strip()
headers = {
    "api-token": f"{ZPkey}",
    'accept': 'application/json',
}
base_url = "http://panel.zeropark.com"
sheetId = (
    os.getenv("BLEND_SHEETS_SPREADSHEET_ID")
    or "1h9lBPTREEJO9VVvj6wctCgCOn3YcwJBGIk_MBwXw-xY"
).strip()


def encode_url(url: str, params: dict) -> str:
  encoded_url = urllib.parse.quote(
      url, safe='')  # Encode entire URL, including https://
  encoded_params = urllib.parse.urlencode(params)
  return f"{encoded_url}%3F{encoded_params}" if params else encoded_url


def campaign_details(id):
  endpoint = base_url + f'/api/v2/campaigns/{id}'
  r = requests.get(endpoint, headers=headers)
  print(r.json())
  return r.json()


def campaigns_data_domainYest():
  endpoint2 = "https://panel.zeropark.com/api/stats/campaign/domain?interval=YESTERDAY&page=0&limit=100&tagIdsFilter=ANY_OF"
  r = requests.get(endpoint2, headers=headers)
  return r.json()['elements']


def campaigns_data_domainToday():
  endpoint2 = "https://panel.zeropark.com/api/stats/campaign/domain?interval=TODAY&page=0&limit=100&tagIdsFilter=ANY_OF"
  r = requests.get(endpoint2, headers=headers)
  return r.json()['elements']


#13.07 campaign_details('f43bb1d0-4da9-11f0-aa2b-12df15f19bdf')
def yesterday_mehilot():
  list = []
  camps = campaigns_data_domainYest()
  print(camps[0].keys())
  for key in camps[0].keys():
    print(key)
    try:
      print(f"{key} keys are {camps[0][key].keys()}")
    except:
      pass

  cost = 0
  for camp in camps:
    availableTraffic = camp['stats'].get('availableVisits', 'none')
    hp = camp['details']['url'].split('oadest=')[1]
    monetized = ''
    geo = camp['details']['geo'].lower()
    if geo == 'gb':
      geo = 'uk'
    if hp[0:5] == 'https':
      try:
        monetized = kl.check_monetization(hp, geo)['result']
      except:
        monetized = f'error occured hp = {hp} , geo = {geo}'
    list.append({
        'name': camp['details']['name'],
        'cost': camp['stats']['spent'],
        'id': camp['details']['id'],
        'visits': camp['stats']['redirects'],
        'availableTraffic': availableTraffic,
        'bid': camp['details']['bid'],
        'hpUrl': hp,
        'geo': geo,
        'monetized': monetized
    })
    cost = cost + camp['stats']['spent']
  print(f'Total cost yesterday: {cost}')

  gd.create_or_update_sheet_from_dicts_withId(sheetId, 'YestMehilot', list)
  return cost

# function recives campaign id and pauses it using zp api
def pause_campaign(id):
  endpoint = f'https://panel.zeropark.com/api/campaign/{id}/pause'
  r = requests.post(endpoint, headers=headers)
  if r.status_code != 200:
    print(f"error in pausing campaign {id} -- response is {r.json()}")
  #print(r.json())
  return

#13.07 function recives campaign id and activates it using zp api
def activate_campaign(id):
  endpoint = f'https://panel.zeropark.com/api/campaign/{id}/resume'
  r = requests.post(endpoint, headers=headers)
  #print(r.json())
  return

#13.07 function goes over the zp plan sheet and checks if the clicks today are higher than the clicks pause limit and pauses the campaign if so , additionaly if there is a paused campaign and the clicks today are lower than the clicks pause limit it activates the campaign
def mehilot():
  updated = []
  now = datetime.now().strftime('%Y-%m-%d -- %H:%M')
  sheet = gd.read_sheet_withID(sheetId, 'Plan')
  camps = campaigns_data_domainToday()
  #print(camps[0]['details'].keys())
  for plan in sheet:
    for camp in camps:
      if camp['details']['name'].split('-')[0] == plan['brand']:
        plan['state'] = camp['details']['state']['state']
        plan[
            'url'] = "https://advertiser-panel.zeropark.com/dashboard/campaign/" + camp[
                'details']['id']
        plan['lastUpdate'] = now
        plan['bid'] = camp['details']['bid']
        plan['clicksToday'] = camp['stats']['redirects']
        plan['monStatus'] = kl.check_monetizationWithResp(
            camp['details']['url'].split('oadest=')[1],
            camp['details']['name'].split('-')[1].lower()).json()
        try:
          plan['monStatus'] = plan['monStatus']['result']
          try:
            plan['monStatus'] = plan['monStatus']['directory']
            plan['monStatus'] = 'ACTIVE'
            plan['campType'] = 'homepage'
          except:
            try:
              plan['monStatus'] = plan['monStatus']['offer']
              plan['monStatus'] = 'ACTIVE'
              plan['campType'] = 'offer'
            except:
              plan['monStatus'] = plan['monStatus']
        except:
          try:
            plan['monStatus'] = plan['monStatus']['error'] + camp['details'][
                'url'].split('oadest=')[1]
          except:
            plan['monStatus'] = plan['monStatus']
        camps.remove(camp)
        if int(plan['ZPclickPause']
               ) < plan['clicksToday'] and plan['state'] == 'ACTIVE':
          pause_campaign(camp['details']['id'])
          plan['state'] = 'PAUSED'
          plan[
              'comment'] = f"paused due to clicks today {plan['clicksToday']} > {plan['ZPclickPause']}"
        if plan['clicksToday'] < int(
            plan['ZPclickPause']) and plan['state'] == 'PAUSED':
          activate_campaign(camp['details']['id'])
          plan[
              'comment'] = f"activated due to clicks today {plan['clicksToday']} < {plan['ZPclickPause']}"
        if plan['monStatus'] != 'ACTIVE' and plan['state'] == 'ACTIVE':
          plan['state'] = 'PAUSED'
          plan['comment'] = f"paused due to monetization paused"
        updated.append(plan)

  gd.create_or_update_sheet_from_dicts_withId(sheetId, 'Plan', updated)


def get_campaignKeys():
  camps = campaigns_data_domainYest()
  print(camps[0].keys())
  for key in camps[0].keys():
    print(key)
    try:
      print(f"{key} keys are {camps[0][key].keys()}")
    except:
      pass

def generate_tracking_urls():
    list = gd.read_sheet_withID(sheetId, 'links')
    completed = []
    for link in list:
        base = 'https://shopli.city/raini?rain=https%3A%2F%2Fdighlyconsive.com/41d92bd2-ab91-4f18-bfe9-0d535663753a?click_id={cid}&adv_price={visit_cost}&sub_id={target}&traffic_type=01'
        url = base + f"&geo={link['geo'].lower()}&hp=https%3A%2F%2F{link['hp']}&brand={link['brand']}-{link['geo']}-KLFLEX-ZP-mehilaWL&oadest=https%3A%2F%2F{link['oadest']}"
        completed.append({'brand' : link['brand'] , 'geo' : link['geo'] ,'oadest' : link['oadest'] ,'hp' : link['hp'], 'url' : url})
    gd.create_or_update_sheet_from_dicts_withId(sheetId, 'links',completed)


#insert homepage with www without https:// https%3A%2F%2F
def create_KLFIXcampaign(brand, geo, hp):
  url = 'https://shopli.city/raini?rain=https%3A%2F%2Fdighlyconsive.com/41d92bd2-ab91-4f18-bfe9-0d535663753a?click_id={cid}&adv_price={visit_cost}&sub_id={target}&traffic_type=01&' + f"geo={geo.lower()}&brand={brand}-{geo}-KLFIX-ZP&hp=https%3A%2F%2F{hp}&oadest=https%3A%2F%2F{hp}"
  settings = {
      "type": "RON",
      "general": {
          "name": f"{brand}-{geo}-KLFIX-All",
          "afterApprovalState": "ACTIVE"
      },
      "offer": {
          "url": url,
          "payout": {
              "type": "AUTO"
          },
          "capping": {
              "type": "OFF"
          }
      },
      "timing": {
          "frequencyFilter": {
              "value": "HALF_HOUR"
          },
          "dayParting": {
              "scheduled": "OFF"
          }
      },
      "targeting": {
          "DEVICE": {
              "deviceFilters": {
                  "devices": ["DESKTOP", "MOBILE"],
                  "mobileOses": [{
                      "os": "IOS_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "IOS_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "desktopOses": [{
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "MACOS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "WINDOWS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "CHROME",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "LINUX",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "mobileBrowsers": [
                      "FACEBOOK", "FIREFOX", "CHROME", "SAFARI", "OPERA",
                      "UC_BROWSER", "ANDROID", "OTHER", "SAMSUNG"
                  ],
                  "desktopBrowsers":
                  ["IE", "FIREFOX", "CHROME", "SAFARI", "OTHER", "EDGE"]
              }
          },
          "GEO": {
              "country": {
                  "code": geo,
              }
          },
          "LOCATION": {
              "mode": "OFF"
          },
          "BRAND": {
              "definition": "CUSTOM",
              "name": brand,
              "brandUrl": hp.split('www.')[1],
          },
          "PLACEMENTS": {
              "categories": [{
                  "category": "Buy_Now_Pay_Later",
                  "status": "ENABLED"
              }, {
                  "category": "Social",
                  "status": "ENABLED"
              }, {
                  "category": "MSN",
                  "status": "ENABLED"
              }, {
                  "category": "Tiles/Speed_Dial",
                  "status": "ENABLED"
              }, {
                  "category": "Knowledge_Panel",
                  "status": "ENABLED"
              }, {
                  "category": "Browser_Autocomplete",
                  "status": "ENABLED"
              }, {
                  "category": "Email",
                  "status": "ENABLED"
              }, {
                  "category": "Deals",
                  "status": "ENABLED"
              }, {
                  "category": "Ad_Network",
                  "status": "DISABLED"
              }, {
                  "category": "Coupon",
                  "status": "ENABLED"
              }, {
                  "category": "Commerce_Content",
                  "status": "ENABLED"
              }]
          }
      },
      "budgets": {
          "bid": 0.03,
          "campaignBudget": {
              "type": "UNLIMITED"
          },
          "dailyBudget": {
              "type": "LIMITED",
              "amount": 20
          },
          "targetBudget": {
              "type": "LIMITED",
              "amount": 1
          },
          "sourceBudget": {
              "type": "UNLIMITED"
          }
      },
      "traffic": "SEARCH",
      "trafficDestination": "HOMEPAGE"
  }
  print(settings)
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)
  print("Status Code:", r.status_code)
  print("Response Text:", r.json())
  if r.text.find(
      'targeting":{"BRAND":{"brandUrl":{"errors":["predefined brand') != -1:
    settings['targeting']['BRAND']['definition'] = 'PREDEFINED'
    #settings['general']['name'] = f"{brand}-{geo}-KLFIX-All"
    idraw = r.json()['targeting']['BRAND']['brandUrl']['errors'][0]
    indrawstart = idraw.find('id=')
    indrawend = idraw.find(']')
    id = idraw[indrawstart + 3:indrawend]
    settings['targeting']['BRAND']['id'] = id
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)
  print(r.json())

  return r.json()


def create_KLWLcampaign(brand, geo, hp):
  url = 'https://shopli.city/raini?rain=https%3A%2F%2Fdighlyconsive.com/41d92bd2-ab91-4f18-bfe9-0d535663753a?click_id={cid}&adv_price={visit_cost}&sub_id={target}&traffic_type=01&' + f"geo={geo.lower()}&brand={brand}-{geo}-KLWL-ZP&hp=https%3A%2F%2F{hp}&oadest=https%3A%2F%2F{hp}"
  settings = {
      "type": "TARGET",
      "general": {
          "name": f"{brand}-{geo}-KLWL-All",
          "afterApprovalState": "ACTIVE"
      },
      "offer": {
          "url": url,
          "payout": {
              "type": "AUTO"
          },
          "capping": {
              "type": "OFF"
          }
      },
      "timing": {
          "frequencyFilter": {
              "value": "HALF_HOUR"
          },
          "dayParting": {
              "scheduled": "OFF"
          }
      },
      "targeting": {
          "DEVICE": {
              "deviceFilters": {
                  "devices": ["DESKTOP", "MOBILE"],
                  "mobileOses": [{
                      "os": "IOS_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "IOS_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "desktopOses": [{
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "MACOS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "WINDOWS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "CHROME",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "LINUX",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "mobileBrowsers": [
                      "FACEBOOK", "FIREFOX", "CHROME", "SAFARI", "OPERA",
                      "UC_BROWSER", "ANDROID", "OTHER", "SAMSUNG"
                  ],
                  "desktopBrowsers":
                  ["IE", "FIREFOX", "CHROME", "SAFARI", "OTHER", "EDGE"]
              }
          },
          "GEO": {
              "country": {
                  "code": geo,
              }
          },
          "LOCATION": {
              "mode": "OFF"
          },
          "BRAND": {
              "definition": "CUSTOM",
              "name": brand,
              "brandUrl": hp.split('www.')[1],
          },
          "PLACEMENTS": {
              "categories": [{
                  "category": "Buy_Now_Pay_Later",
                  "status": "ENABLED"
              }, {
                  "category": "Social",
                  "status": "ENABLED"
              }, {
                  "category": "MSN",
                  "status": "ENABLED"
              }, {
                  "category": "Tiles/Speed_Dial",
                  "status": "ENABLED"
              }, {
                  "category": "Knowledge_Panel",
                  "status": "ENABLED"
              }, {
                  "category": "Browser_Autocomplete",
                  "status": "ENABLED"
              }, {
                  "category": "Email",
                  "status": "ENABLED"
              }, {
                  "category": "Deals",
                  "status": "ENABLED"
              }, {
                  "category": "Ad_Network",
                  "status": "DISABLED"
              }, {
                  "category": "Coupon",
                  "status": "ENABLED"
              }, {
                  "category": "Commerce_Content",
                  "status": "ENABLED"
              }]
          },
          "TARGET": {
              "pageDetails": {
                  "type": "PAGED",
                  "page": 0,
                  "pageSize": 100,
                  "total": 2
              },
              "targets": [{
                  "key": {
                      "hash": "echo-bah-v042le2g39"
                  },
                  "properties": {
                      "bid": 0.1501
                  }
              }, {
                  "key": {
                      "hash": "bravo-ess-vqpn5rx7gr"
                  },
                  "properties": {
                      "bid": 0.1501
                  }
              }]
          }
      },
      "budgets": {
          "bid": 0.03,
          "campaignBudget": {
              "type": "UNLIMITED"
          },
          "dailyBudget": {
              "type": "LIMITED",
              "amount": 20
          },
          "targetBudget": {
              "type": "LIMITED",
              "amount": 1
          },
          "sourceBudget": {
              "type": "UNLIMITED"
          }
      },
      "traffic": "SEARCH",
      "trafficDestination": "HOMEPAGE"
  }
  print(settings)
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)
  print("Status Code:", r.status_code)
  print("Response Text:", r.json())
  if r.text.find(
      'targeting":{"BRAND":{"brandUrl":{"errors":["predefined brand') != -1:
    settings['targeting']['BRAND']['definition'] = 'PREDEFINED'
    #settings['general']['name'] = f"{brand}-{geo}-KLFIX-All"
    idraw = r.json()['targeting']['BRAND']['brandUrl']['errors'][0]
    indrawstart = idraw.find('id=')
    indrawend = idraw.find(']')
    id = idraw[indrawstart + 3:indrawend]
    settings['targeting']['BRAND']['id'] = id
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)
  print(r.json())

  return r.json()


def create_S24campaign(brand, geo, hp):

  geo = geo.upper()
  match geo:
    case 'DE':
      geoid = 9172
    case 'UK':
      geoid = 9174
    case 'GB':
      geoid = 9174
    case 'FR':
      geoid = 9173
    case _:
      geoid = ' error cant find geo'
  url = 'https://shopli.city/raini?rain=https%3A%2F%2Fdighlyconsive.com/a23f6a72-9b6f-4c09-b968-89771c49f73e?traffic_type=07&click_id={cid}&adv_price={visit_cost}&sub_id={target}&' + f"geo={geoid}&brand={brand}-{geo}-S24-ZP&hp=https%3A%2F%2F{hp}&oadest=https%3A%2F%2F{hp}"
  settings = {
      "type": "RON",
      "general": {
          "name": f"{brand}-{geo}-s24-All",
          "afterApprovalState": "ACTIVE"
      },
      "offer": {
          "url": url,
          "payout": {
              "type": "AUTO"
          },
          "capping": {
              "type": "OFF"
          }
      },
      "timing": {
          "frequencyFilter": {
              "value": "HALF_HOUR"
          },
          "dayParting": {
              "scheduled": "OFF"
          }
      },
      "targeting": {
          "DEVICE": {
              "deviceFilters": {
                  "devices": ["DESKTOP", "MOBILE"],
                  "mobileOses": [{
                      "os": "IOS_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "IOS_PHONE",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "ANDROID_TABLET",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "desktopOses": [{
                      "os": "OTHER",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "MACOS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "WINDOWS",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "CHROME",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }, {
                      "os": "LINUX",
                      "version": {
                          "min": "MIN",
                          "max": "MAX"
                      }
                  }],
                  "mobileBrowsers": [
                      "FACEBOOK", "FIREFOX", "CHROME", "SAFARI", "OPERA",
                      "UC_BROWSER", "ANDROID", "OTHER", "SAMSUNG"
                  ],
                  "desktopBrowsers":
                  ["IE", "FIREFOX", "CHROME", "SAFARI", "OTHER", "EDGE"]
              }
          },
          "GEO": {
              "country": {
                  "code": geo,
              }
          },
          "LOCATION": {
              "mode": "OFF"
          },
          "BRAND": {
              "definition": "CUSTOM",
              "name": brand,
              "brandUrl": hp.split('www.')[1],
          },
          "PLACEMENTS": {
              "categories": [{
                  "category": "Buy_Now_Pay_Later",
                  "status": "ENABLED"
              }, {
                  "category": "Social",
                  "status": "ENABLED"
              }, {
                  "category": "MSN",
                  "status": "ENABLED"
              }, {
                  "category": "Tiles/Speed_Dial",
                  "status": "ENABLED"
              }, {
                  "category": "Knowledge_Panel",
                  "status": "ENABLED"
              }, {
                  "category": "Browser_Autocomplete",
                  "status": "ENABLED"
              }, {
                  "category": "Email",
                  "status": "ENABLED"
              }, {
                  "category": "Deals",
                  "status": "ENABLED"
              }, {
                  "category": "Ad_Network",
                  "status": "DISABLED"
              }, {
                  "category": "Coupon",
                  "status": "ENABLED"
              }, {
                  "category": "Commerce_Content",
                  "status": "ENABLED"
              }]
          }
      },
      "budgets": {
          "bid": 0.03,
          "campaignBudget": {
              "type": "UNLIMITED"
          },
          "dailyBudget": {
              "type": "LIMITED",
              "amount": 20
          },
          "targetBudget": {
              "type": "LIMITED",
              "amount": 1
          },
          "sourceBudget": {
              "type": "UNLIMITED"
          }
      },
      "traffic": "SEARCH",
      "trafficDestination": "HOMEPAGE"
  }
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)
  print("Status Code:", r.status_code)
  print("Response Text:", r.json())
  if r.text.find(
      'targeting":{"BRAND":{"brandUrl":{"errors":["predefined brand') != -1:
    settings['targeting']['BRAND']['definition'] = 'PREDEFINED'
    #settings['general']['name'] = f"{brand}-{geo}-KLFIX-All"
    idraw = r.json()['targeting']['BRAND']['brandUrl']['errors'][0]
    indrawstart = idraw.find('id=')
    indrawend = idraw.find(']')
    id = idraw[indrawstart + 3:indrawend]
    settings['targeting']['BRAND']['id'] = id
  r = requests.put('https://panel.zeropark.com/api/v2/search-brand/campaigns',
                   headers=headers,
                   json=settings)

  return r.json()


# 4/11/25 - function pauses all generalMehila campaigns , will be used everyday at midnight to pause all generalMehila campaigns
def pause_generalMehila():
  data = campaigns_data_domainToday()
  #generalMehilas = [ ]
  for element in data:
    if element['details']['name'].split('-')[0] == 'generalMehila':
      pause_campaign(element['details']['id'])
      #generalMehilas.append(element['details'])
      print(f"paused campaign {element['details']['name']}")
# 4/11/25 - function pauses all generalMehila campaigns that passed 1500 redirects today running every hour with mehilot dashboard
def pause_generalMehila2k():
  data = campaigns_data_domainToday()
  for element in data:
    if element['details']['name'].split('-')[0] == 'generalMehila' and element['stats']['redirects'] > 1000:
        if element['details']['name'].split('-')[1] in ['FR','DE','IT']:
            if element['stats']['redirects'] > 2500:
                pause_campaign(element['details']['id'])
        else: pause_campaign(element['details']['id'])
        print(f"paused campaign {element['details']['name']} for passing 2k clicks")

def generalMehilaMon():
  now = datetime.now().strftime('%Y-%m-%d -- %H:%M')
  sheet = gd.read_sheet_withID(sheetId, 'genMehilaCamp')
  updated = []
  for genMe in sheet:
    if genMe['url'] != '0':
      mon = kl.check_monetizationWithResp(genMe['url'],genMe['geo'].lower()).json()
      try:
        mon = mon['result']
        mon = True
      except:
        try:
          mon = mon['messaege']
        except:
          print(f'error in monetization check geo is {genMe["geo"]} and url is {genMe["url"]}')
      genMe['monStatus'] = mon
      genMe['lastUpdate'] = now
      if mon != True:
        pause_campaign(genMe['zpid'])
    else:
      genMe['monStatus'] = 'no url'
      genMe['lastUpdate'] = now
      #print('url not inserted')


    updated.append(genMe)
    time.sleep(1)
  gd.create_or_update_sheet_from_dicts_withID(sheetId,'genMehilaCamp',updated)
  return
#generate_tracking_urls()
#mehilot()
