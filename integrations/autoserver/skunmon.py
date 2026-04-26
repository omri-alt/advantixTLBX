from integrations.autoserver import sk as sk
import time
from integrations.autoserver import kl_as as kl
today = time.strftime("%Y-%m-%d")
from integrations.autoserver import gdocs_as as gdocs

def pause_Unmonetized_KL():
  raw_adv_list = sk.get_advertisers()  #get a list of sk advertisers
  completed = []
  paused = []
  for adv in raw_adv_list[0:]:  #for each advertiser in the list
    namelst = adv['name'].split('-')  #split the name of each advertiser to a list
    geo = namelst[-2].lower()  #extract the geo out of the adv name
    advid = adv['id']  #extract the SK adv id
    campaigns = sk.get_campaignsByAdvid(advid)  #get all campaigns out of the adv    
    cond = ['KL', 'KLFIX']  #condition defenition
    if len(campaigns) > 0 and (namelst[-1] in cond):  #if the adv have campaigns and aff in condition
      for camp in campaigns:  #for each campaign in sk campaigns
        if camp['active']:  #if the key "active" == True
          id = camp['id']  #extract the campaign id
          try:
            data = sk.get_campaignById(id)  #get sk campaign data from it's id
            hpind = data['trackingUrl'].find('hp=', 0) + 3  #find the index of +3 after "hp=" in the tracking url str
            hpIndEnd = data['trackingUrl'][hpind:].find('&')#start at the start index and find the index of the letter "&" which ends the HP parameter
          except:
            print(f"error in campaign {id} sleeping for 60 sec")
            time.sleep(60)
            try:
              data = sk.get_campaignById(id)
              hpind = data['trackingUrl'].find('hp=', 0) + 3
              hpIndEnd = data['trackingUrl'][hpind:].find('&')
            except:
              print("stil error!!!!!!!!")
          if hpIndEnd == -1:
            hpIndEnd = len(data['trackingUrl'][hpind:])
          hp = data['trackingUrl'][hpind:hpind + hpIndEnd]  #extract hp (starts from the first index, and stops at the hpIndEnd indeX (which is comparded to the hpind index)
          try:#find the index in the list of completed and breaks if it's not exists
            index = completed.index(hp)  
          except:# if it's not exists so index = -1 (which isn't possible for index which is a natural number
            index = -1  
          if index == -1:  # if the merchant is not in the completed list
            completed.append(hp)  #add the merchant to the completed lisT
            t = kl.check_monetization(hp, geo)
            match t:
              case False:
                temp = sk.pause_campaign(id)  #if not active, pause the campaign in SK
                paused.append({'hp':hp,'geo':geo,'id':id , 'monetization' : t })
                gdocs.create_or_update_sheet_from_dicts(f'{today}-paused',paused)
                print(f"hp {hp} added to list and the campaign was paused in sk")
              case 'error occured':
                print('yabadabadoo ')
                print(f"error occured in {hp}, geo {geo} and campaign {id} was paused")
                #sk.pause_campaign(id)  #if not active, pause the campaign in SK
                print(f"hp {hp} added to list and the campaign was paused in sk")
              case _:
                print(hp)
                #print(f"hp {hp} added to list and the campaign is active")



  return completed

