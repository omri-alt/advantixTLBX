import ec as ec
import gdocs as gd
import time 

def bulkEC_KLFIX():
  items = []
  bulk = gd.read_sheet('bulkEC-KLFIX')
  for camp in bulk:
    brand = camp['brand'].lower()
    item1 , item2 = ec.create_campaignKLfix(brand,camp['geo'],camp['hp'],camp['fbhp'])
    items.append(item2)
    time.sleep(15)
    gd.create_or_update_sheet_from_dicts('bulkEC-KLFIX-results',items)


##############################################
#to create a KLfix bulk unccoment the below line
ec.bulk_create_campaignsKLfixFromSheet()

##############################################
#to create a KLwl bulk unccoment the below line
#bulkZPwl()