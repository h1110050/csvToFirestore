from geopy.geocoders import Nominatim
import pandas as pd
import datetime as dt
import numpy as np
import time

GOOGLEFORMCSV = "GOOGLEFORM.csv" #replace with name of csv file from google form

df = pd.read_csv(GOOGLEFORMCSV)

#df['expiryDate'] = df['Expiry Date'].map(str) + ' ' + df['Expiry Time'].map(str)
#df['expiryDate'] = pd.to_datetime(df['expiryDate'])

df['expiryDate'] = pd.to_datetime("2019-09-01 00:00:01")
df['startDate'] = pd.to_datetime("2019-08-01 00:00:01")

#if want to format into ISO
#df['expiryDate'] = df['expiryDate'].map(lambda x: dt.datetime.strftime(x, '%Y-%m-%dT%H:%M:%S+08:00'))

geolocator = Nominatim(user_agent="my-project")

for index, item in df.iterrows():
    
    postal = str(item['Postal Code'])
    while len(postal) < 6:
        postal = "0{}".format(postal)
    
    address = "{}, {}, {}, Singapore".format(str(item['Block Number']), item.Street, postal)
    print(address)
    df.loc[index,'locationName'] = address
    
    location = geolocator.geocode(address)
    if not location:
        location = "Could not locate"
        #print(location)
        df.loc[index,'geopy'] = location
        df.loc[index,'lat'] = location
        df.loc[index, 'long'] = location
    else:
        #print(location.address)
        df.loc[index,'geopy'] = location.address
        df.loc[index,'lat'] = location.latitude
        df.loc[index, 'long'] = location.longitude
        
    #time.sleep(1)
    
df.drop(columns=['Block Number', 'Street', 'Postal Code', 'Timestamp'], axis=1, inplace=True)

df.rename(inplace=True, index=str, columns={"Store Name": "merchantName", "Promo Name": "promoName", "Promo Description": 'rDescription', 'Link to picture attachment': 'attachment', 'Limit of Redemption (Leave blank if no limit)': 'redemptionLimit'})

df.head()

export_csv = df.to_csv(r'PATH.csv', index = None, header=True) #Don't forget to add '.csv' at the end of the path

print('done')