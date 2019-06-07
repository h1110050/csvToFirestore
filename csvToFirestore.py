#largely based of code from https://medium.com/@cbrannen/importing-data-into-firestore-using-python-dce2d6d3cd51

import csv
import firebase_admin
import google.cloud
import json
from firebase_admin import credentials, firestore

if (not len(firebase_admin._apps)):
    cred = credentials.Certificate("./ServiceAccountKey.json") #download this from firebase
    firebase_admin.initialize_app(cred)

store = firestore.client()

file_path = "./dataframe.csv"
collection_name = "test_collection"


def batch_data(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


data = []
headers = []
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            for header in row:
                headers.append(header)
            line_count += 1
        else:
            obj = {}
            for idx, item in enumerate(row):
                obj[headers[idx]] = item
            obj['location'] = firestore.GeoPoint(float(obj['lat']), float(obj['long'])) #geopoint transformation
            obj['redemptionLimit'] = int(obj['redemptionLimit']) #str to int
            obj.pop('lat')
            obj.pop('long')
            obj.pop('geopy')
            #obj['expiryDate'] = json.dumps(dt(obj['expiryDate']).isoformat())
            data.append(obj)
            line_count += 1
    print(f'Processed {line_count} lines.')

for batched_data in batch_data(data, 499):
    batch = store.batch()
    for data_item in batched_data:
        doc_ref = store.collection(collection_name).document()
        batch.set(doc_ref, data_item)
    batch.commit()

print('Done')