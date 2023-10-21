from deta import Deta
import json
import time
import random
import config
from datetime import datetime, timezone, timedelta
from collections import Counter

class AllEventsDB:
    def __init__(self, type):
        deta = Deta(config.DETA_API_KEY)
        self.db = deta.Base('all_events')
        self.type = type

    def __get_timestamp(self, data_rec):
        return data_rec.get("timestamp_published")

# Fetch #
    def fetch_by_keys(self, *keys):
        res = {key:val for key,val in self.__from_records_to_dict(self.db.fetch({'type':self.type}).items).items() if key in keys}
        return res
    
    def fetch_rates_comments_by_eventcomment_id(self, eventcomment_id):
        res = {key:val for key, val in self.db.fetch({'eventcomment_id': eventcomment_id, 'type':self.type}).\
               items[0].items() if key in ['rates', 'comments']}
        return res
    
    def fetch_coluumns_last_n_days(self, *keys, days=30):
        timestamp_days_back = int((datetime.now(timezone.utc) - timedelta(days)).timestamp())
        data = self.db.fetch({"timestamp_published?gt": timestamp_days_back, 'type':self.type}).items
        data.sort(key = self.__get_timestamp, reverse=True)
        res = {key:val for key,val in self.__from_records_to_dict(data).items() if key in keys}
        return res
    
    def fetch_dates_titles_last_n_days_str(self, days=30):
        dates_titles_last = self.fetch_coluumns_last_n_days('eventcomment_id', 'timestamp_published', 'timestamp_parsed', 
                                                            'title', days=days)
        dates_titles_last['time_published'] = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')if ts != 0 \
                                                    else '0' for ts in dates_titles_last['timestamp_published']]
        dates_titles_last['time_parsed'] = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')if ts != 0 \
                                                    else '0' for ts in dates_titles_last['timestamp_parsed']]
        return dates_titles_last 
###

    def put_in_db(self, data_dict_update):
        max_items = 25
        data_rec = self.__from_dict_to_records(data_dict_update)
        for i in range(0, len(data_rec), max_items):
            self.db.put_many(data_rec[i : i + max_items])

    def __from_dict_to_records(self, data_dict):
        keys = data_dict.keys()
        vals = list(zip(*data_dict.values()))
        data_recs =[]
        for v in vals:
            dishDict=dict(zip(keys,v))
            data_recs.append(dishDict)
        return data_recs
    
    def __from_records_to_dict(self, data_recs):
        keys = data_recs[0].keys()
        data_dict= {}
        vals = list(zip(*[list(r.values()) for r in data_recs]))
        for k, v in zip(keys, vals):
            data_dict[k] = list(v)
        return data_dict
    
# Update #
    def __get_unix_time_now(self):
        dtime_now_utc = datetime.now(timezone.utc)
        unix_time_now = int(time.mktime(dtime_now_utc.timetuple()))
        return unix_time_now

    def update_timestamp_parsed(self, eventcomment_id):
        eventcomment_id = str(eventcomment_id)
        key = self.db.fetch({"eventcomment_id": eventcomment_id, 'type':self.type}).items[0]['key']
        data = {'timestamp_parsed':self.__get_unix_time_now()}
        self.db.update(data, key)

    def update_rates_comments_by_eventcomment_id(self, eventcomment_id, rates, comments):
        key = self.db.fetch({"eventcomment_id": eventcomment_id, 'type':self.type}).items[0]['key']
        data = {'rates':rates, 'comments':comments}
        self.db.update(data, key)

    def update_rates_comments(self, parsed_rates_comments):
        for eventcomment_id, parsed_rates, parsed_comments in zip(parsed_rates_comments['eventcomment_id'], \
                                            parsed_rates_comments['rates'], parsed_rates_comments['comments']):
            db_rates_comments = self.fetch_rates_comments_by_eventcomment_id(eventcomment_id)
            if parsed_comments > db_rates_comments['comments'] or parsed_rates > db_rates_comments['rates']:
                self.update_rates_comments_by_eventcomment_id(eventcomment_id, parsed_rates, parsed_comments)
###

# Delete duplicates #
    def __get_keys_with_duplicates_announce_id(self, data):
        counts = Counter(data.values())
        nonunique = {v:k for k, v in data.items() if counts[v] > 1}
        res = {val: key for key, val in nonunique.items()}
        return list(res)

    def __delete_by_key(self, keys):
        for key in keys:
            self.db.delete(key)

    def delete_db_duplicates(self):
        key_announce_id_dict = {rec['key']:rec['announce_id'] for rec in self.db.fetch({'type':self.type}).items}
        keys_to_delete = self.__get_keys_with_duplicates_announce_id(key_announce_id_dict)
        self.__delete_by_key(keys_to_delete)
###

class NewsDB:
    def __init__(self, eventcomment_id):
        deta = Deta('a0pdaatz93v_Afj1YuzAhPdmmBm6qJNUoK4LvnnborNS')
        self.db = deta.Base('news')
        self.eventcomment_id = str(eventcomment_id)
        self.data = self.fetch_sorted_comments()
    
    def __get_timestamp(self, data_rec):
        return data_rec.get('comment_timestamp')
    
    def __from_dict_to_records(self, data_dict):
        keys = data_dict.keys()
        vals = list(zip(*data_dict.values()))
        data_recs =[]
        for v in vals:
            dishDict=dict(zip(keys,v))
            data_recs.append(dishDict)
        return data_recs
    
    def __from_records_to_dict(self, data_recs):
        keys = data_recs[0].keys()
        data_dict= {}
        vals = list(zip(*[list(r.values()) for r in data_recs]))
        for k, v in zip(keys, vals):
            data_dict[k] = list(v)
        return data_dict
    
    def __strftime_ts_list(self, ts_list):
        res = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in ts_list]
        return res
    
    def put_in_db(self, data_dict_update):
        max_items = 25
        data_rec = self.__from_dict_to_records(data_dict_update)
        for i in range(0, len(data_rec), max_items):
            self.db.put_many(data_rec[i : i + max_items])
    
# Fetch #
    def fetch_sorted_comments(self):
        data_rec = self.db.fetch({'eventcomment_id':self.eventcomment_id}).items
        if len(data_rec) == 0:
            return
        data_rec.sort(key = self.__get_timestamp, reverse=False)
        return self.__from_records_to_dict(data_rec)
    
    def get_n_last_comments(self, *keys, n_last=None):
        if n_last is None:
            n_last = len(self.data['comment_id'])
        if keys == ():
            res = {key:val[:n_last] for key,val in self.data.items()}
        else:
            res = {key:val[:n_last] for key,val in self.data.items() if key in keys}
        return res
    
    def get_n_last_comments_for_dash(self, n_last=5, len_max=80):
        keys = ['author_steam_id', 'comment_timestamp', 'comment_author', 'comment_text', 'comment_id']
        comments_for_dash = self.get_n_last_comments(*keys, n_last=n_last)
        comments_for_dash['comment_timestamp'] = self.__strftime_ts_list(comments_for_dash['comment_timestamp'])
        comments_for_dash['comment_text'] = [comment[:len_max-3]+"..." if len(comment) > 80 else comment for comment in comments_for_dash['comment_text']]
        comments_for_dash['time'] = comments_for_dash.pop('comment_timestamp')
        comments_for_dash['text'] = comments_for_dash.pop('comment_text')
        comments_for_dash['author'] = comments_for_dash.pop('comment_author')
        comments_for_dash['steam_id'] = comments_for_dash.pop('author_steam_id')
        return comments_for_dash
    
    def get_n_rand_author_with_steam_id_for_dash(self, n_authors = 5):
        zipped = list(zip(self.data['comment_author'], self.data['author_steam_id']))
        random.shuffle(zipped)
        rand_authors_for_dash = {}
        rand_authors_for_dash['author'], rand_authors_for_dash['steam_id'] = zip(*zipped[:n_authors])
        return rand_authors_for_dash

## Delete
    def __delete_by_key(self, keys):
        for key in keys:
            self.db.delete(key)

    def delete_all_for_eventcomment_id(self):
        keys_to_delete = self.data['key']
        self.__delete_by_key(keys_to_delete)




'''
deta = Deta('a0pdaatz93v_Afj1YuzAhPdmmBm6qJNUoK4LvnnborNS')
db = deta.Base('discussions')
db.put({"value": "Hello world!"}, "my-key")
item = db.get("my-key")

item['value']
db.put_many(["First item", "Second item", "Third item"])

db.update(
    {"value": "Hello updated world!"},
    "my-key",
)

dict_list= [{"name": "Beverly", "hometown": "Copernicus City"},
            {"name": "Alex", "hometown": "Belgrade"},
            {"name": "Julia", "hometown": "Novi Sad"}]

db.put_many(dict_list)

with open('dev_bul_update_127_table.json') as f:
    data = json.load(f)

len(data['data'])

db.put_many(data['data'])





len(data_table)

with open('./parser/dev_bul_update_127.json') as f:
    data_t = json.load(f)

data['data'][0:5]
data_t["data"][0:5]
db.put_many(data_t["data"][0:5])

data_dict = {'comment_author':["bob", "lob", 'job']}
db.put_many(data_dict)


yourdict = {
   'name' : ['abc', 'xyz'],
   'email': ['abc@abc.com', 'xyz@xyz.com'],
   'category': ['category 1', 'category 2']
   }

result = [{k: yourdict[k][n]} for k in yourdict for n in range(len(yourdict[k]))]

for k, v in yourdict.items():
    print({k:v1 for v1 in  v})
    [].append(yourdict.itemsD)


allKeys=['A','B','C','D','E']
a=[[1,2,3,4,5],[6,7,8,9,10],[11,12,13,14,15]]

it = iter(a)
b=[]
for i in a:
    dishDict=dict(zip(allKeys,i))
    b.append(dishDict)

print(b)

# from dict to records
mydict = {'A':[1,6,11],'B':[2,7,12],'C':[3,8,13],'D':[4,9,14],'E':[5,10,15]}

def from_dict_to_records(data_dict):
    keys = data_dict.keys()
    vals = list(zip(*data_dict.values()))
    data_recs =[]
    for v in vals:
        dishDict=dict(zip(keys,v))
        data_recs.append(dishDict)
    return data_recs

def from_records_to_dict(data_recs):
    keys = data_recs[0].keys()
    data_dict= {}
    vals = list(zip(*[list(r.values()) for r in data_recs]))
    for k, v in zip(keys, vals):
        data_dict[k] = list(v)
    return data_dict

data_recs = all_items

db = deta.Base('all_events')

def put_in_collection(data_dict):
    max_items = 25
    data_rec = from_dict_to_records(data_dict)
    for i in range(0, len(data_rec), max_items):
        db.put_many(data_rec[i : i + max_items])


put_in_collection(data)
from_dict_to_records(data)

data_dict = data
#from records_to_dict
vals = [list(v.values()) for v in data_recs]
list(zip(*vals))
list(map(list, zip(*vals)))

list(zip(*data_from_db))

data['timestamp_parsed'] = [0]*len(data['timestamp_published'])
data['type'] = ['news']*len(data['timestamp_published'])


item = db.get("7n65ow8q6r4p")

data_from_db = db.fetch().items
keys = data_from_db[0].keys()
for k, rec in zip(keys:
    print()


len(db.find().limit(100).items)

db.fetch({'author_steam_id':76561199213164780}).items

while res.last:
    res = db.fetch(last=res.last)
    all_items += res.items



from_records_to_dict(data_from_db)


def fetch_all_data():
    res = db.fetch()
    all_items = res.items

    # Continue fetching until "res.last" is None.
    while res.last:
        print('f')
        res = db.fetch(last=res.last)
        all_items += res.items
    return all_items


len(all_items)

all_items = fetch_all_data()

len(from_records_to_dict(all_items)['comment_author'])
len(set(from_records_to_dict(all_items)['comment_author']))

for a in all_items:
    if len(a) == 0:
        print(len(a))
'''