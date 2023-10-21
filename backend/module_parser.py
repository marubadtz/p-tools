import requests
import time
from bs4 import BeautifulSoup
import string
import random
from datetime import datetime, timezone
from deta import Deta

# Praser
class LastNewsDumper:
    # Get last 10 news from allnews page
    def __init__(self):
        self.url = "https://steamcommunity.com/app/552990/allnews/"
        self.announce_ids = []
        self.eventcomments_ids = []
        self.titles = []
        self.data = {'announce_id':[],
                     'eventcomment_id':[],
                     'title':[],
                     'timestamp_published':[],
                     'timestamp_parsed':[],
                     'rates':[],
                     'comments':[],
                     'type':[]}

    def get_data_all_news_page(self):
        #Read all news
        url = "https://steamcommunity.com/app/552990/allnews/"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")

        news_soup = soup.find_all("div", {"class": "Announcement_Card"})
        for news_html in news_soup:
            announce_id = news_html['data-partner-event-announce-gid']
            self.data['announce_id'].append(announce_id)

        title_soup = soup.find_all("div", {"class": "apphub_CardContentNewsTitle"})
        for title_html in title_soup:
            self.data['title'].append(title_html.text)

        rates_soup = soup.find_all("div", {"class": "apphub_CardRating"})
        for rate_html in rates_soup:
            self.data['rates'].append(int(rate_html.text.replace(',','')))

        comments_soup = soup.find_all("div", {"class": "apphub_CardCommentCount"})
        for comment_html in comments_soup:
            self.data['comments'].append(int(comment_html.text.replace(',','')))
        
    def get_eventcomment_id(self, announce_id):
        #Example: for 3664293140685803267 looking 3807282428855272513
        url = f"https://steamcommunity.com/games/552990/announcements/detail/{announce_id}"
        res = requests.get(url)
        soup = BeautifulSoup(res.text, "html.parser")
        data = soup.find(lambda tag:tag.name=="script" and "g_sessionID" in tag.text).find_next_sibling()
        #soup = BeautifulSoup(str(data), 'html.parser')
        eventcomments_id = str(data).replace('&quot','"').rsplit(str(announce_id),1)[1].\
                                split('forum_topic_id')[1].split('event_gid')[0].\
                                translate(str.maketrans('', '', string.punctuation))
        self.data['eventcomment_id'].append(eventcomments_id)
        
        dtime = soup.find("meta", {"property":"article:published_time"})['content']
        dtime_utc = datetime.fromisoformat(dtime).astimezone(timezone.utc)
        unix_time = int(time.mktime(dtime_utc.timetuple()))
        self.data['timestamp_published'].append(unix_time)

        time.sleep(0.5)

    def get_data_for_all_eventcomment_id(self):
        for announce_id in self.data['announce_id']:
            self.get_eventcomment_id(announce_id)

    def get_all_data(self):
        self.get_data_all_news_page()
        self.get_data_for_all_eventcomment_id()
        self.data['timestamp_parsed'] = [0] * len(self.data['announce_id'])
        self.data['type'] = ['news'] * len(self.data['announce_id'])

    def data_by_keys(self, *keys):
        res = {key: val for key, val in self.data.items() if key in keys}
        return res

class NewsCommentsDumper:
    def __init__(self, eventcomment_id):
        self.eventcomment_id = str(eventcomment_id)
        self.url = f'https://steamcommunity.com/app/552990/eventcomments/{eventcomment_id}'
        soup = self.read_page(1)
        self.comments_total = int(soup.select_one('span[id*="_pagetotal"]').text.replace(',',''))
        self.comments_per_page = int(soup.select_one('span[id*="_pageend"]').text.replace(',',''))
        self.pages_total = (self.comments_total + self.comments_per_page- 1) // self.comments_per_page
        self.data = {'comment_id':[], 'comment_author':[], 'comment_timestamp':[], 'author_is_dev':[], 
                            'author_steam_id':[], 'comment_text':[], 'comment_len':[],
                            'eventcomment_id':[]}

    def read_page(self, p):
        resp = requests.get(self.url+f'?ctp={p}')
        soup = BeautifulSoup(resp.text, 'html.parser')
        return soup
    
    def parse_data_from_page(self, p):
        soup = self.read_page(p)
        soup_comments = soup.find_all('div', {'class':'commentthread_comment_content'})

        for c in soup_comments:
            self.data['comment_id'].append(c.find('div', {'class':'commentthread_comment_text'})['id'].split('_')[-1])
            self.data['comment_author'].append(c.find('bdi').text)
            self.data['comment_timestamp'].append(int(c.find('span', {'class':'commentthread_comment_timestamp'})['data-timestamp']))
            self.data['author_is_dev'].append(int(c.find('span', {'class':'commentthread_workshop_authorbadge'}) is not None))
            url_profile = c.find('a', {'class':'commentthread_author_link'})['href']
            try:
                steamid = int(url_profile.split('/')[-1])
                self.data['author_steam_id'].append(str(steamid))
            except:
                resp = requests.get(url_profile)
                soup_profile = BeautifulSoup(resp.text, "html.parser")
                steam_id = soup_profile.find('div', {'id':'responsive_page_template_content'}).\
                               find('script').contents[0].split("steamid")[1].split(',')[0].\
                                translate(str.maketrans('', '', string.punctuation))
                self.data['author_steam_id'].append(steam_id)
                time.sleep(0.5)
            try:
                unwanted = c.find('blockquote')
                unwanted.extract()
            except:
                pass
            self.data['comment_text'].append(c.find('div', {'class':'commentthread_comment_text'}).text.strip())
            self.data['comment_len'].append(len(c.find('div', {'class':'commentthread_comment_text'}).text.strip()))
        self.data['eventcomment_id'] = [self.eventcomment_id] * len(self.data['comment_id'])
    
    def parse_data_from_all_pages(self):
        for p in range(1, self.pages_total + 1):
            self.parse_data_from_page(p)
            time.sleep(0.5)


'''
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
        data_rec.sort(key = self.__get_timestamp, reverse=True)
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
        keys = ['author_steam_id', 'comment_timestamp', 'comment_author', 'comment_text']
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

'''