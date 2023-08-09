import requests
import string
import time
from bs4 import BeautifulSoup


def announce_id_to_eventcommnets_id(announce_id):
    details_url = f"https://steamcommunity.com/games/552990/announcements/detail/{announce_id}"
    res = requests.get(details_url)
    soup = BeautifulSoup(res.text)
    time.sleep(0.5)
    detail_soup = soup.find('div', {'id':"application_config"})
    eventcomments_id = int(str(detail_soup).split('forum_topic_id')[1].split('event_gid')[0].replace('&quot;','').\
                        translate(str.maketrans('', '', string.punctuation)))
    return eventcomments_id

#Read all news
url = "https://steamcommunity.com/app/552990/allnews/"

res = requests.get(url)
soup = BeautifulSoup(res.text)
#<div class="apphub_CardContentNewsTitle">

announce_ids = []
eventcomments_ids = []
news_soup = soup.find_all("div", {"class": "Announcement_Card"})
for news_html in news_soup:
    announce_id = int(news_html['data-partner-event-announce-gid'])
    announce_ids.append(announce_id)
    eventcomments_ids.append(announce_id_to_eventcommnets_id(announce_id))


url3 = f"https://steamcommunity.com/app/552990/eventcomments/{eventcomments_id}"

eventcomments_ids

details_url = f"https://steamcommunity.com/games/552990/announcements/detail/{3664293140689082175}"
res = requests.get(details_url)
soup = BeautifulSoup(res.text)


soup.find_all("div", {"class": "discussionwidget_DiscussContainer"}, partial=True)

soup

int(str(soup).split('forum_topic_id')[1].split('event_gid')[0].replace('&quot;','').\
                        translate(str.maketrans('', '', string.punctuation)))

https://steamcommunity.com/app/552990/eventcomments/3807282428855272513
