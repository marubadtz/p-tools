from fastapi import FastAPI, Request
import json
from fastapi.responses import JSONResponse
from datetime import datetime
from deta import Deta
from module_db import AllEventsDB, NewsDB
from module_parser import LastNewsDumper, NewsCommentsDumper

app = FastAPI()


@app.get("/hello")
def root():
    return "Hello from Space! 🚀"

@app.get("/comments/{eventcomment_id}")
async def get_comments(eventcomment_id: int):
    #with open('dev_bul_update_127.json') as f:
    #    data = json.load(f)
    #data_to_send = fetch_5_comments_from_db(eventcomment_id)
    #data_to_send = prepare_comments_to_send(data_to_send)
    eventcomment_id = str(eventcomment_id)
    try:
        news_db = NewsDB(eventcomment_id)
        last_5_comments = news_db.get_n_last_comments_for_dash()
        return JSONResponse(last_5_comments)
    except:
        return

@app.get("/authors/{eventcomment_id}/{n_authors}")
async def get_comments(eventcomment_id: int, n_authors: int):
    try:
        news_db = NewsDB(eventcomment_id)
        rand_authors = news_db.get_n_rand_author_with_steam_id_for_dash(n_authors)
        return JSONResponse(rand_authors)
    except:
        return

@app.get("/titles")
async def get_titles():
    all_events_db = AllEventsDB('news')
    dates_titles_last = all_events_db.fetch_dates_titles_last_n_days_str(days=60)
    return JSONResponse(dates_titles_last)

@app.get("/dump/{eventcomment_id}")
async def get_titles(eventcomment_id: int):
    # Parse button
    new_comments_dumper = NewsCommentsDumper(eventcomment_id)
    news_db = NewsDB(eventcomment_id)
    all_events_db = AllEventsDB('news')

    comment_ids_db = news_db.data['comment_id'] if news_db.data is not None else []

    # Parse pages untill comment_id meets in db
    for i in range(new_comments_dumper.pages_total):
        new_comments_dumper.parse_data_from_page(i+1)
        if set(new_comments_dumper.data['comment_id']) & set(comment_ids_db):
            break

    # Update db with records for missing announce ids
    def update_with_new_comments(comment_id_not_in_db):
        if len(comment_id_not_in_db) > 0:
            ids_to_update = [new_comments_dumper.data['comment_id'].index(id) for id in comment_id_not_in_db]
            data_dict_update = {k:[v[index] for index in ids_to_update] for k, v in new_comments_dumper.data.items()}
            news_db.put_in_db(data_dict_update)
            all_events_db.update_timestamp_parsed(str(eventcomment_id))

    # Find announce ids not in db and update
    comment_ids_parsed = new_comments_dumper.data['comment_id']
    comment_id_not_in_db = list(set(comment_ids_parsed).difference(comment_ids_db))
    update_with_new_comments(comment_id_not_in_db)




@app.get("/__space/v0/actions")
async def scheduled_update():
    # Scheduled tasks #
    # Parse Last News Page, if there are new news add them to db
    # Parse Last News Page
    last_news_dumper = LastNewsDumper()
    last_news_dumper.get_all_data()

    # Fetch all announce ids from db
    all_events_db = AllEventsDB('news')
    all_events_db.fetch_by_keys('announce_id')

    # Find announce ids not in db
    announce_ids_parsed = last_news_dumper.data['announce_id']
    announce_ids_db = all_events_db.fetch_by_keys('announce_id')['announce_id']
    announce_id_not_in_db = list(set(announce_ids_parsed).difference(announce_ids_db))

    # Update db with records for missing announce ids
    def update_with_new_events(announce_id_not_in_db):
        if len(announce_id_not_in_db) > 0:
            ids_to_update = [last_news_dumper.data['announce_id'].index(id) for id in announce_id_not_in_db]
            data_dict_update = {k:[v[index] for index in ids_to_update] for k, v in last_news_dumper.data.items()}
            all_events_db.put_in_db(data_dict_update)
    update_with_new_events(announce_id_not_in_db)

    # Update rates comments
    db_rates_comments = all_events_db.fetch_by_keys('eventcomment_id', 'rates', 'comments')
    parsed_rates_comments = last_news_dumper.data_by_keys('eventcomment_id', 'rates', 'comments')
    all_events_db.update_rates_comments(parsed_rates_comments)


'''
def from_records_to_dict(data_recs):
    keys = data_recs[0].keys()
    data_dict= {}
    vals = list(zip(*[list(r.values()) for r in data_recs]))
    for k, v in zip(keys, vals):
        data_dict[k] = list(v)
    return data_dict

def fetch_5_comments_from_db(eventcomment_id):
    deta = Deta('a0pdaatz93v_Afj1YuzAhPdmmBm6qJNUoK4LvnnborNS')
    db = deta.Base('news')
    return db.fetch({'eventcomment_id':str(eventcomment_id)}).items[:5]

def prepare_comments_to_send(data_to_send):
    col_to_send = ['comment_id', 'author_steam_id', 'comment_author', 'comment_timestamp', 'comment_text']
    data_to_send = {key: from_records_to_dict(data_to_send)[key] for key in col_to_send}
    data_to_send['time'] = [datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') for ts in data_to_send['comment_timestamp']]
    del data_to_send['comment_timestamp']
    return data_to_send
'''