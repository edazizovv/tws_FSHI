#
import json
import datetime


#
import pandas
from apscheduler.schedulers.background import BlockingScheduler
from sqlalchemy import create_engine


#
from zipper.grab import grab_ts_local
from zipper.select import clean, author_filt_selector
from zipper.post import tg_post


#
sched = BlockingScheduler()


@sched.scheduled_job('interval', id='POST-GTHO', minutes=10)
def total():

    with open('./db_auth.json', 'r') as file:
        db_auth = json.load(file)
        user, password, host, dbname = db_auth['user'], db_auth['password'], db_auth['host'], db_auth['dbname']

    conn = create_engine("postgresql+psycopg2://{0}:{1}@{2}/{3}".format(
        user, password, host, dbname
    )).connect()

    authors = pandas.read_csv('./authors.csv').values[:, 0]

    with open('./tg_auth.json', 'r') as file:
        tg_auth = json.load(file)
        tg_token = tg_auth['tg_token']

    channel_ru = '@twitter_zipper_fnshi_ru'
    channel_en = '@twitter_zipper_fnshi_en'
    channel_lang = 'ru'

    go = True
    while go:
        """
        author, date, text, lang, tweet_id, reply_to_id, reply_to_link, media = author_filt_selector(conn,
                                                                                                channel=channel_ru,
                                                                                                format_lang='ru',
                                                                                                     authors=authors)
        if author:
            tg_post(conn=conn,
                    tg_token=tg_token, author=author, date=date, text=text, in_lang=lang, out_lang=channel_lang,
                    channel=channel_ru, tweet_id=tweet_id, reply_to_id=reply_to_id, reply_to_link=reply_to_link,
                    media=media, format_lang='ru')
        """
        author, date, text, lang, tweet_id, reply_to_id, reply_to_link, media = author_filt_selector(conn,
                                                                                                channel=channel_en,
                                                                                                format_lang='en',
                                                                                                     authors=authors)
        if author:
            tg_post(conn=conn,
                    tg_token=tg_token, author=author, date=date, text=text, in_lang=lang, out_lang=lang,
                    channel=channel_en, tweet_id=tweet_id, reply_to_id=reply_to_id, reply_to_link=reply_to_link,
                    media=media, format_lang='en')
        else:
            go = False


sched.start()

# total()
