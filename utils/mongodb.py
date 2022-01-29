from pymongo.errors import BulkWriteError
import logging
import time
import tqdm
import tweepy

logging.basicConfig(format='[%(asctime)s] - %(name)s - %(funcName)s - %(levelname)s : %(message)s', level=logging.INFO)
log = logging.getLogger(__name__)

def bulk_write_to_mongo(collection, data):
    to_insert = len(data)
    try:
        if to_insert > 0:
            collection.insert_many(data, ordered=False)
        return to_insert, 0
    except BulkWriteError as e:
        log.error("BulkWriteError")
        inserted = e.details["nInserted"]
        return inserted, to_insert - inserted

def download_timeline(user_id: str, n: int = 3200, count: int = 200, trim_user=True, tweet_mode="extended", **kwargs):
    log.info(f'Downloading timeline from user id: {user_id}') 
    start_time = time.time()
    tweets = [status for status in tqdm.tqdm(tweepy.Cursor(
        api.user_timeline, 
        user_id=user_id, 
        count=count, 
        trim_user=trim_user, 
        tweet_mode=tweet_mode,
        **kwargs).items(n), total=n)]
    total_time = time.time()  - start_time
    log.info(f"Downloaded finished: {len(tweets)} tweets in {total_time:.4f} seconds.")
    return tweets