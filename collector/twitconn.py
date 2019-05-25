import tweepy
import time
import logging

from typing import Dict, List
from collector import dbconn

from requests.exceptions import Timeout, ConnectionError
from urllib3.exceptions import ReadTimeoutError, ProtocolError
from ssl import SSLError


CLIENT_KEY = 'client_key'
CLIENT_SECRET = 'client_secret'
ACCESS_TOKEN = 'access_token'
ACCESS_SECRET = 'access_secret'


class TweetListener(tweepy.StreamListener):

    def __init__(self, db):
        super(TweetListener, self).__init__()
        self.statuses = list()
        self.db = db

    def on_status(self, status):
        self.statuses.append(status)

    def on_timeout(self):
        pass

    def on_connect(self):
        pass

    def get_statuses(self) -> List:
        statuses = self.statuses[:]
        self.statuses.clear()
        return statuses


class TwitConn:

    def __init__(self, keys: Dict[str, str]):
        self.poster = None
        self.following = False
        self.auth = None
        self.api_twitter = None
        self.listener = None

        self.db = dbconn.DBConn()

        self._init_twitter(keys)
        self._init_listener()

    def _init_twitter(self, keys: Dict[str, str]):
        c_key = keys[CLIENT_KEY]
        c_secret = keys[CLIENT_SECRET]
        a_token = keys[ACCESS_TOKEN]
        a_secret = keys[ACCESS_SECRET]

        self.auth = tweepy.OAuthHandler(c_key, c_secret)
        self.auth.set_access_token(a_token, a_secret)

        self.api_twitter = tweepy.API(self.auth)

    def _init_listener(self):
        self.listener = TweetListener(self.db)

    def init_stream(self, ids: List[int]=[], hashtags: List[str]=[]) -> None:
        self.poster = tweepy.Stream(auth=self.auth, listener=self.listener)
        self.poster.filter(follow=ids, track=hashtags, is_async=True)
        self.following = True

    def init_sample(self) -> None:
        self.poster = tweepy.Stream(auth=self.auth, listener=self.listener)
        self.following = True
        while True:
            try:
                self.poster.sample()
            except (Timeout, SSLError, ReadTimeoutError, ConnectionError, ProtocolError) as e:
                logging.warning("Network error occurred. Keep calm and carry on.", str(e))
            except Exception as e:
                logging.error("Unexpected error occured.", str(e))
            finally:
                logging.info("Stream crashed. Restarting stream.")

            time.sleep(10)

    def get_tweets(self) -> List:
        return self.listener.get_statuses()

    def save_user(self, user_id: int):
        user = self.api_twitter.get_user(user_id)

        self.db.update_user(user)

    def save_tweet(self, status_id: int):
        status = self.api_twitter.get_status(status_id)

        self.db.update_tweet(status)

    def save_stored_tweets(self):
        tweets = self.get_tweets()

        for tweet in tweets:
            self.db.update_tweet(tweet)


