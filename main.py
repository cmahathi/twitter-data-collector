from threading import Thread

from collector import utils, twitconn


KEYS_JSON = 'keys.json'
SETTINGS_JSON = 'settings.json'


if __name__ == "__main__":
    keys = utils.load_keys()
    tc = twitconn.TwitConn(keys['twitter'])
    settings = utils.load_settings()

    sample = Thread(target=tc.init_sample)
    sample.start()
    while True:
        tc.save_stored_tweets()


