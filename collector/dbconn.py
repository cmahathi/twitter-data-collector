import sqlite3
from collector import twitutils, aws
from datetime import datetime
from threading import Thread

ARCHIVE_NAME = 'tweets.log-{year}-{month}-{date}-{hour}-{minute}-{second}'
DB_EXTENSION = 'db'

TWEET_OBJECT_FIELDS = ['id', 'created_at', 'full_text', 'text', 'source', 'in_reply_to_status_id',
                       'in_reply_to_user_id', 'in_reply_to_screen_name', 'user', 'coordinates', 'place',
                       'quoted_status_id', 'retweet_count', 'favorite_count', 'lang']
# 18
TWEET_TABLE_COLUMNS = ['TweetID', 'CreatedAt', 'Text', 'Source', 'ReplyToStatusID', 'ReplyToUserID',
                       'ReplyToScreenName', 'UserID', 'Coordinates', 'Place', 'QuotedStatusID', 'RetweetCount',
                       'FavoriteCount', 'Language', 'Hashtags', 'MentionedUserIDs', 'URLs', 'MediaURLs']
TWEET_DIRECT_MAPPINGS = {
    TWEET_TABLE_COLUMNS[0]: TWEET_OBJECT_FIELDS[0],
    TWEET_TABLE_COLUMNS[1]: TWEET_OBJECT_FIELDS[1],
    # TWEET_TABLE_COLUMNS[2]: TWEET_OBJECT_FIELDS[2],
    TWEET_TABLE_COLUMNS[3]: TWEET_OBJECT_FIELDS[4],
    TWEET_TABLE_COLUMNS[4]: TWEET_OBJECT_FIELDS[5],
    TWEET_TABLE_COLUMNS[5]: TWEET_OBJECT_FIELDS[6],
    TWEET_TABLE_COLUMNS[6]: TWEET_OBJECT_FIELDS[7],
    # UserID: user
    TWEET_TABLE_COLUMNS[8]: TWEET_OBJECT_FIELDS[9],
    TWEET_TABLE_COLUMNS[9]: TWEET_OBJECT_FIELDS[10],
    TWEET_TABLE_COLUMNS[10]: TWEET_OBJECT_FIELDS[11],
    TWEET_TABLE_COLUMNS[11]: TWEET_OBJECT_FIELDS[12],
    TWEET_TABLE_COLUMNS[12]: TWEET_OBJECT_FIELDS[13],
    TWEET_TABLE_COLUMNS[13]: TWEET_OBJECT_FIELDS[14]
    # Hastags: entities-hashtags
    # MentionedUserIDs: entities-user_mentions
    # URLs: entities-urls
    # MediaURLs: extended_entities-media

}

USER_OBJECT_FIELDS = ['id', 'name', 'screen_name', 'location', 'description', 'created_at', 'favourites_count',
                      'statuses_count', 'followers_count', 'friends_count', 'listed_count', 'lang',
                      'profile_background_color', 'profile_background_image_url_https', 'profile_banner_url',
                      'profile_image_url_https', 'profile_link_color', 'profile_sidebar_border_color',
                      'profile_sidebar_fill_color', 'profile_text_color']
USER_TABLE_COLUMNS = ['UserID', 'Name', 'ScreenName', 'Location', 'Description', 'DescriptionURLs', 'CreatedAt',
                      'Favorites', 'Tweets', 'Followers', 'Following', 'ListedCount', 'Language',
                      'ProfileBackgroundColor', 'ProfileBackgroundImageURL', 'ProfileBannerURL', 'ProfileImageURL',
                      'ProfileLinkColor', 'ProfileSidebarBorderColor', 'ProfileSidebarFillColor', 'ProfileTextColor',
                      'URL']
USER_DIRECT_MAPPINGS = {
    USER_TABLE_COLUMNS[0]: USER_OBJECT_FIELDS[0],
    USER_TABLE_COLUMNS[1]: USER_OBJECT_FIELDS[1],
    USER_TABLE_COLUMNS[2]: USER_OBJECT_FIELDS[2],
    USER_TABLE_COLUMNS[3]: USER_OBJECT_FIELDS[3],
    USER_TABLE_COLUMNS[4]: USER_OBJECT_FIELDS[4],
    # DescriptionURLs: entities-description-urls
    USER_TABLE_COLUMNS[6]: USER_OBJECT_FIELDS[5],
    USER_TABLE_COLUMNS[7]: USER_OBJECT_FIELDS[6],
    USER_TABLE_COLUMNS[8]: USER_OBJECT_FIELDS[7],
    USER_TABLE_COLUMNS[9]: USER_OBJECT_FIELDS[8],
    USER_TABLE_COLUMNS[10]: USER_OBJECT_FIELDS[9],
    USER_TABLE_COLUMNS[11]: USER_OBJECT_FIELDS[10],
    USER_TABLE_COLUMNS[12]: USER_OBJECT_FIELDS[11],
    USER_TABLE_COLUMNS[13]: USER_OBJECT_FIELDS[12],
    USER_TABLE_COLUMNS[14]: USER_OBJECT_FIELDS[13],
    USER_TABLE_COLUMNS[15]: USER_OBJECT_FIELDS[14],
    USER_TABLE_COLUMNS[16]: USER_OBJECT_FIELDS[15],
    USER_TABLE_COLUMNS[17]: USER_OBJECT_FIELDS[16],
    USER_TABLE_COLUMNS[18]: USER_OBJECT_FIELDS[17],
    USER_TABLE_COLUMNS[19]: USER_OBJECT_FIELDS[18],
    USER_TABLE_COLUMNS[20]: USER_OBJECT_FIELDS[19]
    # URL: entities-url-urls-expanded_url
}


class DBConn:

    def __init__(self):
        today = datetime.today()

        self.date = today.day
        self.db = None

        self._update_filenames(today)

    def _update_time(self):
        today = datetime.today()

        if today.day != self.date:
            upload = Thread(target=aws.move_to_s3, args=(self.db_name, self.archive_name))
            upload.start()
            self.date = today.day
            self._update_filenames(today)

    def _update_filenames(self, today):
        filename = self._get_filename(today)

        self.db_name = filename + '.db'
        self.archive_name = filename + '.bz2'

        self._connect()

    def _get_filename(self, today):
        year = today.year
        month = today.month
        date = today.day
        hour = today.hour
        minute = today.minute
        second = today.second
        filename = f'tweets.log-{year}-{month}-{date}-{hour}-{minute}-{second}'

        return filename

    def _connect(self):
        if self.db is not None:
            self.db.close()
        self.db = sqlite3.connect(self.db_name)
        cursor = self.db.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS "Tweets" ('
                       '"TweetID"	INTEGER UNIQUE,'
                       '"CreatedAt"	TEXT,'
                       '"Text"	TEXT,'
                       '"Source"	TEXT,'
                       '"ReplyToStatusID"	INTEGER,'
                       '"ReplyToUserID"	INTEGER,'
                       '"ReplyToScreenName"	TEXT,'
                       '"UserID"	INTEGER NOT NULL,'
                       '"Coordinates"	TEXT,'
                       '"Place"	TEXT,'
                       '"QuotedStatusID"	INTEGER,'
                       '"RetweetCount"	INTEGER,'
                       '"FavoriteCount"	INTEGER,'
                       '"Language"	TEXT,'
                       '"Hashtags"	TEXT,'
                       '"MentionedUserIDs"	TEXT,'
                       '"URLs"	TEXT,'
                       '"MediaURLs"	TEXT,'
                       'PRIMARY KEY("TweetID"))')

        cursor.execute('CREATE TABLE IF NOT EXISTS "Users" ('
                       '"UserID"	INTEGER UNIQUE,'
                       '"Name"	TEXT,'
                       '"ScreenName"	TEXT,'
                       '"Location"	TEXT,'
                       '"Description"	TEXT,'
                       '"DescriptionURLs"	TEXT,'
                       '"CreatedAt"	TEXT,'
                       '"Favorites"	INTEGER,'
                       '"Tweets"	INTEGER,'
                       '"Followers"	INTEGER,'
                       '"Following"	INTEGER,'
                       '"ListedCount"	INTEGER,'
                       '"Language"	TEXT,'
                       '"ProfileBackgroundColor"	TEXT,'
                       '"ProfileBackgroundImageURL"	TEXT,'
                       '"ProfileBannerURL"	TEXT,'
                       '"ProfileImageURL"	TEXT,'
                       '"ProfileLinkColor"	TEXT,'
                       '"ProfileSidebarBorderColor"	TEXT,'
                       '"ProfileSidebarFillColor"	TEXT,'
                       '"ProfileTextColor"	TEXT,'
                       '"URL"	TEXT,'
                       'PRIMARY KEY("UserID"))')

    def update_tweet(self, status):
        self._update_time()

        cursor = self.db.cursor()

        data = list()

        for column in TWEET_TABLE_COLUMNS:
            field = TWEET_DIRECT_MAPPINGS.get(column)

            if field is not None:
                data.append(str(getattr(status, field, None)))
            else:
                data.append(None)

        self.update_user(status.user)

        data[2] = twitutils.get_text(status)

        data[7] = status.user.id

        try:
            data[14] = str(status.entities['hashtags'])
        except (KeyError, AttributeError):
            pass

        try:
            data[15] = str([k['id'] for k in status.entities['user_mentions']])
        except (KeyError, AttributeError):
            pass

        try:
            data[16] = str(status.entities['urls'])
        except (KeyError, AttributeError):
            pass

        data[17] = str(twitutils.get_images(status))

        cursor.execute("REPLACE INTO Tweets VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
        self.db.commit()

    def update_user(self, user):
        cursor = self.db.cursor()
        data = list()

        for column in USER_TABLE_COLUMNS:
            field = USER_DIRECT_MAPPINGS.get(column)

            if field is not None:
                data.append(getattr(user, field, None))
            else:
                data.append(None)

        try:
            data[5] = str(user.entities['description']['urls'])
        except (KeyError, AttributeError):
            data[5] = None

        try:
            data[21] = str(user.entities['url']['urls']['expanded_url'])
        except (KeyError, AttributeError):
            data[21] = None

        cursor.execute("REPLACE INTO Users VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", data)
        self.db.commit()

