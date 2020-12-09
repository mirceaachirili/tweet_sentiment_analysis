# Import tweepy library - for connecting to Twitter API
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream, API

# Import library for PostgreSQL database
import psycopg2

import time
import credentials  # Twitter API credentials


class twitterStrListener(StreamListener):
    '''
    Stream tweets through Twitter API and store to database.
    '''

    def __init__(self, time_limit=200):
        self.start = time.time()
        self.limit = time_limit # default time limit
        super(twitterStrListener, self).__init__()

    def on_connect(self):
        print('Connected to Twitter API.')

    def on_status(self, status):
        '''
        Get relevant tweet information.
        '''
        # Tweet id
        tweet_id = status.id 

        # User id
        user_id = status.user.id 

        # Username
        username = status.user.name

        # Tweet text
        if status.truncated == True:  # retrieve full text if tweet is exceeding 140 characters
            tweet = status.extended_tweet['full_text']
            hashtags = status.extended_tweet['entities']['hashtags']
        else:
            tweet = status.text
            hashtags = status.entities['hashtags']
        
        # Read hashtags
        hashtags = [tag['text'] for tag in hashtags]
        
        # Datetime information
        datetime = status.created_at

        # User's followers count
        followers_count = status.user.followers_count

        # Language
        language = status.lang

        # If tweet is not a retweet and language used is english
        if not hasattr(status, "retweeted_status") and language == 'en':
            # Connect to database
            database_connect(user_id, username, tweet_id, tweet, datetime, hashtags, followers_count)

        if (time.time() - self.start) > self.limit:
            print('Time limit reached')
            return False

    def on_error(self, status_code):
        if status_code == 420:
            # Disconnect stream if API limit reached
            return False



def database_connect(user_id, username, tweet_id, tweet, datetime, hashtags, followers_count):
    '''
    Input: 
        user_id - Unique user id (int)
        username - Unique username (string)
        tweet_id - Unique tweet id (int)
        datetime - timestamp in format YYYY-MM-DD HH:MM:SS
        hashtags - list of strings of hashtags in tweet 
        followers_count - number of user followers (int)
    Output:
        Connect to database and insert values to tables
        
    '''
    # Connect to postgresql database
    conn = psycopg2.connect(database='TwitterDB', user=credentials.pg_user, password=credentials.pg_pass)

    # Create cursor and execute SQL queries
    cur = conn.cursor()
    
    # Insert user information
    query = '''
            INSERT INTO twitter_user (user_id, username, followers_count) 
            VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING;
            '''
    cur.execute(query, (user_id, username, followers_count))

    # Insert tweet information
    query = '''
            INSERT INTO tweets (tweet_id, user_id, tweet, datetime) 
            VALUES (%s, %s, %s, %s);
            '''
    cur.execute(query, (tweet_id, user_id, tweet, datetime))
    
    # Insert tweet entity information
    for i in range(len(hashtags)):
        hashtag = hashtags[i]
        query = '''
            INSERT INTO twitter_entity (tweet_id, hashtags) 
            VALUES (%s, %s);
            '''
        cur.execute(query, (tweet_id, hashtag))

    # Commit changes
    conn.commit()

    # Disconnect
    cur.close()
    conn.close()
    

def create_db_tables(database, table_queries):
    '''
    Input: 
        database - database name (string)
        table_queries - SQL queries
        
    Output:
        Connect to database and create tables.
    '''
    # Connect to postgresql database
    conn = psycopg2.connect(database='TwitterDB', user=credentials.pg_user, password=credentials.pg_pass)

    # Create cursor and execute SQL queries
    cur = conn.cursor()

    # Create tables in Postgres
    for query in table_queries:
        cur.execute(query)
    
    # Close connection with server
    conn.commit()
    cur.close()
    conn.close()


# Queries for creating tables in database
    table_queries = (
        # First table
        ''' 
        CREATE TABLE twitter_user(user_id BIGINT PRIMARY KEY,
                                username TEXT,
                                followers_count BIGINT);
        ''',

        # Second table
        '''
        CREATE TABLE tweets(tweet_id BIGINT PRIMARY KEY,
                            user_id BIGINT,
                            tweet TEXT,
                            datetime TIMESTAMP,
                            CONSTRAINT 
                                fk_user FOREIGN KEY(user_id) REFERENCES twitter_user(user_id)); 
        ''',
        '''
        CREATE TABLE twitter_entity(id SERIAL PRIMARY KEY,
                                    tweet_id BIGINT,
                                    hashtags TEXT,
                                    CONSTRAINT 
                                            fk_user FOREIGN KEY(tweet_id) REFERENCES tweets(tweet_id));
        '''
        )


if __name__ == '__main__':
    #Authorize twitter api key
    auth = OAuthHandler(credentials.consumer_key, credentials.consumer_secret)

    # Authorize user access tokes 
    auth.set_access_token(credentials.access_token, credentials.access_token_secret)

    # API call
    api = API(auth)

    # Stream tweets
    twitter_str_listener = twitterStrListener(time_limit=20000)
    twitter_stream = Stream(auth=api.auth, listener=twitter_str_listener)
    twitter_stream.filter(track=['tesla'])



