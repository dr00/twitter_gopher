#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
from gopherbot import GopherBot
from twitterbotglobals import *

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base

from datetime import datetime, timedelta
from email.utils import parsedate_tz

import sqlite3
import sys
import argparse
import os
import time
import re

Base = declarative_base()

class MediaEntity(Base):
    """
    MediaEntity class/table that represents media in tweets.
    """
    __tablename__ = 'media_entities'
    id = Column(Integer, primary_key=True)
    id_str = Column(String, nullable=False)
    url = Column(String)
    display_url = Column(String) 
    expanded_url = Column(String)
    tweet_id = Column(Integer)

    def __init__(self, tweet_id, entity_json):
        if entity_json:
            self.id = entity_json['id']
            self.id_str = entity_json['id_str']
            self.url = entity_json['url']
            self.display_url = entity_json['display_url']
            self.expanded_url = entity_json['expanded_url']
            self.tweet_id = tweet_id

    def __repr__(self):
        id = self.id
        url = self.url
        tid = self.tweet_id
        r = u"<MediaEntity('{}', '{}', '{}')>>".format(id, url, tid)
        return r

class UrlEntity(Base):
    """
    UrlEntity class/table that represents urls in tweets.
    """
    __tablename__ = 'url_entities'
    url = Column(String, primary_key=True)
    tweet_id = Column(Integer, primary_key=True)
    display_url = Column(String) 
    expanded_url = Column(String)


    def __init__(self, tweet_id, entity_json):
        if entity_json:
            self.url = entity_json['url']
            self.display_url = entity_json['display_url']
            self.expanded_url = entity_json['expanded_url']
            self.tweet_id = tweet_id
    
    def __repr__(self):
        url = self.url
        tid = self.tweet_id
        r = u"<UrlEntity('{}', '{}')>>".format(url, tid)
        return r

class UserMentionEntity(Base):
    """
    UserMention class/table that represents mentions in tweets.
    """
    __tablename__ = 'usermention_entities'
    id = Column(Integer, primary_key=True)
    id_str = Column(String, nullable=False)
    screen_name = Column(String)
    name = Column(String)
    tweet_id = Column(Integer)

    def __init__(self, tweet_id, entity_json):
        if entity_json:
            self.id = entity_json['id']
            self.id_str = entity_json['id_str']
            self.screen_name = entity_json['screen_name']
            self.name = entity_json['name']
            self.tweet_id = tweet_id
    
    def __repr__(self):
        id = self.id
        sname = self.screen_name.encode('utf-8', 'ignore')
        tid = self.tweet_id
        r = u"<UserMentionEntity('{}','{}', '{}')>>".format(id, sname, tid)  
        return r

class HashtagEntity(Base):
    """
    HashEntity class/table that represents hashtags in tweets.
    """
    __tablename__ = 'hashtag_entities'
    text = Column(String, primary_key=True)
    tweet_id = Column(Integer, primary_key=True)

    def __init__(self, tweet_id, entity_json):
        if entity_json:
            self.text = entity_json['text']
            self.tweet_id = tweet_id

    
    def __repr__(self):
        tid = self.tweet_id
        text = self.text.encode('utf-8', 'ignore')
        r = u"<HashtagEntity(''{}', '{}')>".format(tid, text)
        return r

class Tweet(Base):
    """
    Tweet class/table that represents tweets by Twitter users.
    """
    __tablename__ = 'tweets'

    id = Column(Integer, primary_key=True)
    id_str = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    favorited = Column(Boolean)
    in_reply_to_screen_name = Column(String)
    in_reply_to_status_id = Column(Integer)
    in_reply_to_status_id_str = Column(String)
    in_reply_to_user_id = Column(Integer)
    in_reply_to_user_id_str = Column(String)
    possibly_sensitive = Column(Boolean)
    retweet_count = Column(Integer, nullable=False, default='0')
    retweeted = Column(Boolean)
    source = Column(String)
    text = Column(String, nullable=False)
    truncated = Column(Boolean)
    user_id = Column(Integer)


    def __init__(self, tweet_json):
        if tweet_json:
            self.id = tweet_json['id']
            self.id_str = tweet_json['id_str']
            self.created_at = to_datetime(tweet_json['created_at'])
            self.favorited = tweet_json['favorited']
            self.in_reply_to_screen_name = tweet_json['in_reply_to_screen_name']
            self.in_reply_to_status_id = tweet_json['in_reply_to_status_id']
            self.in_reply_to_status_id_str = tweet_json['in_reply_to_status_id_str']
            self.in_reply_to_user_id = tweet_json['in_reply_to_user_id']
            self.in_reply_to_user_id_str = tweet_json['in_reply_to_user_id_str']
            self.retweet_count = tweet_json['retweet_count']
            self.retweeted = tweet_json['retweeted']
            self.source = tweet_json['source']
            self.text = tweet_json['text']
            self.truncated = tweet_json['truncated']
            self.user_id = tweet_json['user']['id']

    
    def __repr__(self):
        id = self.id
        text = self.text.encode('utf-8', 'ignore')
        created_at = self.created_at
        r = u"<Tweet('{}','{}', '{}')>".format(id, text, created_at)
        return r


def tweets_insert_all(session, tweets):
    """
    Insert the Tweet objects
    """
    if not session:
        raise RuntimeError('session is empty!')

    if type(tweets) is not list:
        raise TypeError('tweets should be of type list')

    # Don't insert duplicate User.id!
    existing = [id[0] for id in session.query(Tweet.id)]
    new_tweets = [t for t in tweets if t.id not in existing]
    
    if new_tweets:
        commit_tran(session, new_tweets)
    else:
        print('No new tweets to insert!')


def media_insert_all(session, media):
    """
    Insert the MediaEntity objects
    """
    if not session:
        raise RuntimeError('session is empty!')

    if type(media) is not list:
        raise TypeError('media should be of type list')

    # Don't insert duplicate User.id!
    existing = [(m.id, m.tweet_id) for m in session.query(MediaEntity)]
    new_media = [m for m in media if (m.id, m.tweet_id) not in existing]
    
    if new_media:
        commit_tran(session, new_media)
    else:
        print('No new media to insert!')


def usermention_insert_all(session, usermentions):
    """
    Insert the UserMentionEntity objects
    """
    if not session:
        raise RuntimeError('session is empty!')

    if type(usermentions) is not list:
        raise TypeError('usermentions should be of type list')

    # Don't insert duplicate User.id!
    existing = [(m.id, m.tweet_id) for m in session.query(UserMentionEntity)]
    new_mentions = [m for m in media if (m.id, m.tweet_id) not in existing]
    
    if new_mentions:
        commit_tran(session, new_mentions)
    else:
        print('No new usermentions to insert!')


def url_insert_all(session, urls):
    """
    Insert the UrlEntity objects
    """
    if not session:
        raise RuntimeError('session is empty!')

    if type(urls) is not list:
        raise TypeError('urls should be of type list')

    # Don't insert duplicate User.id!
    existing = [(m.url, m.tweet_id) for m in session.query(UrlEntity)]
    new_urls = [u for u in urls if (u.url, u.tweet_id) not in existing]
    
    if new_urls:
        commit_tran(session, new_urls)
    else:
        print('No new new_urls to insert!')


def hashtag_insert_all(session, hashtags):
    """
    Insert the HashtagEntity objects
    """
    if not session:
        raise RuntimeError('session is empty!')

    if type(hashtags) is not list:
        raise TypeError('hashtags should be of type list')

    # Don't insert duplicate User.id!
    existing = [(h.text, h.tweet_id) for h in session.query(Hashtag)]
    new_tags = [h for h in hashtags if (h.text, h.tweet_id) not in existing]
    
    if new_tags:
        commit_tran(session, new_tags)
    else:
        print('No new hashtags to insert!')


def commit_tran(session, orm_obj):
    """
    Commits the transaction agains the database engine.
    """
    if not session:
        raise RuntimeError('session is empty!')

    if not orm_obj:
        raise RuntimeError('No data to insert!')

    try:
        session.add_all(orm_obj)
        session.commit()
        session.flush()
    except Exception as e:
        print('Could not commit transaction!')        
        session.rollback()


def get_timeline(gopher_name, oauth_file, target_name, count=None,
                since_id=None, include_entities=False, max_tries=MAX_TRIES):

    g = GopherBot(gopher_name, oauth_file)
    if not g:
        raise Exception('Could not create GopherBot!')

    
    tries_left = max_tries + 1
    while tries_left:
        if sys.flags.debug:
            print('tries_left:    {}'.format(tries_left))
        timeline = g.get_timeline(target_name, count, since_id, include_entities)
    
        #timestamp = time.strftime('%Y_%m_%d_%H_%M_%S')
        #file_name = 'timeline_{}.json'.format(timestamp)
        #g.save_to_file(str(timeline), file_name, 'w')
    
        tweets = []
        media = []
        urls = []
        mentions = []
        hashtags = []
        for t in timeline:
            tweet = Tweet(t)
            tweets.append(tweet)
            if 'entities' in t:
                entity = t['entities']

                if 'media' in entity:
                    entities = entity['media']
                    media = [MediaEntity(t['id'], m) for m in entities]

                if 'urls' in entity:
                    entities = entity['urls']
                    urls = [UrlEntity(t['id'], u) for u in entities]                    

                if 'user_mentions' in entity:
                    entities = entity['user_mentions']
                    mentions = [UserMentionEntity(t['id'], m) for m in entities]

                if 'hashtags' in entity:
                    entities = entity['hashtags']
                    hashtags = [HashtagEntity(t['id'], m) for m in entities]

        if tweets:
            return {'tweets':tweets, 'media':media, 'urls':urls,\
                    'mentions':mentions, 'hashtags':hashtags}

        tries_left -= 1
    
    if not tries_left:
        raise Exception('Could not get timeline in {} tries'.format(max_tries))


def insert_timeline(db_filename, timeline):
    engine = create_engine(db_filename, echo=ECHO_ON)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    for k in timeline:
        if timeline[k]:
            s = str(timeline[k][0])
            if re.search('<Tweet', s):
                tweets_insert_all(session, timeline[k])
            elif re.search('<MediaEntity', s):
                media_insert_all(session, timeline[k])
            elif re.search('<UrlEntity', s):
                url_insert_all(session, timeline[k])
            elif re.search('<UserMentionEntity', s):
                usermention_insert_all(session, timeline[k])
            elif re.search('<HashtagEntity', s):
                hashtag_insert_all(session, timeline[k])

def get_last_tweet_id(db_filename):

    try:
        engine = create_engine(db_filename, echo=ECHO_ON)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()	          
        max_id = session.query(func.max(Tweet.id)).first()
        return max_id[0]
    except Exception as e:        
        print('Could not get last tweet id')
        print(e)


def process_command_line(argv):
    """
    Returns a Namespace object with the argument names as attributes.
    `argv`` is a list of arguments, or `None` for ``sys.argv[1:]``.
    """

    if argv is None:
        argv = sys.argv[1:]

    # initialize the parse object
    parser = argparse.ArgumentParser(description='Print Twitter statuses.')

    g_help = 'The screen_name of the account to use for getting data'
    parser.add_argument('-g', action='store',
                        dest='gopher_screen_name',
                        default=DEFAULT_GOPHER,
                        help=g_help)

    oa_help = 'The filename that has the oauth_token and oauth_token_secret'
    parser.add_argument('-o', action='store',
                        dest='oauth_filename',
                        default=DEFAULT_OAUTHFILE,
                        help=oa_help)

    d_help = 'The database name to store retrieved data'
    parser.add_argument('-d', action='store',
                        dest='db_filename',
                        default=DEFAULT_DB_FILENAME,
                        help=d_help)

    t_help = 'The screen_name of the account to monitor'
    parser.add_argument('-t', action='store',
                        dest='target_screen_name',
                        default=DEFAULT_TARGET,
                        help=t_help)

    c_help = 'The number of statuses in the timeline to retrieve'
    parser.add_argument('-c', action='store',
                        dest='count',
                        default=MAX_TIMELINE,
                        help=c_help)

    i_help = 'Include entities in tweets'
    parser.add_argument('-i', action='store',
                        dest='include_entities',
                        default=True,
                        help=i_help)

    
    return parser.parse_args()


def _main():
    args = process_command_line(sys.argv)
    gname = args.gopher_screen_name
    ofile = args.oauth_filename
    dfile = 'sqlite:///{0}'.format(args.db_filename)
    tname = args.target_screen_name
    count = args.count
    inc = args.include_entities

    last_id = get_last_tweet_id(dfile)
    timeline = get_timeline(gname, ofile, tname, include_entities=inc, count=count)

    if timeline:
        insert_timeline(dfile, timeline)
    else:
        print("No new tweets in {}'s timeline".format(tname))


if __name__ == '__main__':
    status = _main()
    sys.exit(status)

if __name__ != '__main__':
    args = process_command_line(sys.argv)
    gname = args.gopher_screen_name
    ofile = args.oauth_filename
    dfile = 'sqlite:///{0}'.format(args.db_filename)
    tname = args.target_screen_name
    count = args.count
    inc = args.include_entities
    
    last_id = get_last_tweet_id(dfile)
    
    #timeline = get_timeline(gname, ofile, tname, include_entities=True,count=200)

