import pandas as pd
import numpy as np
import json
import datetime
import os
import gzip
import sliding_window_functions


def get_edges(df, window, source='user_id', target='link', time='time', weighted=False):
    edges = {}
    for behavior, behavior_df in df.groupby(target):
        sorted_behavior_df = behavior_df.sort_values(time)
        times = sorted_behavior_df[time].tolist()
        labels = sorted_behavior_df[source].tolist()
        behavior_edges = sliding_window_functions.get_pairs(times, labels, window, weighted)
        for edge, weight in behavior_edges.items():
            edges[edge] = edges.get(edge, 0) + weight
    return edges


def get_urls(tweet):
    urls = [x['expanded_url'] for x in tweet['entities']['urls']]
    if 'quoted_status_id_str' in tweet:
        if 'quoted_status_permalink' in tweet:
            quote_url = tweet['quoted_status_permalink']['expanded']
            urls = [x for x in urls if x != quote_url]
        elif 'quoted_status' in tweet:
            quoted_status_id = tweet['quoted_status']['id_str']
            urls = [x for x in urls if not (x.endswith(quoted_status_id) or x.split('?')[0].endswith(quoted_status_id))]
        else:
            quoted_status_id = tweet['quoted_status_id_str'] 
            urls = [x for x in urls if not (x.endswith(quoted_status_id) or x.split('?')[0].endswith(quoted_status_id))]
    return urls

def get_urls_v2(tweet):
    urls = []
    if 'entities' in tweet:
        if 'urls' in tweet['entities']:
            urls = [x['expanded_url'] for x in tweet['entities']['urls']]
    return urls

def get_hashtags(tweet):
    hashtags = [x['text'].lower() for x in tweet['entities']['hashtags']]
    hashtags = list(np.unique(hashtags))
    return hashtags

def get_hashtags_v2(tweet):
    hashtags = []
    if 'entities' in tweet:
        if 'hashtags' in tweet['entities']:
            hashtags = [x['tag'].lower() for x in tweet['entities']['hashtags']]
            hashtags = list(np.unique(hashtags))
    return hashtags

def get_mentions(tweet):
    mentions = [x['screen_name'] for x in tweet['entities']['user_mentions']]
    mentions = list(np.unique(mentions))
    return mentions

def expandTweets(tweet):
    #get all but retweet
    tweets = []
    if 'retweeted_status' in tweet:
        tweets.append(tweet['retweeted_status'])
        if 'quoted_status' in tweet['retweeted_status']:
            tweets.append(tweet['retweeted_status']['quoted_status'])
    else:
        tweets.append(tweet)
        if 'quoted_status' in tweet:
            tweets.append(tweet['quoted_status'])
    return tweets
        
def getTime(tweet):
    created_at_format = '%a %b %d %H:%M:%S %z %Y'
    time = datetime.datetime.strptime(tweet['created_at'], created_at_format)
    return time


def getTime_v2(tweet):
    created_at_format = '%Y-%m-%dT%H:%M:%SZ'
    tweet_created_at = tweet['created_at'].replace('.000', '')
    time = datetime.datetime.strptime(tweet_created_at, created_at_format)
    return time


def processTweet(tweet, nameLookup):
    user = tweet['user']['id']
    nameLookup[user] = tweet['user']['screen_name']
    time = getTime(tweet)
    urls = get_urls(tweet)
    hashtags = get_hashtags(tweet)
    mentions = get_mentions(tweet)
    combined = []
    for url in urls:
        for hashtag in hashtags:
            combined.append((url, hashtag))
    links = urls + hashtags + mentions + combined
    types = ['url'] * len(urls) + ['hashtag'] * len(hashtags) + ['mentions'] * len(mentions) + ['url-hashtag'] * len(combined)
    users = [user] * len(types)
    times = [time] * len(types)
    ids = [tweet['id']] * len(types)
    return users, times, links, types, ids, nameLookup 


def processTweet_v2(tweet, names, user_data):
    user = tweet['author_id']
    names[user] = list(filter(lambda user_data: user_data['id'] == user, user_data))[0]['username']
    time = getTime_v2(tweet)
    urls = get_urls_v2(tweet)
    hashtags = get_hashtags_v2(tweet)
    combined = []
    for url in urls:
        for hashtag in hashtags:
            combined.append((url, hashtag))
    links = urls + hashtags + combined
    types = ['url'] * len(urls) + ['hashtag'] * len(hashtags) + ['url-hashtag'] * len(combined)
    users = [user] * len(types)
    times = [time] * len(types)
    ids = [tweet['id']] * len(types)
    return users, times, links, types, names, ids


def edge2df(edges, name_dict=None):
    edge_df = pd.DataFrame(edges.keys(), columns=['source', 'target'])
    edge_df['weight'] = edges.values()
    if name_dict:
        edge_df['source'] = edge_df['source'].apply(lambda x: name_dict[x])
        edge_df['target'] = edge_df['target'].apply(lambda x: name_dict[x])
    edge_df = edge_df[edge_df['source'] != edge_df['target']]
    edge_df.reset_index(drop=True, inplace=True)
    return edge_df

