import pandas as pd
import coordination_helpers 
import sliding_window_functions
import os 
import json 
import datetime
from tqdm import tqdm

'''
hashtag, url, url-hashtag output for twitter v2 structure
'''

users, times, links, types, ids = [], [], [], [], []
names = {}
processed = set()

def get_edges(df, window, source='user_id', target='link', time='time', weighted=False):
    edges = {}
    for behavior, behavior_df in tqdm(df.groupby(target)):
        sorted_behavior_df = behavior_df.sort_values(time)
        times = sorted_behavior_df[time].tolist()
        labels = sorted_behavior_df[source].tolist()
        behavior_edges = sliding_window_functions.get_pairs(times, labels, window, weighted)
        for edge, weight in behavior_edges.items():
            edges[edge] = edges.get(edge, 0) + weight
    return edges

data_dir = './data'
output_dir = './coordination_output'

for filename in tqdm(os.listdir(data_dir)):
    if filename.endswith('.json'):
        print(filename)
        full_filename = os.path.join(data_dir, filename)
        with open(full_filename, 'r', encoding='utf-8') as f: 
            json_data = json.load(f)
            tweet_data = json_data['data']
            user_data = json_data['includes']['users']
            
            for tweet in tqdm(tweet_data):
                is_retweet = False
                try:
                    is_retweet = tweet['referenced_tweets'][0]['type']
                    if is_retweet == 'retweeted':
                        is_retweet = True
                except:
                    pass
                
                if tweet['id'] in processed:
                    continue
                processed.add(tweet['id'])
                us, ts, ls, tys, names, tids = coordination_helpers.processTweet_v2(tweet, names, user_data)
                users += us
                times += ts
                links += ls
                types += tys
                ids += tids

df = pd.DataFrame({'tweet_id':ids, 'user_id':users, 'time':times, 'link':links, 'link_type':types})
df['screen_name'] = df['user_id'].apply(lambda x: names[x])

name_dict = dict(zip(df['screen_name'], df['screen_name']))
url_orig = df[df['link_type'] == 'url']
url_df_raw = get_edges(url_orig, window=datetime.timedelta(minutes=5), source='screen_name')
url_df = coordination_helpers.edge2df(url_df_raw, name_dict)
url_df.to_csv(os.path.join(output_dir, 'urls_id.csv'), index=False)

hashtag_orig = df[df['link_type'] == 'hashtag']
hashtag_df_raw = get_edges(hashtag_orig, window=datetime.timedelta(minutes=5), source='screen_name')
hashtag_df = coordination_helpers.edge2df(hashtag_df_raw, name_dict)
hashtag_df.to_csv(os.path.join(output_dir, 'hashtag_id.csv'), index=False)

url_hashtag_orig = df[df['link_type'] == 'url-hashtag']
url_hashtag_df_raw = get_edges(url_hashtag_orig, window=datetime.timedelta(minutes=5), source='screen_name')
url_hashtag_df = coordination_helpers.edge2df(hashtag_df_raw, name_dict)
url_hashtag_df.to_csv(os.path.join(output_dir, 'url_hashtag_id.csv'), index=False)