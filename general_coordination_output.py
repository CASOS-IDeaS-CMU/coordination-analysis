import pandas as pd
import coordination_helpers 
import sliding_window_functions
import os 
import json 
import datetime
from tqdm import tqdm

users, times, links, types, ids = [], [], [], [], []
names = {}
processed = set()

data_dir = '/storage3/lynnette/us_election_2021'
output_dir_base = '/storage3/lynnette/us_election_2021/coordination_outputs/'

time_window_array = [5, 10, 15, 20]

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

for w in time_window_array:
    print('time window ', w)

    time_window = w
    output_dir = output_dir_base + str(time_window) + 'min/'

    for filename in tqdm(os.listdir(data_dir), desc='processing file'):
        if filename.endswith('.json'):
            full_filename = os.path.join(data_dir, filename)
            with open(full_filename, 'r', encoding='utf-8') as f: 
                print(filename)
                for line in tqdm(f, desc='processing tweets'):
                    try:
                        tweet = json.loads(line.strip())
                        if 'retweeted_status' not in tweet:
                            if tweet['id'] in processed:
                                continue
                            processed.add(tweet['id'])
                            us, ts, ls, tys, names, tids = coordination_helpers.processTweet(tweet, names)
                            users += us
                            times += ts
                            links += ls
                            types += tys
                            ids += tids
                    except:
                        pass


            df = pd.DataFrame({'tweet_id':ids, 'user_id':users, 'time':times, 'link':links, 'link_type':types})
            df['screen_name'] = df['user_id'].apply(lambda x: names[x])

            name_dict = dict(zip(df['screen_name'], df['screen_name']))
            url_orig = df[df['link_type'] == 'url']
            url_df_raw = get_edges(url_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            url_df = coordination_helpers.edge2df(url_df_raw, name_dict)
            url_df.to_csv(os.path.join(output_dir, filename.replace('.json','_') + 'urls_id.csv'), index=False)

            hashtag_orig = df[df['link_type'] == 'hashtag']
            hashtag_df_raw = get_edges(hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            hashtag_df = coordination_helpers.edge2df(hashtag_df_raw, name_dict)
            hashtag_df.to_csv(os.path.join(output_dir, filename.replace('.json','_') + 'hashtag_id.csv'), index=False)

        url_hashtag_orig = df[df['link_type'] == 'url-hashtag']
        url_hashtag_df_raw = get_edges(url_hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
        url_hashtag_df = coordination_helpers.edge2df(hashtag_df_raw, name_dict)
        url_hashtag_df.to_csv(os.path.join(output_dir, filename.replace('.json','_') + 'url_hashtag_id.csv'), index=False)

        mentions_hashtag_orig = df[df['link_type'] == 'mentions']
        mentions_df_raw = get_edges(mentions_hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
        mentions_df = coordination_helpers.edge2df(mentions_df_raw, name_dict)
        mentions_df.to_csv(os.path.join(output_dir, filename.replace('.json','_') + 'mentions_id.csv'), index=False)