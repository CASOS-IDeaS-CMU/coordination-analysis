import pandas as pd
import coordination_helpers 
import sliding_window_functions_fullinfo
import os 
import json 
import datetime
from tqdm import tqdm

users, times, links, types, ids, fulltexts = [], [], [], [], [], []
names = {}
processed = set()

data_dir = r'C:\Users\lynne\Documents\Synchronization\MAISON\code\coordination_db\data_test'
output_dir_base = r'C:\Users\lynne\Documents\Synchronization\MAISON\code\coordination_db\outputs'

time_window_array = [5]

def get_edges(df, window, source='user_id', target='link', time='time', tweetid='tweet_id', fulltexts='texts', weighted=False):
    #edges = {}
    all_edges = []
    for behavior, behavior_df in df.groupby(target):
        sorted_behavior_df = behavior_df.sort_values(time)
        times = sorted_behavior_df[time].tolist()
        labels = sorted_behavior_df[source].tolist()
        tweet_ids = sorted_behavior_df[tweetid].tolist()
        texts = sorted_behavior_df[fulltexts].tolist()
        behavior_edges = sliding_window_functions_fullinfo.get_pairs(times, labels, tweet_ids, texts, window, weighted)
        #behavior_edges = sliding_window_functions.get_pairs(times, labels, window, weighted)
        # for edge, weight in behavior_edges.items():
        #     edges[edge] = edges.get(edge, 0) + weight
        #print(behavior_edges)
        if len(behavior_edges) > 0:
            #all_edges.append(behavior_edges)
            for edge, item in behavior_edges.items():
                all_edges.append(item)
    #print(all_edges)
    return all_edges 

for w in time_window_array:
    print('time window ', w)

    time_window = w
    #output_dir = output_dir_base + str(time_window) + 'min/'
    output_dir = output_dir_base

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
                            us, ts, ls, tys, names, tids, ftexts = coordination_helpers.processTweet(tweet, names)
                            users += us
                            times += ts
                            links += ls
                            types += tys
                            ids += tids
                            fulltexts += ftexts
                    except:
                        pass


            df = pd.DataFrame({'tweet_id':ids, 'user_id':users, 'time':times, 'link':links, 'link_type':types, 'texts': fulltexts})
            df['screen_name'] = df['user_id'].apply(lambda x: names[x])

            name_dict = dict(zip(df['screen_name'], df['screen_name']))
            url_orig = df[df['link_type'] == 'url']
            url_df_raw = get_edges(url_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            url_output_file = os.path.join(output_dir, filename.replace('.json','_') + 'urls_id.json')
            url_fh = open(url_output_file, 'w', encoding='utf-8')
            for edge in url_df_raw:
                json.dump(edge, url_fh)
                url_fh.write('\n')
            url_fh.close()

            hashtag_orig = df[df['link_type'] == 'hashtag']
            hashtag_df_raw = get_edges(hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            hashtags_output_file = os.path.join(output_dir, filename.replace('.json','_') + 'hashtag_id.json')
            hashtag_fh = open(hashtags_output_file, 'w', encoding='utf-8')
            for edge in hashtag_df_raw:
                json.dump(edge, hashtag_fh)
                hashtag_fh.write('\n')
            hashtag_fh.close()

            url_hashtag_orig = df[df['link_type'] == 'url-hashtag']
            url_hashtag_df_raw = get_edges(url_hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            hashtags_url_output_file = os.path.join(output_dir, filename.replace('.json','_') + 'url_hashtag_id.json')
            hashtag_url_fh = open(hashtags_url_output_file, 'w', encoding='utf-8')
            for edge in url_hashtag_df_raw:
                try:
                    json.dump(edge, hashtag_url_fh)
                    hashtag_url_fh.write('\n')
                except:
                    print(edge)
            hashtag_url_fh.close()

            mentions_hashtag_orig = df[df['link_type'] == 'mentions']
            mentions_df_raw = get_edges(mentions_hashtag_orig, window=datetime.timedelta(minutes=time_window), source='screen_name')
            mentions_output_file = os.path.join(output_dir, filename.replace('.json','_') + 'mentions_id.json')
            mentions_fh = open(mentions_output_file, 'w', encoding='utf-8')
            for edge in mentions_df_raw:
                json.dump(edge, mentions_fh)
                mentions_fh.write('\n')
            mentions_fh.close()