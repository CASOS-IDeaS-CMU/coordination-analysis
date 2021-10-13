import os
import json
import gzip
import datetime
import pandas as pd
from tqdm import tqdm
import coordination_helpers
import sliding_window_functions


def check_and_format_input_data(input_data):
    assert type(input_data) == str
    if input_data.endswith('.json'):
        with open(input_data) as f:
            pass
        input_files = [input_data]
    if input_data.endswith('.json.gz'):
        with gzip.open(input_data) as f:
            pass
        input_files = [input_data]
    elif '.' in input_data:
        raise ValueError(f'input data needs to be .json, .json.gz or a directory got {input_data}')
    else:
        directory = input_data
        if directory[-1] != '/':
            directory = directory + '/'
        input_files = []
        for filename in os.listdir(directory):
            if filename.endswith('.json') or filename.endswith('.json.gz'):
                input_files.append(directory + filename)
        if len(input_data) == 0:
            raise ValueError('input directory needs to contain .json or .json.gz files')
    return input_files


def check_output_file(output_file):
    assert(output_file.endswith('.csv'))
    with open(output_file, 'w') as f:
        pass
    return


def parse_tweets_v1(filenames):
    processed = set()
    names = {}
    users, times, links, types, ids = [], [], [], [], []
    for filename in tqdm(filenames, desc='processing file'):
        if filename.endswith('.json'):
            opener = open
        else:
            opener = gzip.open
        with opener(filename) as f:
            for line in f:
                try:
                    tweet = json.loads(line)
                except:
                    continue
                if (len(tweet) <= 1) or (tweet['id'] in processed) or ('retweeted_status' in tweet):
                    continue
                processed.add(tweet['id'])
                usrs, ts, ls, tys, tids, names = coordination_helpers.processTweet(tweet, names)
                users.extend(usrs)
                times.extend(ts)
                links.extend(ls)
                types.extend(tys)
                ids.extend(tids)
    df = pd.DataFrame({'tweet_id':ids, 'user_id':users, 'time':times, 'link':links, 'link_type':types})
    df['screen_name'] = df['user_id'].apply(lambda x: names[x])
    print(f'parsed {len(processed)} tweets.')
    return df

def parse_tweets_v2(filenames):
    raise NotImplementedError('V2 Support Coming Soon!')

def get_synchronized_actions(action_df, window, action_types):
    synchronized_edges = pd.DataFrame()
    for link_type, type_df in action_df.groupby('link_type'):
        if link_type not in action_types:
            continue
        print(f'finding edges from sychronized {link_type} use')
        edges = sliding_window_functions.get_edges(type_df, window=window, source='screen_name')
        edge_df = coordination_helpers.edge2df(edges)
        edge_df['synchronized_action_type'] = link_type
        synchronized_edges = synchronized_edges.append(edge_df)
    return synchronized_edges


def main(input_data, output_data, api_version, time_window_minutes, action_types):
    filenames = check_and_format_input_data(input_data)

    assert time_window_minutes > 0
    window = datetime.timedelta(minutes=time_window_minutes)

    check_output_file(output_data)

    if api_version == 'V1':
        action_df = parse_tweets_v1(filenames)
    elif api_version == 'V2':
        action_df = parse_tweets_v2(filenames)
    else:
        raise ValueError(f"API version needs to be either 'V1' or 'V2', got {api_version}")

    synchronized_edges = get_synchronized_actions(action_df, window, action_types)
    synchronized_edges.to_csv(output_data, index=False)
    return

def read_config():
    with open('CONFIG.json') as f:
        config = json.load(f)
    input_data = config['input_data']
    api_version = config['api_version']
    time_window_minutes = config['time_window_minutes']
    output_data = config['output_data']
    action_types = config['action_types']
    return input_data, api_version, time_window_minutes, output_data, action_types

if __name__ == '__main__':
    input_data, api_version, time_window_minutes, output_data, action_types = read_config()
    main(input_data, output_data, api_version, time_window_minutes, action_types)

