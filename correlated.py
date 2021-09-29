import os
import pandas as pd
from urllib.parse import urlsplit
import numpy as np
import datetime
from collections import Counter

def getLink(l, r):
    if l < r:
        link = (l, r)
    else:
        link = (r, l)
    return link

def getWEdgeFast(df, window, source='source', target='target', time='time', tfidf=False):
    grouped = df.groupby(target)
    links = Counter()
    N = df[target].nunique()
    for target, small_df in grouped:
        if len(small_df) == 1:
            continue
        small_df.sort_values(time, inplace=True)
        if tfidf:
            links = getWindowedEdgesPreprocessedTFIDF(small_df, window, links, N, source, time)
        else:
            links = getWindowedEdgesPreprocessed(small_df, window, links, source, time)
    return links


def getWindowedEdgesPreprocessed(df, window, timeLinks, source='source', time='time'):
    sources = list(df[source])
    sources = [int(x) for x in sources]
    times = list(df[time])

    left = 0
    for i in range(1, len(times)):
        while (times[i] - times[left] > window) and (left <= i):
            for j in range(left + 1, i):
                link = getLink(sources[left], sources[j])
                timeLinks.update([link])
            left += 1
    for j in range(left, len(times)): #i):
        for right in range(j + 1, len(times) + 1): #i + 1):
            link = getLink(sources[left], sources[j])
            timeLinks.update([link])
    return timeLinks


def getWindowedEdgesPreprocessedTFIDF(df, window, timeLinks, N, source='source', time='time'):
    sources = list(df[source])
    sources = [int(x) for x in sources]
    times = list(df[time])

    left = 0
    N = len(sources)
    for i in range(1, len(times)):
        while (times[i] - times[left] > window) and (left <= i):
            for j in range(left + 1, i):
                link = getLink(sources[left], sources[j])
                instances = i - left
                timeLinks[link] += N / instances # np.log(N / instances) ** 2
            left += 1
    for j in range(left, len(times)): #i):
        for right in range(j + 1, len(times) + 1): #i + 1):
            link = getLink(sources[left], sources[j])
            instances = i - left
            if instances == 0:
                break #fix later
            timeLinks[link] += N /  instances # np.log(N / instances) ** 2
            timeLinks.update([link])
    return timeLinks


def printSum(edges):
    print(f'{len(edges)} unique edges')
    s, t = zip(*edges.keys())
    users = set(s)
    users.update(t)
    print(f'{len(users)} unique users')
    print(f'max weight {max(edges.values())}')
    print(f'{sum(edges.values())} total correlated events')
    print()

def subset_users(df, net, thresh=5):
    small_df = df[df['link_type'] == 'tweet']
    counts = small_df[small_df['network'] == net]['source'].value_counts() >= thresh
    subset = set()
    for user, above in counts.items():
        if above:
            subset.add(user)
    return subset
