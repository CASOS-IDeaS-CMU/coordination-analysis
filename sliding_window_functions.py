from collections import Counter
import datetime

def get_edges(df, window, source='user_id', target='link', time='time', weighted=False):
    edges = {}
    for behavior, behavior_df in df.groupby(target):
        sorted_behavior_df = behavior_df.sort_values(time)
        times = sorted_behavior_df[time].tolist()
        labels = sorted_behavior_df[source].tolist()
        behavior_edges = get_pairs(times, labels, window, weighted)
        for edge, weight in behavior_edges.items():
            edges[edge] = edges.get(edge, 0) + weight
    return edges


def get_pairs(times, labels, window, weighted=False):
    assert(len(times) == len(labels))
    if len(labels) <= 2:
        return {}
    assert(window > datetime.timedelta(seconds=0))
    left = 0
    target = 0
    right = 0
    all_edges = {}
    addAllEndFlag = False
    while target < len(labels):
        while times[target] - times[left] > window:
            left += 1

        while (right != len(times) -1) and (times[right + 1] - times[target] <= window):
            right += 1
        if right == len(labels) - 1:
            addAllEndFlag = True
            break

        counts = Counter(labels[left: right + 1])
        if weighted:
            edge_weight = 1 / len(counts)
        else:
            edge_weight = 1
        target_node = labels[target]
        target_count = counts[target_node]
        for source_node, source_count in counts.items():
            if source_node != target_node:
                if target_count <= source_count:
                    sorted_edge = tuple(sorted([source_node, target_node]))
                    all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + edge_weight

        target += 1

    if addAllEndFlag:
        #add all links from target to end
        for i in range(left, len(labels) -1):
            counts = Counter(labels[i:])
            if weighted:
                edge_weight = 1 / len(counts)
            else:
                edge_weight = 1
            target_node = labels[i]
            target_count = counts[target_node]
            for source_node, source_count in counts.items():
                if source_node != target_node:
                    if target_count <= source_count:
                        sorted_edge = tuple(sorted([source_node, target_node]))
                        all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + edge_weight

    return all_edges