from collections import Counter
import datetime

def get_pairs_old(times, labels, window):
    assert(len(times) == len(labels))
    assert(len(labels) > 2)
    assert(window > datetime.timedelta(seconds=0))
    left = 0
    target = 0
    right = 0
    all_edges = {}
    addAllEndFlag = False
    while target < len(labels):
        current_edges = set()
        counts = Counter()

        while times[target] - times[left] > window:
            left += 1
        if left != target:
            counts.update(labels[left:target + 1])
            for i in range(left, target):
                if labels[target] == labels[i]:
                    pass
                else:
                    current_edges.add((labels[target], labels[i]))

        while (right != len(times) -1) and (times[right + 1] - times[target] <= window):
            right += 1
        if right == len(labels) - 1:
            addAllEndFlag = True
            break
        if right != target:
            counts.update(labels[target + 1: right + 1])
            for i in range(target + 1, right + 1):
                if labels[target] == labels[i]:
                    pass
                else:
                    current_links.add((labels[target], labels[i]))

        for edge in current_edges:
            target, other = edge
            if counter[target] <= counter[other]:
                sorted_edge = tuple(sorted(edge))
                all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + 1


        target += 1

    if addAllEndFlag:
        #add all links from target to end
        for i in range(left, len(labels) -1):
            current_edges = set()
            label_reps = 1
            counts = Counter(labels[i:])
            for j in range(i + 1, len(labels)):
                if labels[i] == labels[j]:
                    label_reps += 1
                else:
                    current_edges.add((labels[i], labels[j]))
            for edge in current_edges:
                target, other = edge
                if counter[target] <= counter[other]:
                    sorted_edge = sorted(tuple(edge))
                    all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + 1

    return all_edges


def get_pairs(times, labels, tweet_ids, texts, window, weighted=False):
    assert(len(times) == len(labels))
    if len(labels) <= 2:
        return {}
    assert(window > datetime.timedelta(seconds=0))
    left = 0
    target = 0
    right = 0
    all_edges = {}
    tweet_id_tuple_list = []
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
        tweet_ids_counts = tweet_ids[left: right + 1]
        times_counts = times[left: right + 1]
        text_counts = texts[left: right + 1]

        if weighted:
            edge_weight = 1 / len(counts)
        else:
            edge_weight = 1
        target_node = labels[target]
        target_count = counts[target_node]
        
        i = 0
        for source_node, source_count in counts.items():
            if source_node != target_node:
                edge_dict = {}
                edge_dict['weight'] = edge_weight
                edge_dict['source'] = source_node
                edge_dict['target'] = target_node

                edge_dict['tweet_ids'] = []
                target_tweetid = tweet_ids[target]
                source_tweetid = tweet_ids_counts[i]

                source_text = text_counts[i]
                try:
                    target_text = texts[target]
                except:
                    target_text = texts[-1]

                source_time = times_counts[i]
                source_time = source_time.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
                try:
                    target_time = times_counts[target]
                except:
                    target_time = times_counts[-1]

                target_time = target_time.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')

                coordinated_tweet_info = {'source_tweet_id': source_tweetid, 'source_times': source_time, 'source_text': source_text,
                                        'target_tweet_id': target_tweetid, 'target_times': target_time, 'target_text': target_text
                                        }
                is_in_tuple_list = False

                if target_count <= source_count:
                    tweet_id_tuple = tuple[source_tweetid, target_tweetid]
                    tweet_id_tuple_inverse = tuple[target_tweetid, source_tweetid]
                    if tweet_id_tuple not in tweet_id_tuple_list and tweet_id_tuple_inverse not in tweet_id_tuple_list:
                        is_in_tuple_list = True
                    else:
                        is_in_tuple_list = False

                    #print(target_tweetid)
                    #print(source_tweetid)
                    #sorted_edge = tuple(sorted([source_node, target_node]))
                    #all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + edge_weight

                if (source_node, target_node) in all_edges:
                    if is_in_tuple_list:
                        all_edges[(source_node, target_node)]['weight'] += 1
                    else:
                        all_edges[(source_node, target_node)]['tweet_ids'].append(coordinated_tweet_info)
                elif (target_node, source_node) in all_edges:
                    if is_in_tuple_list:
                        all_edges[(target_node, source_node)]['weight'] += 1
                    else:
                        all_edges[(target_node, source_node)]['tweet_ids'].append(coordinated_tweet_info)
                else:
                    edge_dict['tweet_ids'].append(coordinated_tweet_info)
                    all_edges[(source_node, target_node)] = edge_dict

                #all_edges.append(edge_dict)
            i += 1

        target += 1


    if addAllEndFlag:
        #add all links from target to end
        for i in range(left, len(labels) -1):
            counts = Counter(labels[i:])
            tweet_ids_counts = tweet_ids[left: right + 1]
            times_counts = times[left: right + 1]
            text_counts = texts[left: right + 1]

            if weighted:
                edge_weight = 1 / len(counts)
            else:
                edge_weight = 1
            target_node = labels[target]
            target_count = counts[target_node]
            
            i = 0
            for source_node, source_count in counts.items():
                if source_node != target_node:
                    edge_dict = {}
                    edge_dict['weight'] = edge_weight
                    edge_dict['source'] = source_node
                    edge_dict['target'] = target_node

                    edge_dict['tweet_ids'] = []
                    target_tweetid = tweet_ids[target]
                    source_tweetid = tweet_ids_counts[i]

                    source_text = text_counts[i]
                    try:
                        target_text = texts[target]
                    except:
                        target_text = texts[-1]

                    source_time = times_counts[i]
                    source_time = source_time.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        target_time = times_counts[target]
                    except:
                        target_time = times_counts[-1]

                    target_time = target_time.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')

                    coordinated_tweet_info = {'source_tweet_id': source_tweetid, 'source_times': source_time, 'source_text': source_text,
                                            'target_tweet_id': target_tweetid, 'target_times': target_time, 'target_text': target_text
                                            }
                    is_in_tuple_list = False

                    if target_count <= source_count:
                        tweet_id_tuple = tuple[source_tweetid, target_tweetid]
                        tweet_id_tuple_inverse = tuple[target_tweetid, source_tweetid]
                        if tweet_id_tuple not in tweet_id_tuple_list and tweet_id_tuple_inverse not in tweet_id_tuple_list:
                            is_in_tuple_list = True
                        else:
                            is_in_tuple_list = False

                        #print(target_tweetid)
                        #print(source_tweetid)
                        #sorted_edge = tuple(sorted([source_node, target_node]))
                        #all_edges[sorted_edge] = all_edges.get(sorted_edge, 0) + edge_weight

                    if (source_node, target_node) in all_edges:
                        if is_in_tuple_list:
                            all_edges[(source_node, target_node)]['weight'] += 1
                        else:
                            all_edges[(source_node, target_node)]['tweet_ids'].append(coordinated_tweet_info)
                    elif (target_node, source_node) in all_edges:
                        if is_in_tuple_list:
                            all_edges[(target_node, source_node)]['weight'] += 1
                        else:
                            all_edges[(target_node, source_node)]['tweet_ids'].append(coordinated_tweet_info)
                    else:
                        edge_dict['tweet_ids'].append(coordinated_tweet_info)
                        all_edges[(source_node, target_node)] = edge_dict

                i += 1

    return all_edges