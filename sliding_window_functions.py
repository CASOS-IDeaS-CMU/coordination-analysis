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