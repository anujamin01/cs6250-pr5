#!/usr/bin/env python3

import pybgpstream
from collections import defaultdict
"""
CS 6250 BGP Measurements Project

Notes:
- Edit this file according to the project description and the docstrings provided for each function
- Do not change the existing function names or arguments
- You may add additional functions but they must be contained entirely in this file
"""


# Task 1A: Unique Advertised Prefixes Over Time
def unique_prefixes_by_snapshot(cache_files):
    """
    Retrieve the number of unique IP prefixes from each of the input BGP data files.

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A list containing the number of unique IP prefixes for each input file.
        For example: [2, 5]
    """
    # the required return type is 'list' - you are welcome to define additional data structures, if needed
    unique_prefixes_by_snapshot = []

    for fpath in cache_files:
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", fpath)

        # implement your solution here
        unique_prefixes = set()

        # parse the records in the stream
        for record in stream.records():
            
            
            while True:
                entry = record.get_next_elem()
                if entry is None: 
                    break
  
                if 'prefix' in entry.fields:
                    prefix = entry.fields['prefix']
                    unique_prefixes.add(prefix)
                      
        unique_prefixes_by_snapshot.append(len(unique_prefixes))
    return unique_prefixes_by_snapshot


# Task 1B: Unique Autonomous Systems Over Time
def unique_ases_by_snapshot(cache_files):
    """
    Retrieve the number of unique ASes from each of the input BGP data files.

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A list containing the number of unique ASes for each input file.
        For example: [2, 5]
    """
    # the required return type is 'list' - you are welcome to define additional data structures, if needed
    unique_ases_by_snapshot = []

    for fpath in cache_files:
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", fpath)

        # implement your solution here
        unique_asses = set()

        # process each record
        for record in stream.records():
            for entry in record: 
                if 'as-path' in entry.fields:
                    as_path = entry.fields['as-path']
                    # parse the AS path
                    for part in as_path.split():
                        unique_asses.add(part)
        unique_ases_by_snapshot.append(len(unique_asses))
    return unique_ases_by_snapshot


# Task 1C: Top-10 Origin AS by Prefix Growth
def top_10_ases_by_prefix_growth(cache_files):
    """
    Compute the top 10 origin ASes ordered by percentage increase of advertised prefixes (smallest to largest)

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A list of the top 10 origin ASes ordered by percentage increase of advertised prefixes (smallest to largest)
        AS numbers are represented as strings.

        For example: ["777", "1", "6"]
          corresponds to AS "777" as having the smallest percentage increase (of the top ten) and AS "6" having the
          highest percentage increase (of the top ten).
    """
    # the required return type is 'list' - you are welcome to define additional data structures, if needed
    top_10_ases_by_prefix_growth = []
    
    # track prefix/origin globally
    #mp_snapshot = defaultdict(lambda: defaultdict(set))

    for ndx, fpath in enumerate(cache_files):
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", fpath)
        prefixes = {}
        # implement your solution here
        # mp = defaultdict(set) # map the origin AS to the prefix
        # process each record
        for record in stream.records():
            for entry in record:   
                if 'as-path' in entry.fields and entry.fields['as-path'] and 'prefix' in entry.fields:
                    ass_path = entry.fields['as-path'].split()
                    if not ass_path:
                        continue

                    prefix = entry.fields["prefix"]
                    origin = ass_path[-1]

                    if origin not in prefixes:
                        prefixes[origin] = set()
                    prefixes[origin].add(prefix)
        top_10_ases_by_prefix_growth.append(prefixes)

    # calculate growth rates
    growth = {}

    unique_ass = set()

    for elt in top_10_ases_by_prefix_growth:
        unique_ass.update(elt.keys())
    
    # find first and last appearance and calculate growth
    for origin_as in unique_ass:
        # find 1st appearance
        idx_1 = None
        for i, a in enumerate(top_10_ases_by_prefix_growth):
            if origin_as in a:
                idx_1 = i
                break
        
        if idx_1 is None:
            continue

        # find last
        idx_2 = None
        for i in range(len(top_10_ases_by_prefix_growth)-1,-1,-1):
            if origin_as in top_10_ases_by_prefix_growth[i]:
                idx_2 = i
                break

        if idx_2 is None or idx_1 == idx_2:
            continue

        c_1 = len(top_10_ases_by_prefix_growth[idx_1][origin_as])
        c_2 = len(top_10_ases_by_prefix_growth[idx_2][origin_as])

        if c_1 > 0:
            percentage = (c_2 - c_1) / c_1
            growth[origin_as] = percentage


    # sort top 10
    top = sorted(growth.items(), key=lambda x:-x[1])[:10]
    top.reverse()
    return [n for n,_ in top]


# Task 2: Routing Table Growth: AS-Path Length Evolution Over Time
def shortest_path_by_origin_by_snapshot(cache_files):
    """
    Compute the shortest AS path length for every origin AS from input BGP data files.

    Retrieves the shortest AS path length for every origin AS for every input file.

    Your code should return a dictionary where every key is a string representing an AS name and every value is a list
    of the shortest path lengths for that AS.

    Note: If a given AS is not present in an input file, the corresponding entry for that AS and file should be zero (0)
    Every list value in the dictionary should have the same length.

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A dictionary where every key is a string representing an AS name and every value is a list, containing one entry
        per file, of the shortest path lengths for that AS
        AS numbers are represented as strings.

        Example:
        Given three cache files (also called "snapshots"), the results {"455": [4, 2, 3], "533": [4, 10, 2]}
        mean that AS 455 has a shortest path length of 4 in the first cache file, a shortest path length of 2 in the second
        cache file, and a shortest path of 3 in the third cache file. Similarly, AS 533 has shortest path lengths of 4, 10, and 2.

        TODO: 
        1. identify all origin ASses
        2. find all paths where an origin AS is the origin
        3. calculate path length
        4. find shortest path length
        5. filter out paths of length 1
    """
    # the required return type is 'dict' - you are welcome to define additional data structures, if needed
    shortest_path_by_origin_by_snapshot = {}
    # origin_set = set()
    for ndx, fpath in enumerate(cache_files):
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", fpath)

        # keep track of min paths key: AS, value: min path len
        min_paths = defaultdict(int)
        # parse records
        for record in stream.records():
            for entry in record:
                if 'as-path' in entry.fields and entry.fields['as-path']:
                    ass_path = entry.fields['as-path']
                    ass_path_arr = ass_path.split()

                    # get origin 
                    origin = ass_path_arr[-1]

                    # origin_set.add(origin)
                    # count unique AS in path
                    unique_ass = set()
                    for a in ass_path_arr:
                        unique_ass.add(a)
                    
                    # update path length
                    if len(unique_ass) <= 1:
                        continue
                    if (origin not in min_paths or len(unique_ass) < min_paths[origin]):
                        min_paths[origin] = len(unique_ass)
        # update the snapshot
        for origin, length in min_paths.items():
            if origin not in shortest_path_by_origin_by_snapshot:
                shortest_path_by_origin_by_snapshot[origin] = [0] * len(cache_files)
            
            if len(shortest_path_by_origin_by_snapshot[origin]) <= ndx:
                shortest_path_by_origin_by_snapshot[origin].extend([0] * (ndx+1-len(shortest_path_by_origin_by_snapshot[origin])))
            shortest_path_by_origin_by_snapshot[origin][ndx] = length


    # final update
    for o in shortest_path_by_origin_by_snapshot:
        if len(shortest_path_by_origin_by_snapshot[o]) < len(cache_files):
            shortest_path_by_origin_by_snapshot[o].extend([0]* (len(cache_files) - len(shortest_path_by_origin_by_snapshot[o])))
    return shortest_path_by_origin_by_snapshot


# Task 3: Announcement-Withdrawal Event Durations
def aw_event_durations(cache_files):
    """
    Identify Announcement and Withdrawal events and compute the duration of all explicit AW events in the input BGP data

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A dictionary where each key is a string representing the address of a peer (peerIP) and each value is a
        dictionary with keys that are strings representing a prefix and values that are the list of explicit AW event
        durations (in seconds) for that peerIP and prefix pair.

        For example: {"127.0.0.1": {"12.13.14.0/24": [4.0, 1.0, 3.0]}}
        corresponds to the peerIP "127.0.0.1", the prefix "12.13.14.0/24" and event durations of 4.0, 1.0 and 3.0.
        1. look for pair of last A, first W 
    """
    # the required return type is 'dict' - you are welcome to define additional data structures, if needed
    aw_event_durations = {}

    last_A = defaultdict(dict) # key = Announcement, value = first withdrawl
    for ndx, fpath in enumerate(cache_files):
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "upd-file", fpath)

        # implement your solution here
        # process each record
        for record in stream.records():
            timestamp = record.time
            for entry in record:

                # get metadata like peer ip and prefix
                if 'prefix' in entry.fields and entry.fields['prefix']:
                    prefix = entry.fields['prefix']
                    peer_ip = entry.peer_address

                    if peer_ip not in last_A:
                        last_A[peer_ip] = {}
                    if peer_ip not in aw_event_durations:
                        aw_event_durations[peer_ip] = {}
                    if prefix not in aw_event_durations[peer_ip]:
                        aw_event_durations[peer_ip][prefix] = []


                    # map the announcement and withdrawl times
                    if entry.type == 'A':
                        last_A[peer_ip][prefix] = timestamp 
                    
                    elif entry.type == 'W':
                        if prefix in last_A[peer_ip]:
                            event_duration = timestamp - last_A[peer_ip][prefix]

                            if event_duration > 0:
                                aw_event_durations[peer_ip][prefix].append(event_duration)

                            # withdraw announcement
                            del last_A[peer_ip][prefix]
    
    # filter out the empty entries
    for p_ip in list(aw_event_durations.keys()):
        aw_event_durations[p_ip] = {
            prefix:event_duration for prefix, event_duration in aw_event_durations[p_ip].items() if event_duration
        }

        if not aw_event_durations[p_ip]:
            del aw_event_durations[p_ip]

    return aw_event_durations

# Task 4: RTBH Event Durations
def rtbh_event_durations(cache_files):
    """
    Identify blackholing events and compute the duration of all RTBH events from the input BGP data

    Identify events where the prefixes are tagged with at least one Remote Triggered Blackholing (RTBH) community.

    Args:
        cache_files: A chronologically sorted list of absolute (also called "fully qualified") path names

    Returns:
        A dictionary where each key is a string representing the address of a peer (peerIP) and each value is a
        dictionary with keys that are strings representing a prefix and values that are the list of explicit RTBH event
        durations (in seconds) for that peerIP and prefix pair.

        For example: {"127.0.0.1": {"12.13.14.0/24": [4.0, 1.0, 3.0]}}
        corresponds to the peerIP "127.0.0.1", the prefix "12.13.14.0/24" and event durations of 4.0, 1.0 and 3.0.
    """
    # the required return type is 'dict' - you are welcome to define additional data structures, if needed
    rtbh_event_durations = defaultdict(list) # key: peer IP, value: list of RTBH event duration
    last_A = {} # keep track of the most recent annoucement for each pair
    for fpath in cache_files:
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "upd-file", fpath)

        # implement your solution here 
        # process each record
        for record in stream.records():
            for entry in record:
                # get metadata like peer ip and prefix
                if 'prefix' in entry.fields and entry.fields['prefix']:
                    prefix = entry.fields['prefix']
                    peer_ip = entry.peer_address
                    timestamp = record.time
                    k = (peer_ip, prefix)

                    # map the announcement and withdrawl times
                    if entry.type == 'W':
                        if k in last_A:
                            start = last_A[k]
                            event_duration = timestamp - start

                            if event_duration > 0:
                                rtbh_event_durations[k].append(float(event_duration))
                            del last_A[k]

                    elif entry.type == 'A':

                        # check for rtbh community
                        communities = entry.fields.get("communities", [])
                        has_rtbh = any(c.endswith(':666') for c in communities)

                        # store most recent rtbh
                        if has_rtbh:
                            last_A[k]=timestamp
                        else:
                            # remove previous rtbh announcement
                            last_A.pop(k,None)
    # convert back to dict
    res = {}
    for (peer_ip, prefix), durations in rtbh_event_durations.items():
        if durations:
            if peer_ip not in res:
                res[peer_ip] = {}
            res[peer_ip][prefix] = durations

    return res
