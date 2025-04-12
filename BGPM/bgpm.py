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
        for record in stream:
            for entry in record:
                prefix = entry.fields['prefix']
                unique_prefixes.add(prefix)
        
        unique_ases_by_snapshot.append(len(unique_prefixes))
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
        for record in stream:
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
    mp_snapshot = {} 

    for ndx, fpath in enumerate(cache_files):
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "rib-file", fpath)

        # implement your solution here
        mp = defaultdict(set) # map the origin AS to the prefix
        # process each record
        for record in stream:
            for entry in record:   
                if 'as-path' in entry.fields and entry.fields['as-path'] != "":
                    prefix = entry.fields["prefix"]
                    ass_path = entry.fields['as-path'].split()

                    origin = ass_path[-1]

                    mp[origin].add(prefix)
        
        # fix global tracking
        for k,v in mp.items():
            if k not in mp_snapshot:
                mp_snapshot[k] = [0] * len(cache_files)
            mp_snapshot[k][ndx] = len(v)

    # calculate growth rates
    growth = {}
    for origin, cnts in mp_snapshot.items():
        # find non 0 indices
        idxs = [i for i, cnt in enumerate(cnts) if cnt > 0]
        if len(idxs) > 0:
            idx_0 = min(idxs)
            idx_n = max(idxs)

            if idx_0 != idx_n:
                cnt_1 = cnts[idx_0]
                cnt_2 = cnts[idx_n]

                if cnt_1 > 0: # avoid division 
                    percentage = (cnt_2 - cnt_1) / cnt_1
                    growth[origin] = percentage

    # sort top 10
    top = sorted(mp_snapshot.items(), key=lambda x:x[1])[:10]
    top_10_ases_by_prefix_growth = [n for n,_ in top]
    return top_10_ases_by_prefix_growth


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
        for record in stream:
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
                        if a not in unique_ass:
                            unique_ass.add(a)
                    
                    # update path length
                    if len(unique_ass) <= 1:
                        continue
                    if (origin not in min_paths or len(unique_ass) < min_paths[origin]):
                        min_paths[origin] = len(unique_ass)
        # update the snapshot
        for origin, length in min_paths.items():
            if origin not in shortest_path_by_origin_by_snapshot:
                shortest_path_by_origin_by_snapshot[origin] = [0] * length
            shortest_path_by_origin_by_snapshot[origin][ndx] = length
    
    origin_set = set(shortest_path_by_origin_by_snapshot.keys())
    # final update
    for o in origin_set:
        if len(shortest_path_by_origin_by_snapshot[o]) < len(cache_files):
            shortest_path_by_origin_by_snapshot[o].extend([0]* len(cache_files) - len(shortest_path_by_origin_by_snapshot[o]))
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
        for record in stream:
            timestamp = record.time
            for entry in record:

                # get metadata like peer ip and prefix
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
    rtbh_event_durations = {} # key: peer IP, value: list of RTBH event duration
    last_A = {} # keep track of the most recent annoucement for each pair
    for fpath in cache_files:
        stream = pybgpstream.BGPStream(data_interface="singlefile")
        stream.set_data_interface_option("singlefile", "upd-file", fpath)

        # implement your solution here 
        # process each record
        for record in stream:
            timestamp = record.time
            for entry in record:

                # get metadata like peer ip and prefix
                prefix = entry.fields['prefix']
                peer_ip = entry.peer_address

                if peer_ip not in last_A:
                    last_A[peer_ip] = {}
                if peer_ip not in rtbh_event_durations:
                    rtbh_event_durations[peer_ip] = {}
                if prefix not in rtbh_event_durations[peer_ip]:
                    rtbh_event_durations[peer_ip][prefix] = []


                # map the announcement and withdrawl times
                if entry.type == 'A':
                    has_rtbh = False

                    if 'communities' in entry.fields and entry.fields['communitites']:
                        communities = entry.fields['communities'].split()
                        has_rtbh = any('666' in c for c in communities) # looks like :666 suffix for rtbh?

                        if has_rtbh:
                            last_A[peer_ip][prefix] = timestamp
                        else:
                            if prefix in last_A[peer_ip]:
                                del last_A[peer_ip][prefix]

                elif entry.type == 'W':

                    if prefix in last_A[peer_ip]:

                        event_duration = timestamp - last_A[peer_ip][prefix]

                        if event_duration > 0:
                            rtbh_event_durations[peer_ip][prefix].append(event_duration)

                        # withdraw last announcement
                        del last_A[peer_ip][prefix]
    
    # filter out the empty entries
    for p_ip in list(rtbh_event_durations.keys()):
        rtbh_event_durations[p_ip] = {
            prefix:event_durations for prefix, event_durations in rtbh_event_durations[p_ip].items() if event_durations
        }

        if not rtbh_event_durations[p_ip]:
            del rtbh_event_durations[p_ip]

    return rtbh_event_durations
