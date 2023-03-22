import os
import pandas as pd
import numpy as np
import threading

"""Custom thread class that runs a given function. The output of that function is the self.value"""
class CustomThread(threading.Thread):
    def __init__(self, target, args=()):
        threading.Thread.__init__(self)
        self.value = None
        self.target = target
        self.args = args
 
    def run(self):
        self.value = (self.target(*self.args))

"""Helper function that starts a thread and returns its handle.
Takes in a function and a list of arguments"""
def start_thread(_target=None, _args=()):
    if _target == None:
        raise Exception("No function passed to a thread")
    thread = CustomThread(target=_target, args=_args)
    thread.start()
    return thread

"""The main function for MapReduce"""
def map_reduce(input_dir, output_dir, mappers_count, reducers_count, key_col, val_col):
    # In this case there is no need for <key, value> pairs as there are no values
    mapped_clicks = map(input_dir, mappers_count, key_col)
    reduced_clicks = reduce(mapped_clicks, reducers_count, key_col, val_col)
    reduced_clicks.to_csv(output_dir, index=False)
    
"""Returns an array of arrays with dates from each file"""
def map(input_dir, mappers_count, key_col):
    mapped_clicks = [] # stores arrays of mapped dates from each file [[date1, date2..], [date1, date2], ..]
    threads = []

    input_files = os.listdir(input_dir) # An array of all file names in the directory

    mappers_count = min(mappers_count, len(input_files)) # Check if the number of mappers is logical
    files_count_per_mapper = len(input_files) // mappers_count
    start_id = 0
    # Distributes files over mappers
    for i in range(1, mappers_count):
        end_id = i * files_count_per_mapper
        input_files_set = input_files[start_id:end_id]
        # start the mapping function on a thread
        thread_handle = start_thread(map_clicks, (input_dir, input_files_set, key_col))
        threads.append(thread_handle)
        start_id = end_id

    # Account for the last mapper that might have less files to map
    input_files_set = input_files[start_id:]
    thread_handle = start_thread(map_clicks, (input_dir, input_files_set, key_col))
    threads.append(thread_handle)

    for x in threads:
        x.join()
        mapped_clicks.extend(x.value)

    return mapped_clicks

"""Returns an array of all dates in particular files"""
def map_clicks(input_dir, input_files_set, key_col):
    mapped_clicks = []
    for input_file in input_files_set:
        df = pd.read_csv(input_dir + "/" + input_file)
        # No need to track values as there are none, so save "date" column only
        mapped_clicks.extend([df[key_col].to_numpy().tolist()])
    return mapped_clicks

"""Reduces the provided mapped_clicks and returns a DataFrame containing <key, val> (<date, count>) pairs"""
def reduce(mapped_clicks, reducers_count, key_col, val_col):
    threads = []
    # Combined <key, value> pairs from each map
    reduced_clicks = []

    reducers_count = min(reducers_count, len(mapped_clicks)) # check if the number of reducers is logical
    mapped_clicks_count_per_reducer = len(mapped_clicks) // reducers_count
    start_id = 0
    # Distribute mapped_clicks sets over reducers
    for i in range(1, reducers_count):
        end_id = i * mapped_clicks_count_per_reducer
        mapped_clicks_sets = mapped_clicks[start_id:end_id]
        # Start reducing function on a thread
        thread_handle = start_thread(_target=reduce_clicks, _args=[mapped_clicks_sets])
        threads.append(thread_handle)
        start_id = end_id

    # Account for the last reducer that might have less files to map
    mapped_clicks_sets = mapped_clicks[start_id:]
    thread_handle = start_thread(_target=reduce_clicks, _args=[mapped_clicks_sets])
    threads.append(thread_handle)

    for x in threads:
        x.join()
        # Combine results of each reducer
        reduced_clicks.extend(x.value)

    # Linearly combine results of each reducer.
    # Not sure if it is possible to execute this part using threads in paraller?
    combined_clicks = {}
    for pair in reduced_clicks:
        key = pair[0]
        if key not in combined_clicks:
            combined_clicks[key] = pair[1]
        else:
            combined_clicks[key] += pair[1]
    
    # Format data
    df = pd.DataFrame.from_dict(combined_clicks, orient='index').reset_index()
    df.columns = [key_col, val_col]
    return df

"""Returns a list of combined <key, value> pairs from mapped_clicks_sets"""
def reduce_clicks(mapped_clicks_sets):
    reduced_clicks = {}
    for mapped_click_set in mapped_clicks_sets:
        for mapped_click in mapped_click_set:
            key = mapped_click
            if key not in reduced_clicks:
                reduced_clicks[key] = 1
            else:
                reduced_clicks[key] += 1
    return list(reduced_clicks.items())

mappers_count = 2
reducers_count = 2
key_col = "date"
val_col = "count"

map_reduce("./data/clicks", "./data/total_clicks.csv", mappers_count, reducers_count, key_col, val_col)