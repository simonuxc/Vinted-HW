import os
import pandas as pd
import threading

"""Custom thread class that runs a given function
This is needed as we need to save the ouput of that function"""
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
    
"""Returns mapped values
When users are mapped, array contain only their combined indexes.
When clicks are mapped, nested arrays contain <key, val> pairs, where key=user_id and val=(date&click_target)"""
def map(input_dir, mappers_count, key_col, filter_col=None, filter=None):
    mapped_values = [] # stores arrays of mapped users IDs from each file
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
        thread_handle = start_thread(map_values, (input_dir, input_files_set, key_col, filter_col, filter))
        threads.append(thread_handle)
        start_id = end_id

    # Account for the last mapper that might have less files to map
    input_files_set = input_files[start_id:]
    thread_handle = start_thread(map_values, (input_dir, input_files[start_id:], key_col, filter_col, filter))
    threads.append(thread_handle)

    for thread in threads:
        thread.join()
        mapped_values.extend(thread.value)

    return mapped_values

"""Maps values of the given input_files_sets"""
def map_values(input_dir, input_files_set, key_col, filter_col=None, filter=None):
    mapped_values = []
    for input_file in input_files_set:
        df = pd.read_csv(input_dir + "/" + input_file)
        # If users are being mapped, then filtering is necessary.
        # Also, only their IDs are saved, as no other value is relevant
        if filter != None:
            df = df.query(filter_col + "=='" + filter + "'")[key_col].to_list()
        # If clicks are being mapped, then we construct pairs of <key, val>, where key is user_id
        # and val is date and click_target
        else:
            df = [df[[key_col, 'date', 'click_target']].to_numpy().tolist()]
        mapped_values.extend(df)
    return mapped_values

"""Reduces the mapped_clicks based on mapped_users IDs
and returns a DataFrame containing <key, val> pairs, where key is user_id and val is data and click_target"""
def reduce(mapped_users, mapped_clicks, reducers_count):
    threads = []
    # An array with arrays of combined <key, value> pairs for each map
    reduced_clicks = []

    reducers_count = min(reducers_count, len(mapped_clicks)) # check if the number of reducers is logical
    mapped_clicks_count_per_reducer = len(mapped_clicks) // reducers_count
    start_id = 0
    # Distribute mapped_clicks sets over reducers
    for i in range(1, reducers_count):
        end_id = i * mapped_clicks_count_per_reducer
        mapped_clicks_sets = mapped_clicks[start_id:end_id]
        # Start reducing function on a thread
        thread_handle = start_thread(_target=reduce_clicks, _args=(mapped_users, mapped_clicks_sets))
        threads.append(thread_handle)
        start_id = end_id

    # Account for the last reducer that might have less files to map
    mapped_clicks_sets = mapped_clicks[start_id:]
    thread_handle = start_thread(_target=reduce_clicks, _args=(mapped_users, mapped_clicks_sets))
    threads.append(thread_handle)

    for x in threads:
        x.join()
        reduced_clicks.extend(x.value)

    reduced_clicks = pd.DataFrame(reduced_clicks)
    reduced_clicks.columns = ["id", "date", "click_target"]
    return reduced_clicks

"""Returns a list of combined <key, value> pairs from mapped_clicks_sets"""
def reduce_clicks(mapped_users, mapped_clicks_sets):
    # Basically skip clicks that do not have a matching ID in mapped_users list
    reduced_clicks = []
    for mapped_clicks_set in mapped_clicks_sets:
        for mapped_click in mapped_clicks_set:
            click_id = mapped_click[0]
            if click_id in mapped_users:
                reduced_clicks.append(mapped_click)

    return reduced_clicks

def output_results(output_dir, mapped_clicks):
    mapped_clicks.to_csv(output_dir, index=False)

mappers_count = 2
reducers_count = 2
key_col = "date"
val_col = "count"

key_col, filter_col, filter = "id", "country", "LT"
mapped_users = map("./data/users", mappers_count, key_col, filter_col, filter)

key_col = "user_id"
mapped_clicks = map("./data/clicks", mappers_count, key_col)

# return a dataframe containing all clicks that were made by LT users
mapped_clicks = reduce(mapped_users, mapped_clicks, reducers_count)
output_results("data/filtered_clicks.csv", mapped_clicks)