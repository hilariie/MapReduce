"""This script implements a simplified map reduce solution for the purpose of comparing performance between
using threads and multiprocessing"""


def map_func(x):
    """ Simple occurrence mapper """
    return x, 1


def shuffle(mapper_out):
    """ Organise the mapped values by key """
    data = {}
    for k, v in mapper_out:
        if k not in data:
            data[k] = [v]
        else:
            data[k].append(v)
    return data


def reduce_func(x, y):
    """ Simple sum reducer """
    return x + y


def pret_print(str_):
    """display string while processing"""
    while True:
        print(str_)
        time.sleep(0.4)


if __name__ == '__main__':
    import time
    from functools import reduce
    import yaml
    import os
    import pandas as pd
    import concurrent.futures
    import threading
    from MapReduce import pretty_print

    # Get the parent directory of main.py
    parent_directory = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to config.yaml
    config_path = os.path.join(parent_directory, '..', 'config.yaml')
    # get path to data from config file and read data
    with open(config_path, 'r') as file:
        yml = yaml.safe_load(file)
    path = os.path.join(parent_directory, '..', yml['path'])
    df = pd.read_csv(path, header=None)
    col1 = df.columns[0]
    map_in = df[col1].tolist()

    # Map Reduce using Threads and Multiprocessing
    for i in range(2):
        t1 = time.time()
        if i == 0:
            pretty_print('multiprocessing')
            # call function to continually print in background until Map reduce process is done
            threading.Thread(target=pret_print, daemon=True, args=('multiprocessing...', )).start()
            # Map Reduce using mutliprocessing
            with concurrent.futures.ProcessPoolExecutor() as executor:
                map_out = list(executor.map(map_func, map_in))
                reduce_in = executor.submit(shuffle, map_out)
                reduce_in = reduce_in.result()
                for key, values in reduce_in.items():
                    _ = executor.submit(reduce, reduce_func, values)
        else:
            pretty_print('threading')
            # call function to continually print in background until Map reduce process is done
            threading.Thread(target=pret_print, daemon=True, args=('threading...', )).start()
            # Map Reduce using Threads
            with concurrent.futures.ThreadPoolExecutor() as executor:
                map_out = list(executor.map(map_func, map_in))
                reduce_in = executor.submit(shuffle, map_out)
                reduce_in = reduce_in.result()
                for key, values in reduce_in.items():
                    _ = executor.submit(reduce, reduce_func, values)
        t2 = time.time()
        print("Done\nElapsed time: {:.3} seconds\n".format(t2 - t1))






