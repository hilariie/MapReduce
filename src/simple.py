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

    with open(config_path, 'r') as file:
        yml = yaml.safe_load(file)
    path = os.path.join(parent_directory, '..', yml['path'])
    df = pd.read_csv(path, header=None)
    col1 = df.columns[0]
    map_in = df[col1].tolist()
    # map_out = []


    for i in range(2):
        if i == 0:
            USE_MULTIPROCESSING = True  # Set to True to use multiprocessing, False to use threading
        else:
            USE_MULTIPROCESSING = False  # Set to True to use multiprocessing, False to use threading

        t1 = time.time()
        if USE_MULTIPROCESSING:
            pretty_print('multiprocessing')
            threading.Thread(target=pret_print, daemon=True, args=('multiprocessing...', )).start()
            with concurrent.futures.ProcessPoolExecutor() as executor:
                map_out = list(executor.map(map_func, map_in))
        else:
            pretty_print('threading')
            threading.Thread(target=pret_print, daemon=True, args=('threading...', )).start()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # executor.map(pret_print, [1, 2])
                map_out = list(executor.map(map_func, map_in))
        t2 = time.time()
        print("Done\nElapsed time: {:.3} seconds\n".format(t2 - t1))
    reduce_in = shuffle(map_out)
    reduce_out = {}

    for key, values in reduce_in.items():
        reduce_out[key] = reduce(reduce_func, values)


