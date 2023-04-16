#!/usr/bin/env python3
"""Performs MapReduce on CSV dataset to identify word(s) with the highest count or occurrence"""

if __name__ == '__main__':
    from MapReduce import *
    import yaml
    import time
    from functools import reduce
    from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

    # Get the parent directory of main.py
    parent_directory = os.path.dirname(os.path.abspath(__file__))

    # Construct the path to config.yaml
    config_path = os.path.join(parent_directory, '..', 'config.yaml')

    with open(config_path, 'r') as file:
        yml = yaml.safe_load(file)
    # Read parameters from yml file
    pattern = yml['pattern']
    column_index = yml['column_index']
    header = yml['header']
    sum_col = yml['sum_column']

    pretty_print('Data pre-processing', count_=5)
    # Read data from CSV file
    # Construct the path to dataset path in config.yaml
    path = os.path.join(parent_directory, '..', yml['path'])
    data = list(read_data(path, column_index, sum_col, header=header, duplicate=yml['duplicates']))

    pretty_print('Map Reduce')
    map_reduce = MapReduce(pattern)

    threading = yml['threading']
    # Get start time
    t1 = time.time()
    if threading:
        print('Threading in use')
        max_count, word = map_reduce.map_reduce_parallel(ThreadPoolExecutor(), data)
    else:
        print('Multiprocessing in use')
        max_count, word = map_reduce.map_reduce_parallel(ProcessPoolExecutor(), data)

    print('Final shuffle succesful')
    # Get end time
    t2 = time.time()

    # Display output in user-friendly manner
    pretty_print('Results', count_=8)
    print(f"{yml['column1_search']} with the highest {yml['column2_search']}")
    pretty_print("ID(s)", count_=9)
    print("\t", word)
    print("-" * 27)
    print(f"Sum of {yml['column2_search']} per {yml['column1_search']}: {max_count}")
    print("Elapsed time: {:.3} seconds".format(t2 - t1))
