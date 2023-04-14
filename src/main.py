#!/usr/bin/env python3
"""Performs MapReduce on CSV dataset to identify passenger(s) with the highest number of flights"""

from MapReduce import *
import yaml
import time
from functools import reduce

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
column_values, column_val2, col2 = read_data(path, column_index, sum_col, header=header)

pretty_print('Map Reduce')
map_reduce = MapReduce(pattern)
# Get start time
t1 = time.time()
with ThreadPoolExecutor() as executor:
    # perform first mapping operation
    mapper1_output = executor.map(map_reduce.mapper1, column_values, column_val2)
    # perform second mapping operation
    mapper_output = executor.map(map_reduce.mapper2, column_values, col2)
    print('Mapping successful')
    # perform shuffling operation
    reduce_input = map_reduce.shuffle(mapper_output)
    print('Shuffle succcessful')

    # Perform reducing operation
    reduce_out = {}
    for key, values in reduce_input.items():
        future = executor.submit(reduce, map_reduce.reduce_func, values)
        reduce_out[key] = future

if reduce_out:
    print('Reducing Successful')
else:
    print('Reducing Failed')
    raise ValueError("output of mapreduce is empty")

# Perform final output operation
first_key_, first_val = map_reduce.final_output(reduce_out)
print('Final shuffle succesful')
# Get end time
t2 = time.time()

# Display output in user-friendly manner
pretty_print('Results', count_=8)
print(f"{yml['column1_search']} with the highest {yml['column2_search']}")
pretty_print("ID(s)", count_=9)
print("\t", first_val)
print("-" * 27)
print(f"Sum of {yml['column2_search']} per {yml['column1_search']}: {first_key_}")
print("Elapsed time: {:.3} seconds".format(t2 - t1))
