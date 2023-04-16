import unittest
import tempfile
import os
import csv
from concurrent.futures import ThreadPoolExecutor
from MapReduce import MapReduce, read_data, header_reader

# Get the parent directory of main.py
parent_directory = os.path.dirname(os.path.abspath(__file__))

# Construct the path to config.yaml
config_path = os.path.join(parent_directory, '..', 'config.yaml')


class MapReduceTests(unittest.TestCase):
    def setUp(self):
        pattern = ["^[A-Z]{3}\\d{4}[A-Z]{2}\\d{1}$", "^[A-Z]{3}\\d{4}[A-Z]{1}$"]
        self.map_reduce = MapReduce(pattern)
        self.def_col_ind = [0, 1]
        # Create temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        # Create temporary csv file in the temporary directory
        csv_path = os.path.join(self.temp_dir.name, 'test_file.csv')
        # Write data to CSV file
        with open(csv_path, 'w', newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(["AAA1234XY9", "XXX1234AB9", "NGA", 5])
            writer.writerow(["AAA1235XY9", "ZZZ1234XX9", "FRA", 6])
            writer.writerow(["AAA1236XY9", "XYZ1234YY9", "NGA", 10])
        self.csv_path = csv_path

    def tearDown(self):
        # Delete the temporary file and directory
        os.unlink(self.csv_path)
        self.temp_dir.cleanup()

    def test_mapper2(self):
        self.assertEqual(self.map_reduce.mapper2("ABC1234DE01", 1), ("ABC1234DE01", None))
        self.assertEqual(self.map_reduce.mapper2("ABC1234D*0", 1), ("ABC1234D*0", None))
        self.assertEqual(self.map_reduce.mapper2("aBC1234DBE0", 10), ("aBC1234DBE0", None))
        self.assertEqual(self.map_reduce.mapper2("ABC134DAE0"), ("ABC134DAE0", None))
        self.assertEqual(self.map_reduce.mapper2(""), ("", None))
        self.assertEqual(self.map_reduce.mapper2("XBC1234EF5", None), ("XBC1234EF5", None))
        self.assertEqual(self.map_reduce.mapper2("XBC1234EF5", 'str'), ("XBC1234EF5", None))
        self.assertEqual(self.map_reduce.mapper2("ABC0000DE0", 2), ('ABC0000DE0', 2))
        self.assertEqual(self.map_reduce.mapper2("XBC1234EF5"), ("XBC1234EF5", 1))

    def test_shuffle(self):
        input_data = [("id1", 1), ("id2", 1), ("id3", 1), ("id1", 1), ("id2", 1)]
        expected_output = {'id1': [1, 1], 'id2': [1, 1], 'id3': [1]}
        self.assertEqual(self.map_reduce.shuffle(input_data), expected_output)
        input_data = [("id1", 1), ("id2", None), ("id1", 1), ("id2", 1)]
        expected_output = {'id1': [1, 1], 'id2': [1]}
        self.assertEqual(self.map_reduce.shuffle(input_data), expected_output)

    def test_reduce_func(self):
        self.assertEqual(self.map_reduce.reduce_func(1, 2), 3)
        self.assertEqual(self.map_reduce.reduce_func(0, 0), 0)
        self.assertEqual(self.map_reduce.reduce_func(-1, 3), 2)

    def test_final_output(self):
        input_data = {'id1': 4, 'id2': 2, 'id3': 4, 'id5': 2}
        output_list = "id1\n\t id3"
        expected_output = (4, output_list)
        self.assertEqual(self.map_reduce.final_output(input_data), expected_output)

    def test_map_reduce_parallel_correct(self):
        input_data = [['ABC0000DE0', 'XBC1234EF5'], ["ABC1111A", "XYZ2222B"], [2, 5]]
        output = (5, 'XBC1234EF5')
        self.assertEqual(self.map_reduce.map_reduce_parallel(ThreadPoolExecutor(), input_data, test=True), output)

        input_data2 = [['ABC0000DE0', 'XBC1234EF5', 'ABC0000DE0'],
                      ["ABC1111A", "XYZ2222B", 'XYZ2222B'], [1, 1, 1]]
        output2 = (2, 'ABC0000DE0')
        self.assertEqual(self.map_reduce.map_reduce_parallel(ThreadPoolExecutor(), input_data2, test=True), output2)

        input_data3 = [['ABC0000DE0', 'XBC1234EF5', 'ABC0000DE0'],
                       ["ABC1111A", "XYZ2222B", 'XYZ2*22B'], [1, 1, 1]]
        output3 = (1, 'ABC0000DE0\n\t XBC1234EF5')
        self.assertEqual(self.map_reduce.map_reduce_parallel(ThreadPoolExecutor(), input_data3, test=True), output3)

    def test_map_reduce_parallel_incorrect(self):
        input_data = [['ABC00*0DE0', 'XBC1234EF5'], ["ABC1111A", "XYZ2!22B"], [2, 5]]
        with self.assertRaises(ValueError):
            self.map_reduce.map_reduce_parallel(ThreadPoolExecutor(), input_data, test=True)

    def test_header_reader(self):
        self.assertEqual(header_reader(True), 0)
        self.assertEqual(header_reader(False), None)

    def test_header_reader_wrong(self):
        with self.assertRaises(ValueError):
            header_reader(0)
        with self.assertRaises(ValueError):
            header_reader('None')
        with self.assertRaises(ValueError):
            header_reader('')
        with self.assertRaises(ValueError):
            header_reader([])
        with self.assertRaises(ValueError):
            header_reader('False')

    def test_read_data_correct(self):
        exp_output1 = ['AAA1234XY9', 'AAA1235XY9', 'AAA1236XY9'], ["XXX1234AB9", "ZZZ1234XX9", "XYZ1234YY9"], [1, 1, 1]
        exp_output2 = ['NGA', 'FRA', 'NGA'], ["AAA1234XY9", "AAA1235XY9", "AAA1236XY9"], [1, 1, 1]
        exp_output3 = ['NGA', 'FRA', 'NGA'], ["AAA1234XY9", "AAA1235XY9", "AAA1236XY9"], [5, 6, 10]
        self.assertEqual(read_data(self.csv_path, column_ind=self.def_col_ind), exp_output1)
        self.assertEqual(read_data(self.csv_path, column_ind=[2, 0]), exp_output2)
        self.assertEqual(read_data(self.csv_path, sum_col=3, column_ind=[2, 0]), exp_output3)

    def test_read_data_incorrect(self):
        false_input1 = [1, 2, 3, 4]
        false_input2 = []
        false_input3 = [4]
        false_input4 = 4
        with self.assertRaises(ValueError):
            read_data(self.csv_path, column_ind=false_input1)
        with self.assertRaises(ValueError):
            read_data(self.csv_path, column_ind=false_input2)
        with self.assertRaises(KeyError):
            read_data(self.csv_path, column_ind=false_input3)
        with self.assertRaises(KeyError):
            read_data(self.csv_path, self.def_col_ind, sum_col=false_input4)

    def test_read_data_extension(self):
        input_file = "data.csb"
        with self.assertRaises(FileNotFoundError):
            read_data(input_file, self.def_col_ind)

    def test_read_data_file_not_found(self):
        input_file = "nonexistent.csv"
        with self.assertRaises(FileNotFoundError):
            read_data(input_file, self.def_col_ind)


if __name__ == '__main__':
    unittest.main()
