![mad_reduce_image](images/mapreduce.webp)
# Map Reduce without Hadoop

The objective of this project is to implement a MapReduce solution which is implemented 
in parallel without the use of Hadoop to determine the passenger(s) 
from a provided dataset, with the highest number of flights.

This project was implemented using an OOP approach to encourage re-usability in similar tasks
___
## Walkthrough
This program;
* Receives a path to a CSV file
* Reads the data and performs some preprocessing steps
* Maps each word(default is passenger ID) to a number/count (default is count) -> `mapper`
* Appends all the numbers/counts for each ID into a list and sets the list as a value to the ID(key) -> `sort`
* Sums up the numbers/counts for each ID -> `reducer`
* Displays the passengers with the highest number of trips based on how many times their ID appeared in the data.
___
## File description
* `config.yaml` - configure this file to change the outputs.
  * threading: Option for switching between threading and multiprocessing.
  * duplicates: Option to drop duplicates in data or not.
  * path: Path to csv file containing data.
  * header: If the csv file has headers or not.
  * pattern: The program makes sure words match a particular pattern.
  * column_index: Provide the column indices to be used.
  * sum_column: The column to be summed by the Reducer.
  * column1_search: The string/word to be searched for in the data.
  * column2_search: The definition of column to be summed by reducer.
  * The [config file](config.yaml) has detailed comments for better understanding


* `src/simple.py` - This script compares performance of threading and 
multiprocessing with a simplified Map Reduce solution.
  * To run, enter this code in the src folder: `python simple.py`
  * Output:

    ![comparing performance between threading and multiprocessing](images/comparison.png)

  * Threading seemed to more efficient than multiprocessing referring to the results above. It is important to note that
  the data used for the test had just 500 rows.
* `src/main.py` - This script performs MapReduce on a specified dataset using either threads or multiprocessing
and displays the word(s) with the highest count (default is passengers with the most trips)
  * Output:
  
    ![default_results.png](images/default_results.png)
  * By modifying the `config.yaml` file alone, the program was configured to search and display Flight ID(s) with the most 
  Flight Time from another dataset.
    * Output:
  
    ![configured_results.png](images/configured_results.png)

* `src/MapReduce.py`: This script contains the MapReduce implementation.
* `src/testMapReduce.py`: This script contains unit tests for the MapReduce implementation.
  * To run the tests, run this code in src folder: `python testMapReduce.py`
___
## Handling Big Data
The algorithm was tested on a dataset of 1 million rows. Using multiprocessing, the operation completed in about 5.30 seconds
but completed in about 48 seconds using threading. However, testing the algorithm on a small dataset, using threading,
the MapReduce operation completed in less than a second whereas for multiprocessing, it takes about 3 seconds.

System specs: 12 core-cpu, 16gb RAM.

All experiments can be done by modifying the configuration file accordingly
* small dataset - data\AComp_Passenger_data.csv

* large dataset - data\AComp_Passenger_data_duplicates.csv (set duplicates to false in config file)

---
## Dependencies
* **Python3**: This program uses the `concurrent.futures` module, which isn't available 
in lower versions of Python, e.g., python 2.7.

* This program requires only 2 external libraries - `pandas` and `yaml`.

* The versions of the external libraries used in development have been 
put in a `requirement.txt` file. Run the following command to install:
```
pip install -r requirements.txt
```
Alternatively, you can install the two libraries mentioned above separately by using the following commands:

```
pip install pandas
pip install pyyaml
```
___
## How to run
Execute following command in preferred directory:
```
git clone <https link to repo>

cd map-reduce-using-threads-without-hadoop/src/

python main.py
```

---
## License
[License](LICENSE)