import numpy as np
import pandas

from json import loads
from os import listdir
from os.path import isfile, isdir, join
from subprocess import run, PIPE


QUERY_PATH = "/home/dan/data/garbage/git/rumble-root-queries/queries"


def process_result(res):
	"""
	This method processes the results returned by a JSONiq query, and returns a valid json data structure, 
	equivalent to the initial result. 
	"""
	l_idx = [idx for idx, c in enumerate(res) if c == '{']
	r_idx = [idx for idx, c in enumerate(res) if c == '}']
	
	if len(l_idx) != len(r_idx):
		raise Exception("The number of '{' characters is not equal to the number of '}' characters: {} - {}".format(len(l_idx), len(r_idx)))

	# Create a pandas DataFrame from the results
	df = pandas.DataFrame(np.random.rand(len(l_idx), 2), columns=['x', 'y'])

	for idx, t in enumerate(zip(l_idx, r_idx)):
		d = loads(res[t[0]: t[1] + 1])
		df.iloc[idx, 0], df.iloc[idx, 1] = d['x'], d['y'] 

	return df


def execute_query(path):
	"""
	Executes a query located at the specified path, and compares the output to the contents of the the `ref.csv` file
	located at the same path.

	:param path: the path to where the .jq and the ref.csv file is located
	:return: True if the results match, false otherwise
	"""
	print("We're at query folder", path)
	query_file = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and f.endswith(".jq")][0]
	ground_truth_file = [join(path, f) for f in listdir(path) if isfile(join(path, f)) and f == "ref.csv"][0]

	# Note: One must have added the run_job.sh script to the PATH environment variable 
	ground_truth = pandas.read_csv(ground_truth_file)
	processed_result = process_result(run(['run_job.sh', query_file], stdout=PIPE).stdout.decode('utf-8'))

	# Remove the entries in the histogram which have 0 frequency, and prepare for testing
	processed_result = processed_result[processed_result['y'] > 0]
	processed_result.reset_index(drop=True, inplace=True)

	ground_truth = ground_truth[ground_truth['y'] > 0]
	ground_truth.reset_index(drop=True, inplace=True)

	# The two DataFrames should be equal
	return pandas.testing.assert_frame_equal(processed_result, ground_truth, check_dtype=False)


def execute_queries(path):
	"""
	Execute the queries located the specified path. The folder should contain one subfolder per query. The outputs
	are compared against ground truths. If all the outputs 

	:param path: The location of the directory where the query folders are located.
	:return: True if all the queries are executed successfully or False otherwise
	"""
	success_lst = []
	error_dict = {}

	for folder in [f for f in listdir(path) if isdir(join(path, f))]:
		dir_path = join(path, folder)
		try:
			execute_query(dir_path)
		except IndexError as e:
			print("No .jq or .csv at path", dir_path)
		except AssertionError as e:
			error_dict[dir_path] = str(e)
		else:
			success_lst.append(dir_path)

	# Print the results of the evaluation	
	if success_lst:
		print("The following queries succeeded:")
		for l in success_lst:
			print(" >> {}".format(l))

	if error_dict:
		print("The following queries failed:")
		for k, v in error_dict.items():
			print(" >> {}: {}".format(k, v))

	# Return True if no errors, False otherwise
	return error_dict is None


def main():
	execute_queries(QUERY_PATH)
	# print(execute_query('/home/dan/data/garbage/git/rumble-root-queries/queries/q3'))


if __name__ == '__main__':
	main()
