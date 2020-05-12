from os import listdir
from os.path import isfile, isdir, join


QUERY_PATH = "/home/dan/data/garbage/git/rumble-root-queries/queries"


def execute_query(path):
	print("We're at query folder", path)
	query_file = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith(".jq")][0]
	ground_truth = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith(".csv")][0]

	print("Query file:", query_file)
	print("Ground truth:", ground_truth)



def execute_queries(path):
	for folder in [f for f in listdir(path) if isdir(join(path, f))]:
		dir_path = join(path, folder)
		try:
			execute_query(dir_path)
		except Exception as e:
			print("No .jq or .csv at path", dir_path)


def main():
	execute_queries(QUERY_PATH)


if __name__ == '__main__':
	main()
