import glob
import pandas
from os.path import dirname, join


def reverse_df(path):
	df = pandas.read_csv(path)
	df.iloc[:, 0], df.iloc[:, 1] = df.iloc[:, 1], df.iloc[:, 0].copy()
	df.columns = ['x', 'y']
	df.to_csv(path, index=False)


def main():
	basedir = dirname(__file__)	
	for csv in glob.glob(join(basedir, '**/ref.csv'), recursive=True):
		reverse_df(csv)


if __name__ == '__main__':
	main()
