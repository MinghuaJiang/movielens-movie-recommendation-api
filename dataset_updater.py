import os
import urllib
import zipfile
import filecmp
from dao import MovieInfoDao
import numpy as np
import pandas as pd
from subprocess import call

def update_dataset():
    datasets_path = os.path.join('.', 'datasets')
    md5_path = os.path.join(datasets_path, 'ml-latest.zip.md5')
    md5_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip.md5'
    if os.path.isfile(md5_path):
        md5_path_bak = os.path.join(datasets_path, 'ml-latest.zip.md5_bak')
        os.rename(md5_path, md5_path_bak)
        urllib.urlretrieve(md5_url, md5_path)
        if filecmp.cmp(md5_path, md5_path_bak, shallow=False):
            os.remove(md5_path_bak)
            return False
        else:
            os.remove(md5_path_bak)
            download_dataset()
            return True
    else:
        urllib.urlretrieve(md5_url, md5_path)
        download_dataset()
        return True


def download_dataset():
    datasets_path = os.path.join('.', 'datasets')
    complete_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'
    small_dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest-small.zip'

    complete_dataset_path = os.path.join(datasets_path, 'ml-latest.zip')
    small_dataset_path = os.path.join(datasets_path, 'ml-latest-small.zip')

    urllib.urlretrieve(small_dataset_url, small_dataset_path)
    urllib.urlretrieve(complete_dataset_url, complete_dataset_path)

    with zipfile.ZipFile(small_dataset_path, "r") as z:
        z.extractall(datasets_path)

    with zipfile.ZipFile(complete_dataset_path, "r") as z:
        z.extractall(datasets_path)

    call("hadoop fs -put datasets")

def refresh_movie_info():
    movieDao = MovieInfoDao()
    movieDao.truncate_table()
    datasets_path = os.path.join('.', 'datasets')
    small_dataset_path = os.path.join(datasets_path, 'ml-latest-small')
    movie_file = os.path.join(small_dataset_path, 'movies.csv')
    link_file = os.path.join(small_dataset_path, 'links.csv')
    movie = pd.read_csv(movie_file)
    link = pd.read_csv(link_file, dtype={'movieId': np.int, 'imdbId': np.str, 'tmdbId': np.str})
    merged = movie.merge(link, on='movieId')
    for row in merged.iterrows():
        movieDao.add_movie_info(row[1]['movieId'], row[1]['title'], row[1]['genres'], row[1]['imdbId'], row[1]['tmdbId'])
    movieDao.bulk_insert()

if __name__ == '__main__':
    #update_dataset()
    refresh_movie_info()