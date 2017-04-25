import os
import urllib
import zipfile
import filecmp


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


if __name__ == '__main__':
    update_dataset()