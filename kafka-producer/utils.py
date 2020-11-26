import os
import py7zr
import random
import sys
from glob import glob
from urllib.request import urlretrieve

# url to download 1998 spam dataset
SPAM_URL = "http://untroubled.org/spam/1998.7z"


def get_n_random_email_files(n):
    txt_file_path = os.path.join(_get_data_dir(), "*", "*", "*.txt")
    all_txt_files = glob(txt_file_path, recursive=True)
    n = min(n, len(all_txt_files))
    sample_files = random.sample(all_txt_files, n)
    return sample_files


def download_spam_data(data_dir="./data", filename="1998-spam.7z", url=SPAM_URL):
    # creates the data dir if it does not already exist
    data_dir = _get_data_dir(data_dir=data_dir)
    path_to_file = os.path.join(data_dir, filename)
    if not os.path.exists(path_to_file):
        _download_data(filename=path_to_file, url=url)
    _extract_7zip_data(data_dir=data_dir, zip_file=path_to_file)


def _download_data(filename, url):
    # osFilename = os.path.join(filename)
    if not os.path.exists(filename):
        urlretrieve(url, filename)


def _extract_7zip_data(data_dir, zip_file):
    # Ensure data is available
    if not os.path.exists(zip_file):
        sys.exit("no file found! Make sure data is downloaded first!")
    with py7zr.SevenZipFile(zip_file, mode="r") as z:
        z.extractall(data_dir)


def _get_data_dir(data_dir=None):
    if data_dir is None:
        data_dir = os.environ.get("DATA_DIR", os.path.join("usr", "app", "data"))
    data_dir = os.path.expanduser(data_dir)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir
