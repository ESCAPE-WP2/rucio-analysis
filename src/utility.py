import os
import random
import string
import tempfile
from datetime import datetime
from pathlib import Path


class bcolors:
    """ Struct-like object to store terminal colour codes. """

    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def generateRandomFile(size, prefix="", suffix=""):
    """
    Generate a randomly named file of size, <size>, with random contents.

    Returns a handle to the file.
    """
    if prefix:  # add file prefix if set.
        prefix += "_"
    if suffix:  # add file suffix if set.
        suffix += "_"
    todaysDatetime = datetime.now().strftime("%d%m%yT%H.%M.%S")
    basename = "{}{}KB_{}{}".format(prefix, size // 1000, todaysDatetime, suffix)
    absFilename = os.path.join(tempfile.gettempdir(), basename)
    with open(absFilename, "wb") as f:
        f.write(os.urandom(size))
    return f


def generateRandomFilesDir(nFiles, size, dirId=1, prefix="", suffix=""):
    """
    Generate a directory of, <nFiles>, of size, <size>, with random contents.
    A directory id, <dirId>, can be passed optionally to avoid naming collisions when
    load testing.

    Returns the path to the created directory.
    """
    if prefix:  # add file prefix if set.
        prefix += "_"
    if suffix:  # add file suffix if set.
        suffix += "_"
    todaysDatetime = datetime.now().strftime("%d%m%yT%H.%M.%S")
    tmpDir = tempfile.gettempdir()
    dirName = "{}{}x{}KB_{}_d{}{}".format(
        prefix, nFiles, size // 1000, todaysDatetime, dirId, suffix
    )

    # Create directory structure.
    #
    Path(os.path.join(tmpDir, dirName)).mkdir(parents=True, exist_ok=True)

    # Create files.
    #
    for idx in range(1, nFiles + 1):
        basename = "{}{}KB_{}_d{}_f{}".format(
            prefix, size // 1000, todaysDatetime, dirId, idx
        )
        absFilename = os.path.join(tmpDir, dirName, basename)
        with open(absFilename, "wb") as f:
            f.write(os.urandom(size))
    return os.path.join(tmpDir, dirName)


def generateMetadataDict(key_prefix, n_num, n_str, n_obj, n_arr, n_bool, n_null):
    """
    Generate a dictionary of metadata comprising data type counts according to the
    passed arguments.
    """

    meta_dict = {}
    for i_num in range(1, n_num + 1):
        meta_dict[key_prefix + "_num_" + str(i_num)] = random.randint(0, 100)
    for i_str in range(1, n_str + 1):
        meta_dict[key_prefix + "_str_" + str(i_str)] = "".join(
            random.choice(string.ascii_lowercase) for _ in range(10)
        )
    for i_obj in range(1, n_obj + 1):
        meta_dict[key_prefix + "_obj_" + str(i_obj)] = {
            "sub_key_{}_1".format(i_obj): 1,
            "sub_key_{}_2".format(i_obj): "value_2",
        }
    for i_arr in range(1, n_arr + 1):
        meta_dict[key_prefix + "_arr_" + str(i_arr)] = [1, 2, 3]
    for i_bool in range(1, n_bool + 1):
        meta_dict[key_prefix + "_bool_" + str(i_bool)] = random.choice([True, False])
    for i_null in range(1, n_null + 1):
        meta_dict[key_prefix + "_null_" + str(i_null)] = None

    return meta_dict
