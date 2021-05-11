import os
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


def generateRandomFile(size, prefix=""):
    """
    Generate a randomly named file of size, <size>, with random contents.

    Returns a handle to the file.
    """
    if prefix:               # add file prefix if set.
        prefix += "_"
    todaysDatetime = datetime.now().strftime("%d%m%yT%H.%M.%S")
    basename = "{}{}KB_{}".format(prefix, size // 1000, todaysDatetime)
    absFilename = os.path.join(tempfile.gettempdir(), basename)
    with open(absFilename, "wb") as f:
        f.write(os.urandom(size))
    return f


def generateRandomFilesDir(nFiles, size, dirId=1, prefix=""):
    """
    Generate a directory of, <nFiles>, of size, <size>, with random contents.
    A directory id, <dirId>, can be passed optionally to avoid naming collisions when
    load testing.

    Returns the path to the created directory.
    """
    if prefix:              # add file prefix if set.
        prefix += "_"
    todaysDatetime = datetime.now().strftime("%d%m%yT%H.%M.%S")
    tmpDir = tempfile.gettempdir()
    dirName = "{}{}x{}KB_{}_d{}".format(
        prefix, nFiles, size // 1000, todaysDatetime, dirId
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
