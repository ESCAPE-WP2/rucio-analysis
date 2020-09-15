import os
import tempfile
import uuid

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def generateRandomFile(size):
    """ Generate a randomly named file of size <size> with random contents. """
    
    basename = '{}KB_{}'.format(size//1000, uuid.uuid4().hex)
    absFilename = os.path.join(tempfile.gettempdir(), basename)
    with open(absFilename, 'wb') as f:
        f.write(os.urandom(size))
    return f