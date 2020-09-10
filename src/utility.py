import os
import tempfile
import uuid

def generateRandomFile(size):
    """ Generate a randomly named file of size <size> with random contents. """
    
    basename = '{}KB_{}'.format(size//1000, uuid.uuid4().hex)
    absFilename = os.path.join(tempfile.gettempdir(), basename)
    with open(absFilename, 'wb') as f:
        f.write(os.urandom(size))
    return f