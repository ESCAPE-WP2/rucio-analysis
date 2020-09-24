import argparse
import importlib
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning) 

from logger import Logger
from session import Session

if __name__ == "__main__":
    parser = argparse.ArgumentParser()  
    parser.add_argument('-t', help="tests file path", default="../etc/tests.yml", 
        type=str)
    parser.add_argument('-v', help="verbose?", action='store_true')
    iargs = parser.parse_args()

    if iargs.v:
        logger = Logger(level='DEBUG').logger
    else:
        logger = Logger(level='INFO').logger

    session = Session(tests=iargs.t, logger=logger)
    for test in session.tests:
        try:
            desc = session.tests[test]['description']
            module_name = session.tests[test]['module_name']
            class_name = session.tests[test]['class_name']
            enabled = session.tests[test]['enabled']
            args = session.tests[test]['args']
            kwargs = session.tests[test]['kwargs']
            if not enabled:
                logger.warning("Test is not enabled!")
                continue
            try:
                module = importlib.import_module('{}'.format(module_name))
                test = getattr(module, class_name)(logger)
            except ImportError as e:
                logger.critical("Module not found.")
                logger.critical(repr(e))
                exit()          
            except AttributeError as e:
                logger.critical("Class not found.")
                logger.critical(repr(e))
                exit() 

            test.run(args, kwargs)
        except KeyError as e:
            logger.critical("Required key not found in config.")
            logger.critical(repr(e))
            exit()
            


