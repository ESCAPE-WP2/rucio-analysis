import argparse
import importlib
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

from logger import Logger
from session import Session

if __name__ == "__main__":
    parser = argparse.ArgumentParser()  
    parser.add_argument('-t', help="tasks file path", default="../etc/tests.yml",
        type=str)
    parser.add_argument('-v', help="verbose?", action='store_true')
    iargs = parser.parse_args()

    if iargs.v:
        logger = Logger(level='DEBUG').get()
    else:
        logger = Logger(level='INFO').get()
    Logger(name="rucio", level='INFO')          # set Rucio logger format

    session = Session(tasks=iargs.t, logger=logger)
    for task in session.tasks:
        try:
            desc = session.tasks[task]['description']
            module_name = session.tasks[task]['module_name']
            class_name = session.tasks[task]['class_name']
            enabled = session.tasks[task]['enabled']
            args = session.tasks[task]['args']
            kwargs = session.tasks[task]['kwargs']
            kwargs['task_name'] = task

            if iargs.v:
                logger = Logger(name='{}'.format(class_name), level='DEBUG').get()
            else:
                logger = Logger(name='{}'.format(class_name), level='INFO').get()

            if not enabled:
                logger.warning("Task is not enabled!")
                continue
            try:
                module = importlib.import_module('{}'.format(module_name))
                task = getattr(module, class_name)(logger)
            except ImportError as e:
                logger.critical("Module not found.")
                logger.critical(repr(e))
                exit()          
            except AttributeError as e:
                logger.critical("Class not found.")
                logger.critical(repr(e))
                exit()

            task.run(args, kwargs)
        except KeyError as e:
            logger.critical("Required key not found in config.")
            logger.critical(repr(e))
            exit()
