from session import Session
from logger import Logger
import argparse
import importlib
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', help="tasks file path",
                        default="../etc/tasks/stubs.yml",
                        type=str)
    parser.add_argument('-v', help="verbose?", action='store_true')
    iargs = parser.parse_args()

    # Set default root loggers.
    #
    # These will be overriden by per-task loggers, but provide a failsafe
    # if exceptions occur while instantiating these tasks.
    #
    if iargs.v:
        logger = Logger(level='DEBUG').get()
    else:
        logger = Logger(level='INFO').get()
    Logger(name="rucio", level='INFO')

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

            # Create new per-task loggers.
            #
            if iargs.v:
                logger = Logger(name='{}'.format(class_name), level='DEBUG').get()
            else:
                logger = Logger(name='{}'.format(class_name), level='INFO').get()

            if not enabled:
                logger.warning("Task is not enabled!")
                continue
            try:
                # Import module specified in the task definitiong with the <module_name>
                # field, and assign reference to corresponding <class_name> from this
                # module to <task>.
                #
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

            # Begin task with <args> and <kwargs> as input parameters.
            #
            task.run(args, kwargs)
        except KeyError as e:
            logger.critical("Required key not found in config.")
            logger.critical(repr(e))
            exit()
