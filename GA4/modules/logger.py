import logging
import datetime as dt
import traceback

def myLogger(level,msg,filename):
    logging.basicConfig(filename=filename,
                        format='%(asctime)s %(levelname)s %(message)s')

    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)

    getattr(logger,level)(msg)


def format_exception_logfile(exception:Exception):
    traceback_list = traceback.format_tb(exception.__traceback__)
    tb_str_logfile = ""
    for i in traceback_list:
        str_tuple = str(i).split(",")
        tb_str_logfile += f"\n{str_tuple[0]} in {str_tuple[1]}"
    return f"Exception:{exception.__repr__()}\nTraceback:{tb_str_logfile}"