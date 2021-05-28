""" Developer: Gerald Jones
    Purpose: This module contains several tools for catching and dealing with errors
"""
import traceback
import os
import sys
import time
###################################
#  Printing Error Messages        #
###################################
def get_exception_type():
    return sys.exc_info()[0]
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
def stderr(msg, sep=' ', msg_num=-99):
    eprint(msg, sep=sep)
    quit(msg_num)

