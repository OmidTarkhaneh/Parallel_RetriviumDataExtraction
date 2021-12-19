import glob
import itertools
import os

def delete_cmls():
    '''
    This function remove all the extracted cml files to save resources
    '''
    files = glob.glob('./cmlFiles/*.cml')
    for f in files:
        os.remove(f)
