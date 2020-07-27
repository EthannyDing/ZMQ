import sys, os, subprocess, time, json

def clean(*args):

    # CONFIG = args[0]
    # index = args[1]
    print("TM Cleaner is working")
    time.sleep(2)
    print('Cleaning is done.')


if __name__ == '__main__':

    CONFIG, index = sys.argv[1:]
    clean(CONFIG, index)
