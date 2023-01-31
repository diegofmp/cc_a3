from time import time, ctime
from datetime import datetime

class Custom_Logger():
    def __init__(self, filename="sys_logs.txt"):
        self.filename = filename

    def write(self, action):
        with open(self.filename, 'a') as f:
            f.write('{},{}\n'.format(datetime.now().strftime("%m/%d/%Y-%H:%M:%S"), action))