import os
import sys
import shlex
import subprocess as sp
from subprocess import Popen
from datetime import datetime
from csv import writer
import random

folders = ["output" , "script_log" , "log"]
cmd = "rm -rf ../{0}/*.txt  ../{0}/*.csv" 
for folder in folders:
    cmd1 = cmd.format(folder)
    os.system(cmd1)