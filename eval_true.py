# 2015.04.14 22:32:30 EDT
#Embedded file name: eval.py
from subprocess import *
import resource
import time
import sys
print '---------------------------------------------------'
print 'Autograding ...'
print '---------------------------------------------------'
logfile = sys.argv[1]
print 'The stderr will be logged to file ', logfile
commandList = sys.argv[2:len(sys.argv)]
print 'command you are executing ', ' '.join(commandList)
f = open(logfile, 'w')
try:
    output = check_output(" ".join(commandList), shell=True, stderr=f)
except CalledProcessError as e:
    print 'Program failed, please check the log for details'
    exit()

print 'Analyzing your memory usage and Efficiency..\n'
ave_mem = 0
ave_time = 0
testNum = 5
for i in range(0, testNum):
    print 'testing ', i + 1
    start = time.time()
    call(commandList, stderr=f)
    time_ = time.time() - start
    print '\ntime (s):', time_
    mem_ = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    print 'memory (byte):', mem_
    ave_time += time_
    ave_mem += mem_

f.close()
print '\n\naverage time usage (s):', ave_time / testNum
print 'average memory usage (byte):', ave_mem / testNum
#+++ okay decompyling eval.pyc 
# decompiled 1 files: 1 okay, 0 failed, 0 verify failed
# 2015.04.14 22:32:30 EDT
