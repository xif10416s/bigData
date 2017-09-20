import numpy as np
import matplotlib.pyplot as plt
import datetime as dt

from numpy.numarray import arange

duomengCnt = []
hspCnt = []
for line in open("/Users/seki/Downloads/duomengdownload.txt"):
    # print line,
    items = line.split("\t")
    if int(items[1]) <= 50 and int(items[2]) <=300:
        duomengCnt.append(items[1])
        hspCnt.append(items[2])
plt.xlabel('duomeng  download cnt')
plt.ylabel('hsp download cnt')
plt.xticks(arange(0,100,1))
plt.yticks(arange(0,500,10))
plt.scatter(duomengCnt,  hspCnt)
plt.show()
