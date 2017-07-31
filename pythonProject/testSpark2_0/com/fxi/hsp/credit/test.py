import numpy as np
import matplotlib.pyplot as plt
import datetime as dt


def testFunc(i):
    return i + 1

rsDic = {}

for line in open("/Users/seki/work/tmp/hsp/userDailyCredit/rsscala_2016_12_13_32768.txt"):
    # print line,
    items = line.split("\t")
    # print items[1]
    if items[1] not in rsDic:
        rsDic[items[1]] = [[], [], [], []]
    rsDic[items[1]][0] += items[0],
    rsDic[items[1]][1] += items[2],
    rsDic[items[1]][2] += items[3],
    rsDic[items[1]][3] += items[4],

rsDicDelt = {}
summary = {}
for i, item in enumerate(rsDic.items()):
    rsDicDelt[item[0]] = [[], [], [], []]
    # if i == 0:
    #     print item[0]
    for j in np.arange(0, len(item[1][3]) - 1):
        if j == 0:
            rsDicDelt[item[0]][0].append(0)
        else:
            rsDicDelt[item[0]][0].append(float(item[1][3][j + 1]) - float(item[1][3][j]))
    for j in np.arange(0, len(item[1][2]) - 1):
        if j == 0:
            rsDicDelt[item[0]][1].append(0)
        else:
            rsDicDelt[item[0]][1].append(float(item[1][2][j + 1]) - float(item[1][2][j]))
    for j in np.arange(0, len(item[1][1]) - 1):
        if j == 0:
            rsDicDelt[item[0]][2].append(0)
        else:
            rsDicDelt[item[0]][2].append(float(item[1][1][j + 1]) - float(item[1][1][j]))
    for j in np.arange(0, len(item[1][0]) - 1):
        rsDicDelt[item[0]][3].append(item[1][0][j])

    for j in np.arange(0, len(item[1][2])):
        if item[1][0][j] not in summary:
            summary[item[1][0][j]] = 0.0
        summary[item[1][0][j]] += float(item[1][2][j])

plt.xlabel('credit')
plt.ylabel('time')

# for i, item in enumerate(rsDic.items()):
#     x = np.arange(0, len(item[1][0]), 1)
#     xt = [dt.datetime.strptime(d, '%Y%m%d').date() for d in item[1][0]]
#     plt.xticks(rotation=27)
#
#     if i <= 1000:
#         plt.subplot(311)
#         plt.title('total credit change')
#         plt.plot(xt, item[1][3])
#         plt.subplot(312)
#         plt.title(' expend change')
#         plt.plot(xt, item[1][2])
#         plt.subplot(313)
#         plt.title('remain change')
#         plt.plot(xt, item[1][1])


for i, item in enumerate(rsDicDelt.items()):
    x = np.arange(0, len(item[1][1]), 1)
    xt = [dt.datetime.strptime(d, '%Y%m%d').date() for d in item[1][3]]
    plt.xticks(rotation=27)

    if i <= 1000:
        plt.subplot(311)
        plt.title('total credit change')
        plt.plot(xt, item[1][0])
        plt.subplot(312)
        plt.title(' expend change')
        plt.plot(xt, item[1][1])
        plt.subplot(313)
        plt.title('remain change')
        plt.plot(xt, item[1][2])

        # print 'item' + len(item[1][1]).__str__()

start = dt.datetime.strptime("20150920", "%Y%m%d")
end = dt.datetime.strptime("20161211", "%Y%m%d")
d = [start + dt.timedelta(days=x) for x in range(0, (end - start).days)]

sumValue = []
for date in d:
    if date.strftime("%Y%m%d") in summary:
        sumValue.append(float(summary[date.strftime("%Y%m%d")]) / len(rsDicDelt.keys()))
    else:
        sumValue.append(0.0)
# plt.plot(414)
plt.figure(2)
plt.title('summary expend')
plt.plot(d, sumValue)

# p1 =[[1,2,3,4],[2,3,4,5]]
# plt.plot(p1[0],p1[1],'g^')
# plt.plot(p1[1],p1[0])
# plt.axis([0,500,0,1000])
plt.show()
print testFunc(10)

plt.show(arange(10))
