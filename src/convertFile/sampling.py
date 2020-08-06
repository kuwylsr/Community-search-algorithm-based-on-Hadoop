import random



'''
从5000TOP社区中随机抽样100个社区
'''
def randomSample():
    rs = random.sample(range(1,5001),1000)
    print(rs)
    num = 0
    with open('com-orkut.top5000.cmty.txt') as f:
        for line in f:
           num = num + 1
           if num in rs:
               with open("randomSample1000.txt",'a') as f1:
                   f1.write(line)
#====================================================================================
'''
对无向图进行预处理，建立map映射
'''
def preh():
    flag = {}
    num = 0
    with open('com-orkut.ungraph.txt') as f:
        for line in f:
            num = num + 1
            if num % 1000 ==0:
                print("->"+str(num))
            temp2 = line.strip().split('\t')
            if line.split(" ")[0] == "#":
                continue
            flag[temp2[0]] = 1
            flag[temp2[1]] = 1
    return flag
'''
将经过预处理的无向图，根据抽样的社区进行筛选
'''
def filterGraph():
    with open('randomSample.txt') as f:
        temp1 = []
        for line in f:
            temp1 = temp1 + line.strip().split('\t')
    num =0
    content = "\n"
    flag = preh()
    with open('com-orkut.ungraph.txt') as f:
        for line in f:
            num = num + 1
            if num % 1000 ==0:
                print(num)
            temp2 = line.strip().split('\t')
            if line.split(" ")[0] == "#":
                continue
            if (flag[temp2[0]] == 0) or (flag[temp2[1]] ==0):
                continue
            if not(temp2[0] in temp1):
                flag[temp2[0]] = 0
                continue
            if not(temp2[1] in temp1):
                flag[temp2[1]] = 0
                continue
            content = content + line + "\n"
            # if (temp2[0] in temp1) and (temp2[1] in temp1):
            #     # with open('test_com-orkut.ungraph.txt','a') as f1:
            #     #     f1.write(line)
            #     content = content + line + "\n"

'''
处理社区格式，与程序相一致
'''
def handleStaCommunity():
    num = -1
    with open('randomSample1000.txt') as f:
        for line in f:
            num = num + 1
            content = str(num) + "\t" + "[" + line.strip().replace('\t',',') + "]" + "\n"
            with open('StandardCommunityTestExperimentData1000.txt','a') as f1:
                 f1.write(content)

'''
通过处理后的无向图node:neighbors的形式，根据抽样的社区进行筛选
'''
def filterData():
    with open('randomSample1000.txt') as f:
        temp1 = []
        for line in f:
            temp1 = temp1 + line.strip().split('\t')
    print(temp1)
    num = 0
    content = "\n"
    with open('ExperimentData.txt') as f:
        for line in f:
            num = num + 1
            if num%1000==0:
                print(num)

            temp2 = line.strip().split(':')
            if not(temp2[0] in temp1):
                continue
            else:
                tempedge = temp2[1].split(",")
                retA = list(set(list(tempedge)).intersection(set(list(temp1))))
                if retA == []:
                    continue
            content = (temp2[0])+":"+",".join(retA)+"\n"
            with open('cpmTestExperimentData1000.txt','a') as f1:
                 f1.write(content)
def calNodesAndEdges():
    nodeNum = 0
    edgeNum = 0
    with open('cpmScience.txt') as f:
        for line in f:
            nodeNum = nodeNum + 1
            tempnode = line.strip().split(':')
            tempedge = tempnode[1].split(",")
            edgeNum = edgeNum + len(tempedge)
    print("nodeNum: " + str(nodeNum))
    print("edgeNum: " + str(edgeNum))

if __name__ == '__main__':
    #randomSample()
    #handleStaCommunity()
    #filterData()
    calNodesAndEdges()



