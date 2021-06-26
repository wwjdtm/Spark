#submit 
from pyspark import SparkConf, SparkContext

def FindTriangle(edges, raw) :

    def takeEdge(x) :
        if x[0] < x[1] :
            return (x[0], x[1])
        else :
            return (x[1], x[0])
    
    words = raw.flatMap(lambda line : line.split("\t"))
    tmp1 = words.map(lambda w : (w, 1))
    wc = tmp1.reduceByKey(lambda a, b : a + b)
    print("#3 count degree = ", wc.take(10))
    print('-----')

    edges1 = edges.map(takeEdge)
    print("edges = ", edges1.take(10))
    print("-----")
 
    mapper_output = edges1.groupByKey().map(lambda x: (x[0], list(x[1]))).sortByKey()
    print("mapper output = ", mapper_output.take(10))
    print("-----")


    def Reducer(x) :
        listt = []
        for a in range(0, len(x[1])):
            for b in range(a+1, len(x[1])) :
                listt.append(((x[1][a],x[1][b]),x[0]))
        return listt

    reducer_output = mapper_output.flatMap(Reducer)
    #print("-----------------------------------------")
    print("reducer output = ",reducer_output.take(10))

    # edge add $ 
    edge_add = edges1.map(lambda x : ((x[0], x[1]),"$"))
    final_reducer_output = edge_add.join(reducer_output)
    print('-----')
    print("join two output = ", final_reducer_output.take(10))

    def findtrianglecnt(x) :
        listt = []
        # check $ and wedge
        if "$" in x[1] :
            values = set(x[1]) - {"$"}
            for value in values :
                listt.append((x[0][0],x[0][1],value))
        return listt

    #triangle list
    aa = final_reducer_output.flatMap(findtrianglecnt)

    #triangle count
    cnt = aa.count()
    
    aa = aa.flatMap(lambda a : a)
    tmp2 = aa.map(lambda w : (str(w),1))
    wc2 = tmp2.reduceByKey(lambda a,b : a+ b)
    print('-----')
    print("#2 triangle cnt = ", wc2.take(10))
    print('-----')
    #join degree + triangle list
    cc = wc.join(wc2)
    #print(cc.take(10))
    
    #take clustering coefficient
    def takeCC(x) :
        output = []
        cc = x[1][1] / (x[1][0] * (x[1][0]-1) / 2)
        output.append((cc, x[0]))
        return output


    cntcc = cc.flatMap(takeCC).sortByKey(False)
    print("#4 clustering coefficient top list = ", cntcc.take(10))
    
    return cnt

    
if __name__ == "__main__" :
    conf = SparkConf().setAppName("find Triangle")
    sc = SparkContext(conf=conf)
    raw = sc.textFile("com-amazon.ungraph.txt")
    edges = raw.map(lambda line : tuple(map(int, line.split("\t"))))
    trai = FindTriangle(edges, raw)
    print("---------------------------")
    print("#1 sum of triangles = : ", trai)
