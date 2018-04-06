import psycopg2
import random

tableName = 'Y'

try:
    conn=psycopg2.connect(dbname='huyhoang', user='huyhoang')
except:
    print("Unable to connect to the database.")

cur = conn.cursor()

try:
    k = 3  # Number of clusters
    # Getting number of row
    cur.execute("SELECT count(*) FROM "  + tableName)
    numRow = cur.fetchone()[0]
    #print(numRow)
    d = 7
    cur.execute("DROP TABLE IF EXISTS YH")
    cur.execute("CREATE TABLE YH AS "
                "SELECT sum(1) OVER(rows unbounded preceding) AS i, "
                "Y1, Y2, Y3, Y4, Y5, Y6, Y7 FROM " + tableName)
    #print(cur.statusmessage)
    cur.execute("COMMIT")
    #print(cur.statusmessage)

    cur.execute("DROP TABLE IF EXISTS  YV; CREATE TABLE YV(i INT, l INT, v DOUBLE PRECISION); COMMIT")

    for coli in range(1, d + 1):
        cur.execute("INSERT INTO YV SELECT i, " + str(coli) + ", Y" + str(coli) + " FROM YH")
    cur.execute("COMMIT")


    randomIndices = [] # To hold the id of the random row
    randomIndices.append(random.randint(1, numRow+1))
    for i in range(k-1):
        newRandNum = random.randint(1, numRow)
        while (newRandNum in randomIndices):
            newRandNum = random.randint(1, numRow+1)
        randomIndices.append(newRandNum)
    print("random rows: " + str(randomIndices))
    cur.execute("DROP TABLE IF EXISTS C_temp; COMMIT")
    cur.execute("CREATE TABLE C_temp (like YH including all); COMMIT")
    for i in range(k):
        cur.execute("INSERT INTO C_temp SELECT * FROM YH WHERE i = " + str(randomIndices[i]) + "; COMMIT")

    cur.execute("DROP TABLE IF EXISTS CH; COMMIT")
    cur.execute("DROP TABLE IF EXISTS CH")
    cur.execute("CREATE TABLE CH AS "
                "SELECT sum(1) OVER(rows unbounded preceding) AS j, "
                "Y1, Y2, Y3, Y4, Y5, Y6, Y7 FROM C_temp; COMMIT")

    cur.execute("DROP TABLE IF EXISTS  CV; CREATE TABLE CV(j INT, l INT, v DOUBLE PRECISION); COMMIT")
    for coli in range(1, d + 1):
        cur.execute("INSERT INTO CV SELECT j, " + str(coli) + ", Y" + str(coli) + " FROM CH")
    cur.execute("COMMIT")

    cur.execute("DROP TABLE IF EXISTS  C")
    stm = "CREATE TABLE C AS SELECT l"
    for i in range(1, k+1):
        stm += ", MAX(CASE WHEN (j = " + str(i) + ") THEN v ELSE NULL END) AS c" + str(i)
    stm += " FROM CV GROUP BY l; COMMIT"
    cur.execute(stm)

    cur.execute("DROP TABLE IF EXISTS YD")
    stm = "CREATE TABLE YD AS SELECT i"
    for i in range(1, k+1):
        stm += ", sum((YV.v-C.C" + str(i) + ")^2.0) AS d" + str(i)
    stm += " FROM YV, C WHERE YV.l = C.l GROUP BY i; COMMIT"
    cur.execute(stm)

    cur.execute("DROP TABLE IF EXISTS YNN")
    cur.execute("CREATE TABLE YNN(i INT, j INT)")
    stm = "INSERT INTO YNN SELECT i, CASE"
    for x in range(1, k):
        t = 0
        stm += " WHEN "
        for y in range(1, k+1):
            if x != y:
                stm += "d" + str(x) + " <= d" + str(y)
                t += 1
                if t < (k-1):
                    stm += " AND "
        stm += " THEN " + str(x)

    stm += " ELSE " + str(k) + " END FROM YD; COMMIT"
    cur.execute(stm)

    cur.execute("DROP TABLE IF EXISTS NLQ")
    cur.execute("CREATE TABLE NLQ AS "
                "SELECT l, j, sum(1.0) AS N, "
                "sum(YV.v) AS M, "
                "sum(YV.v*YV.v) AS Q "
                "FROM YV,YNN WHERE YV.i = YNN.i GROUP BY l, j;")

    # Initializing table WCR
    cur.execute("DROP TABLE IF EXISTS WCR")
    cur.execute("CREATE TABLE WCR(l INT, j INT, W double precision, C double precision, R double precision); COMMIT")

    for cluster in range(1, k+1):
        stm = "INSERT INTO WCR\n"
        for col in range(1, d+1):
            stm += "SELECT " + str(col) + "," + str(cluster) + ",0,Y" + str(col) + ",0 " \
                                                                                   "FROM CH WHERE j = " + str(cluster)
            if col < d:
                stm += "\nUNION\n"
            else:
                stm += ";"
        cur.execute(stm)






except psycopg2.Error as e:
    print("error: " + str(e))
