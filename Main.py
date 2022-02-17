import QueryLoader as Ql

if __name__ == '__main__':
    query = Ql.QueryLoader()
    for i in range(1, 12):
        query.load_query(i).write.option("headers", "true").csv("hdfs://localhost:9000/project2/question" + str(i) +
                                                                ".csv")
