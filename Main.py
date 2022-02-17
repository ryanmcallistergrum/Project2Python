import QueryLoader as Ql

if __name__ == '__main__':
    query = Ql.QueryLoader()
    for i in range(1, 12):
        df = query.load_query(i)
        df.show(df.count(), False)
        # query.load_query(i).write.option("headers", "true").csv(f"hdfs://localhost:9000/project2/question{str(
        # i)}.csv")
