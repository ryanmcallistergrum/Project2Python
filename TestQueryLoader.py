import unittest

from QueryLoader import QueryLoader


class TestQueryLoader(unittest.TestCase):
    ql = QueryLoader()

    def test_query_loader(self):
        self.assertIsNotNone(self.ql)

    def test_get_spark_session(self):
        self.assertIsNotNone(self.ql.get_spark_session())

    def test_load_query(self):
        for i in range(1, 12):
            query = self.ql.load_query(i)
            query.show()
            self.assertIsNotNone(query)
