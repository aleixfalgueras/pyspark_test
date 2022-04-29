import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession

from lib.genericutils import addAuditColumns
from testresources.fixtures import loadFixtures

class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Unit-tests")
                     .getOrCreate())

        cls.fixtures = loadFixtures(cls.spark)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_something(self):

        self.assertEqual(True, True)  # add assertion here

    def test_etl(self):
        print(self.spark.version) #2.4.7.7.1.7.21-1

        #1. Prepare an input data frame that mimics our source data.
        input_data = [(1, "Bangalore", "2021-12-01", 5),
                      (2,"Bangalore" ,"2021-12-01",3),
                      (5,"Amsterdam", "2021-12-02", 10),
                      (6,"Amsterdam", "2021-12-01", 1),
                      (8,"Warsaw","2021-12-02", 15),
                      (7,"Warsaw","2021-12-01",99)]

        input_df = self.spark.createDataFrame(data=input_data)
        #2. Prepare an expected data frame which is the output that we expect.
        input_df.show()

    def test_addAuditColumns(self):

        df_audit = addAuditColumns(self.fixtures,"1")
        df_audit.show()





if __name__ == '__main__':
    unittest.main()
