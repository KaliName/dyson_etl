import unittest

import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["customers3"]

import os


class MyTestCase(unittest.TestCase):

    # def test_inner_merge(self):
    #
    # def test_pop(self):

    def test_data_dir(self):
        dir = os.listdir("../data")
        print(dir)
        self.assertTrue(len(dir) != 0)

    def test_db_name(self):
        db_names = myclient.list_database_names()
        print(db_names)
        self.assertTrue(len(db_names) != 0)

    def test_specific_database(self):
        flag = False
        dblist = myclient.list_database_names()

        if "mydatabase" in dblist:
            print("The database exists.")
            flag = True
        return self.assertTrue(flag)

    # def test_to_json_attributes(self):


if __name__ == '__main__':
    unittest.main()
