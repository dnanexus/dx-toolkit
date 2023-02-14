import unittest

from dxpy.utils.sfconnector import DXSFConnector
from dxpy.utils.sfconnector import SFConnectionError

# TODO Currently we need to plug in the credentials manually to run this test. This will be refactored later
#   once we figure out the right way to test Snowflake feature.
@unittest.skip
class DXSFConnectorTests(unittest.TestCase):

    def setUp(self):
        self.sf_config = {
            'user': "<replaceme>",
            'password': "<replaceme>",
            'account': "<replaceme>"
        }
        self.sf_connector = DXSFConnector(config=self.sf_config)

    def tearDown(self):
        if self.sf_connector:
            self.sf_connector.close()

    def test_connect_fail(self):
        sf_connector = DXSFConnector(config={
            'user': "test",
            'password': "test",
            'account': "wrong_account"
        })
        try:
            sf_connector.connect()
        except Exception as e:
            self.assertTrue(isinstance(e, SFConnectionError))

    def test_query(self):
        self.sf_connector.connect()
        cursor = self.sf_connector.cursor()
        cmd = "select type, count(*) from arun_geno_db.public.genotype group by type"
        cursor.execute_async(cmd)
        cursor.query_result(cursor.sfqid)
        self.assertEqual(cursor.rowcount, 3)
        if cursor:
            cursor.close()

    def test_bad_query(self):
        self.sf_connector.connect()
        cursor = self.sf_connector.cursor()
        cmd = "select1 type, count(*) from arun_geno_db.public.genotype group by type"
        try:
            cursor.execute_async(cmd)
            print(cursor.query_result(cursor.sfqid))
            # iterate over the result
            for row in cursor:
                print(row)
            if cursor:
                cursor.close()
        except Exception as e:
            self.assertTrue('SQL compilation error' in str(e.msg))


if __name__ == '__main__':
    unittest.main()
