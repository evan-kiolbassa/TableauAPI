import unittest
from TabClasses.HyperAPI.hyperQuery import TableauDataSource

class TestTableauDataSource(unittest.TestCase):
    def setUp(self):
        self.ds = TableauDataSource('test_datasource.hyper')

    def test_create_table(self):
        # test creating a new table with valid input
        self.ds.create_table('test_table', {'id': 'INTEGER', 'name': 'VARCHAR(50)'})
        self.assertEqual(len(self.ds.connection.catalog.get_tables()), 1)
        self.assertEqual(self.ds.connection.catalog.get_tables()[0].name, 'test_table')

        # test creating a table with an empty definition
        with self.assertRaises(ValueError):
            self.ds.create_table('test_table2', {})

        # test creating a table with a duplicate name
        with self.assertRaises(ValueError):
            self.ds.create_table('test_table', {'id': 'INTEGER', 'name': 'VARCHAR(50)'})

    def test_append_rows(self):
        # create a new table for testing
        self.ds.create_table('test_table', {'id': 'INTEGER', 'name': 'VARCHAR(50)'})

        # test appending rows with valid input
        self.ds.append_rows('test_table', [(1, 'John'), (2, 'Jane')])
        result = self.ds.connection.execute_query('SELECT * FROM test_table')
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][1], 'John')
        self.assertEqual(result[1][1], 'Jane')

        # test appending rows to a nonexistent table
        with self.assertRaises(ValueError):
            self.ds.append_rows('nonexistent_table', [(3, 'Bob')])

        # test appending rows with an invalid number of columns
        with self.assertRaises(ValueError):
            self.ds.append_rows('test_table', [(4, 'Tom', 'tom@example.com')])

    def test_update_rows(self):
        # create a new table for testing
        self.ds.create_table('test_table', {'id': 'INTEGER', 'name': 'VARCHAR(50)'})
        self.ds.append_rows('test_table', [(1, 'John'), (2, 'Jane')])

        # test updating rows with valid input
        self.ds.update_rows('test_table', "UPDATE test_table SET name = 'Mary' WHERE id = 1")
        result = self.ds.connection.execute_query('SELECT * FROM test_table WHERE id = 1')
        self.assertEqual(result[0][1], 'Mary')

        # test updating rows in a nonexistent table
        with self.assertRaises(ValueError):
            self.ds.update_rows('nonexistent_table', "UPDATE nonexistent_table SET name = 'Bob' WHERE id = 3")

    def test_delete_rows(self):
        # create a new table for testing
        self.ds.create_table('test_table', {'id': 'INTEGER', 'name': 'VARCHAR(50)'})
        self.ds.append_rows('test_table', [(1, 'John'), (2, 'Jane'), (3, 'Bob')])

        # test deleting rows with valid input
        self.ds.delete_rows('test_table', "DELETE FROM test_table WHERE id = 1")
        result = self.ds.connection.execute_query('SELECT * FROM test_table')
        self.assertEqual(len(result), 2)

        # test deleting rows from a nonexistent table
        with self.assertRaises(ValueError):
            self.ds.delete_rows('nonexistent_table', "DELETE FROM nonexistent_table WHERE id = 3")

    def tearDown(self):
        self.ds.connection.close()

    def test_search_jobs_by_id(self):
        # mock a successful response from the server
        self.mock_response.json.return_value = {
            "id": "test_job_id",
            "name": "test_job",
            "frequency": {
                "intervalInMinutes": 60
            },
            "taskType": "external",
            "taskPayload": {
                "url": "http://example.com/script.py",
                "parameters": "--arg1=value1 --arg2=value2"
            },
            "state": "Active",
            "priority": 50,
            "lastRunTime": "2022-03-01T12:00:00Z",
            "createdAt": "2022-02-28T12:00:00Z",
            "updatedAt": "2022-03-01T12:00:00Z"
        }

        # call the search_jobs_by_id method
        job_id = 'test_job_id'
        job_info = self.scheduler.search_jobs_by_id(job_id)

        # assert that requests.get was called with the correct arguments
        expected_url = f"http://testserver/api/3.10/sites/test_site_id/schedules/{job_id}"
        expected_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': 'test_token'
        }
        self.mock_request.assert_called_once_with('GET', expected_url, headers=expected_headers)

        # assert that the correct job information was returned
        self.assertEqual(job_info, {
            "id": "test_job_id",
            "name": "test_job",
            "frequency": {
                "intervalInMinutes": 60
            },
            "taskType": "external",
            "taskPayload": {
                "url": "http://example.com/script.py",
                "parameters": "--arg1=value1 --arg2=value2"
            },
            "state": "Active",
            "priority": 50,
            "lastRunTime": "2022-03-01T12:00:00Z",
            "createdAt": "2022-02-28T12:00:00Z",
            "updatedAt": "2022-03-01T12:00:00Z"
        })

if __name__ == '__main__':
    unittest.main()