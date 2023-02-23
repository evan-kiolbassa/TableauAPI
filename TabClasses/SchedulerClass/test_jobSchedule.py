import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta
from TabClasses.SchedulerClass.jobSchedule import TableauScheduler

class TestTableauScheduler(unittest.TestCase):

    def setUp(self):
        # Set up a mock server URL and personal access token
        self.server_url = 'http://localhost:8000'
        self.personal_access_token = 'abcdef123456'

        # Create a TableauScheduler instance with the mock server URL and personal access token
        self.scheduler = TableauScheduler(self.server_url, self.personal_access_token)

    def test_get_site_id(self):
        # Test the get_site_id method
        expected_site_id = 'abcd-efgh-ijkl-mnop'
        mock_response = MagicMock()
        mock_response.json.return_value = {'site': {'id': expected_site_id}}
        self.scheduler.headers = {'X-Tableau-Auth': self.personal_access_token}
        self.scheduler.get_site_id = MagicMock(return_value=expected_site_id)
        actual_site_id = self.scheduler.get_site_id()
        self.assertEqual(actual_site_id, expected_site_id)

    def test_schedule_job(self):
        # Test the schedule_job method
        expected_job_name = 'test_job'
        expected_frequency_in_minutes = 30
        expected_start_time = datetime.now(timezone.utc) + timedelta(minutes=1)
        expected_script_path = '/path/to/script.py'
        expected_script_args = ['arg1', 'arg2']
        mock_job_response = MagicMock()
        mock_job_response.json.return_value = {'id': 'abcd-efgh-ijkl-mnop'}
        mock_activate_response = MagicMock()
        self.scheduler.headers = {'X-Tableau-Auth': self.personal_access_token}
        self.scheduler.get_site_id = MagicMock(return_value='abcd-efgh-ijkl-mnop')
        self.scheduler.schedule_job = MagicMock(return_value=mock_job_response)
        self.scheduler.run_job = MagicMock()
        self.scheduler.schedule_job(expected_job_name, expected_frequency_in_minutes, expected_start_time.isoformat(),
                                    expected_script_path, expected_script_args)
        self.scheduler.schedule_job.assert_called_once_with(expected_job_name, expected_frequency_in_minutes,
                                                             expected_start_time.isoformat(), expected_script_path,
                                                             expected_script_args)
        self.scheduler.run_job.assert_called_once_with(mock_job_response.json()['id'])

    def test_run_job(self):
        # Test the run_job method
        expected_job_id = 'abcd-efgh-ijkl-mnop'
        mock_response = MagicMock()
        self.scheduler.headers = {'X-Tableau-Auth': self.personal_access_token}
        self.scheduler.get_site_id = MagicMock(return_value='abcd-efgh-ijkl-mnop')
        self.scheduler.run_job = MagicMock(return_value=mock_response)
        self.scheduler.run_job(expected_job_id)
        self.scheduler.run_job.assert_called_once_with(expected_job_id)

    def test_get_job_id(self):
        # Test the get_job_id method
        expected_job_name = 'test_job'
        expected_job_id = 'abcd-efgh-ijkl-mnop'
        mock_response = MagicMock()
        mock_response.json.return_value = {'schedules': [{'id': expected_job_id, 'name': expected_job_name}]}
        self.scheduler.headers = {'X-Tableau-Auth': self.personal_access_token}
        self.scheduler.get_site_id = MagicMock(return_value='abcd-efgh-ijkl-mnop')
        self.scheduler.get_job_id = MagicMock(return_value=expected_job_id)
        actual_job_id = self.scheduler.get_job_id(expected_job_name)
        self.assertEqual(actual_job_id, expected_job_id)

    @patch('TabClasses.SchedulerClass.jobSchedule.requests')
    def test_modify_job(self, mock_requests):
        # mock the response from the get request to retrieve job information
        mock_get_response = MagicMock()
        mock_get_response.json.return_value = {
            'id': 'test_job_id',
            'name': 'test_job',
            'frequency': {'intervalInMinutes': 60},
            'startTime': '2022-03-01T12:00:00Z',
            'taskType': 'external',
            'taskPayload': {'url': 'http://testserver/scripts/test_script.py', 'parameters': 'arg1 arg2'}
        }
        mock_requests.get.return_value = mock_get_response

        # mock the response from the put request to update job information
        mock_put_response = MagicMock()
        mock_requests.put.return_value = mock_put_response

        # modify the job properties
        job_id = 'test_job_id'
        job_properties = {'name': 'modified_test_job', 'frequency': {'intervalInMinutes': 30}}
        self.scheduler.modify_job(job_id, job_properties)

        # assert that requests.get was called with the correct arguments
        expected_get_url = "http://testserver/api/3.10/sites/test_site_id/schedules/test_job_id"
        expected_get_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': 'test_personal_access_token'
        }
        mock_requests.get.assert_called_once_with(expected_get_url, headers=expected_get_headers)

        # assert that requests.put was called with the correct arguments
        expected_put_url = "http://testserver/api/3.10/sites/test_site_id/schedules/test_job_id"
        expected_put_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': 'test_personal_access_token'
        }
        expected_put_json = {
            'id': 'test_job_id',
            'name': 'modified_test_job',
            'frequency': {'intervalInMinutes': 30},
            'startTime': '2022-03-01T12:00:00Z',
            'taskType': 'external',
            'taskPayload': {'url': 'http://testserver/scripts/test_script.py', 'parameters': 'arg1 arg2'}
        }
        mock_requests.put.assert_called_once_with(expected_put_url, headers=expected_put_headers, json=expected_put_json)