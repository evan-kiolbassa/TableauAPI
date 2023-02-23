import requests
import datetime

class TableauScheduler:
    """
    A class for scheduling and managing tasks on Tableau Server
    using the Tableau Server REST API.
    """
    def __init__(self, server_url, personal_access_token):
        # constructor takes the Tableau Server URL and a personal access token for authentication
        self.server_url = server_url
        self.personal_access_token = personal_access_token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': self.personal_access_token
        }
        self.site_id = self.get_site_id()

    def schedule_job(self, job_name, frequency_in_minutes, start_time, script_path, script_args):
        """
        Schedules a job that runs the specified script with the specified
        arguments at the specified frequency.

        Parameters:
            job_name (str): The name of the scheduled job.
            frequency_in_minutes (int): The frequency of the job in minutes.
            start_time (str): The start time of the job in ISO 8601 format ("YYYY-MM-DDTHH:mm:ssZ").
            script_path (str): The path to the script to run.
            script_args (list of str): The list of arguments to pass to the script.
        """

        # create the job
        job_payload = {
            "name": job_name,
            "frequency": {"intervalInMinutes": frequency_in_minutes},
            "startTime": start_time,
            "taskType": "external",
            "taskPayload": {
                "url": script_path,
                "parameters": " ".join(script_args)
            }
        }
        response = requests.post(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules",
                                 headers=self.headers,
                                 json=job_payload)
        response.raise_for_status()
        job_id = response.json()['id']

        # activate the job
        response = requests.put(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                headers=self.headers,
                                json={"state": "Active"})
        response.raise_for_status()

    def run_job(self, job_id):
        """
        Runs a scheduled job immediately.

        Parameters:
            job_id (str): The ID of the job to run.
        """
        # method runs a scheduled job immediately
        response = requests.post(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}/runNow",
                                 headers=self.headers)
        response.raise_for_status()

    def get_site_id(self):
        """
        Retrieves the ID of the current site.

        Returns:
            str: The ID of the current site.
        """
        # method retrieves the ID of the current site
        response = requests.get(f"{self.server_url}/api/3.10/auth/whoami",
                                headers=self.headers)
        response.raise_for_status()
        return response.json()['site']['id']

    def get_job_id(self, job_name):
        """
        Retrieves the ID of a scheduled job by name.

        Parameters:
            job_name (str): The name of the job to search for.

        Returns:
            str or None: The ID of the scheduled job, or None if the job is not found.
        """
        # method retrieves the ID of a scheduled job by name
        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules",
                                headers=self.headers)
        response.raise_for_status()
        jobs = response.json()['schedules']
        for job in jobs:
            if job['name'] == job_name:
                return job['id']
        return None

    def get_all_jobs(self):
        """
        Retrieves a list of all scheduled jobs on the Tableau Server.

        Returns:
            list of dict: A list of dictionaries, where each
            dictionary represents a scheduled job.
        """
        # method retrieves a list of all scheduled jobs
        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules",
                                headers=self.headers)
        response.raise_for_status()
        return response.json()['schedules']

    def search_jobs_by_id(self, job_id):
        """
        Retrieves the information of a scheduled job by ID.

        Parameters:
            job_id (str): The ID of the job to search for.

        Returns:
            dict or None: A dictionary representing the scheduled job, or None if the job is not found.
        """

        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                headers=self.headers)
        response.raise_for_status()
        job_info = response.json()
        if job_info.get('error'):
            # if the job is not found, the response will contain an error message
            return None
        else:
            # return the job information
            return job_info

    def modify_job(self, job_id, job_properties):
        # method modifies the properties of a scheduled job
        # job_id: ID of the job to modify
        # job_properties: dictionary of job properties to update

        # get the current job information
        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                headers=self.headers)
        response.raise_for_status()
        job_info = response.json()

        # update the job properties
        job_payload = job_info.copy()
        job_payload.update(job_properties)

        # prompt the user for confirmation before making changes
        print(f"Current job information for job {job_id}:")
        print(job_info)
        print(f"Proposed changes to job {job_id}:")
        print(job_properties)
        confirmation = input("Do you want to make these changes? (y/n) ")
        if confirmation.lower() == 'y':
            # submit the updated job information
            response = requests.put(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                    headers=self.headers,
                                    json=job_payload)
            response.raise_for_status()
        else:
            print("Changes not submitted.")

    def delete_job(self, job_id):
        # method deletes a scheduled job by ID
        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                headers=self.headers)
        response.raise_for_status()

        job_info = response.json()

        # Prompt to verify deletion of scheduled tasks
        print(f"Are you sure you want to delete the job '{job_info['name']}' (ID: {job_id})?")
        confirmation = input("Enter 'y' to confirm, or 'n' to cancel: ")
        if confirmation.lower() == 'y':
            print("Deleting job...")
            response = requests.delete(f"{self.server_url}/api/3.10/sites/{self.site_id}/schedules/{job_id}",
                                    headers=self.headers)
            response.raise_for_status()
            print("Job deleted.")
        else:
            print("Deletion cancelled.")