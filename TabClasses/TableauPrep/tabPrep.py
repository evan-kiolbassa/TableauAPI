import requests
import json
import os
import base64
import uuid

class TableauPrepFlow:
    """
    A class for interacting with Tableau Prep flows using the Tableau Server REST API.
    """

    def __init__(self, server_url, personal_access_token, tabpy_conn_string):
        """
        Constructor for the TableauPrepFlow class.
        Parameters:
            server_url (str): The URL of the Tableau Server.
            personal_access_token (str): The personal access token for authentication.
        """
        self.server_url = server_url
        self.personal_access_token = personal_access_token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': self.personal_access_token
        }
        self.site_id = self.get_site_id()
        self.tabpy_conn_string = tabpy_conn_string


    def get_flow_ids(self, flow_name):
        """
        Retrieves the IDs of all Tableau Prep flows with the specified name.
        Parameters:
            flow_name (str): The name of the Tableau Prep flow.
        Returns:
            list of str: A list of flow IDs.
        """
        response = requests.get(f"{self.server_url}/api/1.0/flows", headers=self.headers)
        response.raise_for_status()
        flows = response.json()['flows']
        return [flow['id'] for flow in flows if flow['name'] == flow_name]

    def create_flow(self, flow_name, flow_description, project_name):
        """
        Creates a new Tableau Prep flow with the specified name.
        Parameters:
            flow_name (str): The name of the new flow.
        Returns:
            str: The ID of the new flow.
        """
        project_id = self.get_project_id(project_name)

        flow_payload = {
            "project": {
                "id": project_id
            },
            "name": flow_name
        }

        response = requests.post(f"{self.server_url}/api/3.11/sites/{self.site_id}/flows",
                                 headers=self.headers,
                                 json=flow_payload)
        response.raise_for_status()

        flow_id = response.json()['id']
        return flow_id

    def add_script(self, flow_id, script_name, script_code, flow_name):
        """
        Adds a Python script to a Tableau Prep flow.
        Parameters:
            flow_id (str): The ID of the Tableau Prep flow.
            script_name (str): The name of the script to add.
            script_path (str): The path to the script to add.
            script_args (list of str): The list of arguments to pass to the script.
        """
        # create the TabPy client
        tabpy_client = tabpy_tools.Client(f'{self.tabpy_conn_string}/')

        # deploy the script to TabPy
        tabpy_client.deploy(script_name, script_code)

        # get the flow ID
        flow_id = self.get_flow_id(flow_name)

        # create the script step
        script_step = {
            "step": {
                "type": "script",
                "name": script_name,
                "arguments": {
                    "script": f"http://localhost:9004/endpoints/{script_name}"
                },
                "id": str(uuid.uuid4())
            },
            "outputConnections": [],
            "inputConnections": []
        }

        # add the script step to the flow
        response = requests.post(f"{self.server_url}/api/1.0/flows/{flow_id}/steps",
                                 headers=self.headers,
                                 json=script_step)
        response.raise_for_status()

    def get_project_id(self, project_name):
        """
        Retrieves the ID of a project by name.
        Parameters:
            project_name (str): The name of the project to search for.
        Returns:
            str or None: The ID of the project, or None if the project is not found or the user does not have
            permission to access it.
        """
        response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/projects",
                                headers=self.headers)
        response.raise_for_status()

        projects = response.json()['projects']
        for project in projects:
            if project['name'] == project_name:
                project_id = project['id']
                # check if the user has permission to access the project
                response = requests.get(f"{self.server_url}/api/3.10/sites/{self.site_id}/projects/{project_id}/permissions",
                                        headers=self.headers)
                if response.status_code == 200:
                    return project_id
                else:
                    print(f"Warning: User does not have permission to access project {project_name}.")
                    return None
        return None
