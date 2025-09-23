"""
Utilities for managing BigQuery dataset authorizations using REST API calls.
"""
import json
import requests
from typing import List, Dict, Any
from google.auth.transport.requests import Request


class DatasetAuthorizationManager:
    """Manager for BigQuery dataset authorization operations using REST API."""

    def __init__(self, client):
        """
        Initialize the authorization manager.

        Args:
            client: BigQuery client instance with credentials
        """
        self.client = client
        self.credentials = client._credentials

    def _get_access_token(self) -> str:
        """
        Get access token for API calls.

        Returns:
            str: Valid access token
        """
        if not self.credentials.valid:
            self.credentials.refresh(Request())
        return self.credentials.token

    def _make_api_request(self, method: str, url: str, data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Make authenticated API request to BigQuery.

        Args:
            method: HTTP method (GET, PUT, etc.)
            url: API endpoint URL
            data: Request payload (for PUT requests)

        Returns:
            Dict[str, Any]: API response as JSON

        Raises:
            requests.HTTPError: If API request fails
        """
        headers = {
            'Authorization': f'Bearer {self._get_access_token()}',
            'Content-Type': 'application/json'
        }
        response = requests.request(method, url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()

    def _parse_dataset_id(self, dataset_id: str) -> tuple[str, str]:
        """
        Parse dataset ID into project and dataset components.

        Args:
            dataset_id: Dataset reference (e.g., 'project.dataset' or 'dataset')

        Returns:
            tuple[str, str]: (project_id, dataset_name)
        """
        parts = dataset_id.split('.')
        if len(parts) == 2:
            return parts[0], parts[1]
        else:
            return self.client.project, parts[0]

    def add_authorized_dataset(self, shared_dataset_id: str, authorized_dataset_id: str) -> str:
        """
        Add dataset authorization using REST API.

        Args:
            shared_dataset_id: Dataset that contains the data to be shared
            authorized_dataset_id: Dataset whose views will be authorized to access shared dataset

        Returns:
            str: Success message

        Raises:
            requests.HTTPError: If API request fails
        """
        # Parse dataset IDs
        shared_project, shared_dataset = self._parse_dataset_id(shared_dataset_id)
        auth_project, auth_dataset = self._parse_dataset_id(authorized_dataset_id)

        # 1. GET current dataset metadata
        get_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{shared_project}/datasets/{shared_dataset}"
        dataset_metadata = self._make_api_request('GET', get_url)

        # 2. Check if already authorized
        access_list = dataset_metadata.get('access', [])
        for entry in access_list:
            if (entry.get('dataset', {}).get('dataset', {}).get('project_id') == auth_project and
                entry.get('dataset', {}).get('dataset', {}).get('dataset_id') == auth_dataset):
                return f"Dataset {authorized_dataset_id} is already authorized to access {shared_dataset_id}"

        # 3. Add new authorization entry
        new_entry = {
            "dataset": {
                "dataset": {
                    "project_id": auth_project,
                    "dataset_id": auth_dataset
                },
                "target_types": ["VIEWS"]
            }
        }
        access_list.append(new_entry)
        dataset_metadata['access'] = access_list

        # 4. PUT updated metadata
        put_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{shared_project}/datasets/{shared_dataset}"
        self._make_api_request('PUT', put_url, dataset_metadata)

        return f"Successfully authorized {authorized_dataset_id} to access {shared_dataset_id}"

    def remove_authorized_dataset(self, shared_dataset_id: str, authorized_dataset_id: str) -> str:
        """
        Remove dataset authorization using REST API.

        Args:
            shared_dataset_id: Dataset that contains the shared data
            authorized_dataset_id: Dataset whose authorization should be removed

        Returns:
            str: Success message

        Raises:
            requests.HTTPError: If API request fails
        """
        # Parse dataset IDs
        shared_project, shared_dataset = self._parse_dataset_id(shared_dataset_id)
        auth_project, auth_dataset = self._parse_dataset_id(authorized_dataset_id)

        # 1. GET current dataset metadata
        get_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{shared_project}/datasets/{shared_dataset}"
        dataset_metadata = self._make_api_request('GET', get_url)

        # 2. Remove from access array
        access_list = dataset_metadata.get('access', [])
        original_length = len(access_list)

        # Filter out the authorization entry
        access_list = [
            entry for entry in access_list
            if not (entry.get('dataset', {}).get('dataset', {}).get('project_id') == auth_project and
                    entry.get('dataset', {}).get('dataset', {}).get('dataset_id') == auth_dataset)
        ]

        if len(access_list) == original_length:
            return f"Authorization for {authorized_dataset_id} not found in {shared_dataset_id}"

        dataset_metadata['access'] = access_list

        # 3. PUT updated metadata
        put_url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{shared_project}/datasets/{shared_dataset}"
        self._make_api_request('PUT', put_url, dataset_metadata)

        return f"Successfully removed authorization for {authorized_dataset_id} from {shared_dataset_id}"

    def list_authorized_datasets(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        List all authorized datasets for a given dataset.

        Args:
            dataset_id: Dataset to check for authorizations

        Returns:
            List[Dict[str, Any]]: List of authorized dataset information

        Raises:
            requests.HTTPError: If API request fails
        """
        project, dataset = self._parse_dataset_id(dataset_id)

        # GET dataset metadata
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}"
        metadata = self._make_api_request('GET', url)

        # Extract authorized datasets
        authorized = []
        for entry in metadata.get('access', []):
            if 'dataset' in entry and 'dataset' in entry['dataset']:
                dataset_info = entry['dataset']['dataset']
                authorized.append({
                    'project_id': dataset_info['project_id'],
                    'dataset_id': dataset_info['dataset_id'],
                    'target_types': entry['dataset'].get('target_types', ['VIEWS'])
                })

        return authorized

    def show_all_access_entries(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Show all access entries for a dataset.

        Args:
            dataset_id: Dataset to show access entries for

        Returns:
            List[Dict[str, Any]]: All access entries

        Raises:
            requests.HTTPError: If API request fails
        """
        project, dataset = self._parse_dataset_id(dataset_id)

        # GET dataset metadata
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{project}/datasets/{dataset}"
        metadata = self._make_api_request('GET', url)

        return metadata.get('access', [])