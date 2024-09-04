#import libraries
import os
import pytz
import base64
import urllib.parse
import time
import json
import re
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import requests

warnings.filterwarnings("ignore")

class Zoom:

    def __init__(self, client_id, client_secret, account_id, page_size=300) -> None:
        self.auth_url = "https://zoom.us/oauth/token"
        self.base_url = "https://api.zoom.us/v2"
        self.client_id = client_id
        self.client_secret = client_secret
        self.account_id = account_id
        self.headers = {}
        self.access_token = ""
        self.endpoint = ""
        self.page_size = page_size
        self._generate_access_token()

    def _base64_encode(self):
        return base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()

    def _generate_access_token(self):
        data = {
            'grant_type': 'account_credentials',
            'account_id': self.account_id
        }
        self.headers = {
            'Host': 'zoom.us',
            'Authorization': f'Basic {self._base64_encode()}'
        }
        response = requests.post(self.auth_url, data=data, headers=self.headers)
        response.raise_for_status()  # Raises an exception for HTTP errors
        self.access_token = response.json().get("access_token")

    def _get_all_meetings_json(self, next_page_token=""):
        self.endpoint = f"/users/me/meetings?page_size={self.page_size}"
        if next_page_token:
            self.endpoint += f"&next_page_token={next_page_token}"
        url = self.base_url + self.endpoint
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.get(url, headers=self.headers)
        try:
            response.raise_for_status()  # Raises an exception for HTTP errors
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching meetings: {response.text}")
            raise e

        return response.json()

    def get_all_meetings(self):
        response_data = self._get_all_meetings_json()

        # Check if the response contains the 'meetings' key
        if not response_data or 'meetings' not in response_data:
            print("Error: Unable to fetch meetings. Response:", response_data)
            return []  # Return an empty list to avoid iterating over None

        meeting_data = response_data.get("meetings", [])

        # Check for additional pages and fetch them
        next_page_token = response_data.get("next_page_token")
        while next_page_token:
            additional_data = self._get_all_meetings_json(next_page_token=next_page_token)
            meeting_data.extend(additional_data.get("meetings", []))
            next_page_token = additional_data.get("next_page_token")

        result = []

        for meeting in meeting_data:
            meeting_id = meeting.get('id')
            print(meeting_id)
            meeting_instances = self.get_meeting_instances(meeting_id) or []  # Ensure meeting_instances is a list
            for instance in meeting_instances:
                result.append({**meeting, **instance})

        return result

    def get_meeting_details(self, meeting_id):
        self.endpoint = f"/meetings/{meeting_id}"
        url = self.base_url + self.endpoint
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()  # Raises an exception for HTTP errors
        return response.json()

    def _get_meeting_participants_json(self, meeting_id, next_page_token=""):
        self.endpoint = f"/report/meetings/{meeting_id}/participants?page_size={self.page_size}"
        if next_page_token:
            self.endpoint += f"&next_page_token={next_page_token}"
        url = self.base_url + self.endpoint
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raises an exception for HTTP errors
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching participants for meeting {meeting_id}: {response.text}")
            if response.status_code == 400:
                print("Bad Request: Check if the meeting ID is correct and if it is a completed meeting.")
            raise e

    def get_meeting_participants(self, meeting_id, is_instance=False):
        if is_instance:
            meeting_id = urllib.parse.quote(urllib.parse.quote(meeting_id, safe=''))  # Handle instances if needed
        else:
            # Check if a UUID format is required
            try:
                meeting_details = self.get_meeting_details(meeting_id)  # Validate meeting details first
                meeting_id = meeting_details.get('uuid', meeting_id)  # Use UUID if available and required
            except Exception as e:
                print(f"Could not retrieve meeting details: {e}")
                return []

        response_data = self._get_meeting_participants_json(meeting_id=meeting_id)
        meeting_data = response_data.get("participants", [])
        next_page_token = response_data.get("next_page_token")
        while next_page_token:
            data = self._get_meeting_participants_json(meeting_id=meeting_id, next_page_token=next_page_token)
            meeting_data += data.get("participants", [])
            next_page_token = data.get("next_page_token")

        return meeting_data
    
    def get_meeting_instances(self, meeting_id):
        self.endpoint = f"/past_meetings/{meeting_id}/instances"
        url = self.base_url + self.endpoint
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raises an exception for HTTP errors
            return response.json().get("meetings", [])
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching meeting instances: {response.text}")
            if response.status_code == 400:
                print("Bad Request: Please check if the meeting ID is correct and if it is a recurring meeting.")
            raise e
        
    def get_meeting_attendance(self, meeting_date=None):
        meetings = self.get_all_meetings()

        if not meetings:
            print("No meetings found.")
            return

        meeting_df = pd.DataFrame(meetings)
        print("Columns in DataFrame:", meeting_df.columns)

        if 'start_time' not in meeting_df.columns:
            print("Error: 'start_time' column is missing.")
            print("Available columns:", meeting_df.columns)
            return

        # Convert start_time to datetime with West Central Africa Time (WAT) timezone
        meeting_df['start_time'] = pd.to_datetime(meeting_df['start_time']).dt.tz_convert('Africa/Lagos')  # WAT is typically represented by 'Africa/Lagos'

        # Extract the meeting date
        meeting_df['meeting_date'] = meeting_df['start_time'].dt.date

        # If meeting_date is not provided, use the current date in the desired timezone
        if meeting_date:
            current_date = pd.to_datetime(meeting_date).date()  # Use specified date
        else:
            desired_timezone = pytz.timezone('Africa/Lagos')  # West Central Africa Time
            current_datetime = datetime.now(desired_timezone)
            current_date = current_datetime.date()

        print(f"Filtering meetings for date: {current_date}")
        print("Available meeting dates:", meeting_df['meeting_date'].unique())

        # Filter the DataFrame for matching dates
        filtered_row = meeting_df[meeting_df['meeting_date'] == current_date]

        if filtered_row.empty:
            print(f"No meetings scheduled for the specified date: {current_date}.")
            return

        # Process each filtered meeting
        result = pd.DataFrame()
        for i in range(len(filtered_row)):
            meeting_to_process = filtered_row.iloc[i]
            meeting_id = meeting_to_process['id']
            topic = meeting_to_process['topic']
            meeting_date = meeting_to_process['meeting_date']
            meeting_duration = meeting_to_process['duration']
            start_time = meeting_to_process['start_time']
            attendance = self.get_meeting_participants(meeting_id)

            # Combine meeting details with participant data
            attendance_combined = [{**item, 'Topic': topic, 'meeting_date': meeting_date,
                                    'meeting_duration': meeting_duration, 'start_time': start_time}
                                for item in attendance]
            attendance_combined = pd.DataFrame(attendance_combined)

            # Process leave_time and calculate the maximum end_time
            if 'leave_time' in attendance_combined.columns:
                attendance_combined['leave_time'] = pd.to_datetime(attendance_combined['leave_time'])
                attendance_combined['end_time'] = attendance_combined['leave_time'].max()

            # Append the combined data to the result DataFrame
            result = pd.concat([result, attendance_combined], ignore_index=True)

        return result


# Replace the credentials with your actual Zoom API credentials
zoom_instance = Zoom(client_id='YX1hPaIGRxSJmIpTjjCiIQ', client_secret='GcK62zYkWphDyXOFeuMFIVE2qFYYDBnr', account_id='PncShf-GReaX-pPDjJa7jQ')
data = zoom_instance.get_meeting_attendance()
print(data)
