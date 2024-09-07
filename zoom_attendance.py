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
from datetime import datetime, timedelta, timezone

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

    def get_meeting_participants(self, meeting_id):
        # Fetch meeting instances to get the correct UUID for completed meetings
        try:
            instances = self.get_meeting_instances(meeting_id)
            if not instances:
                print(f"No instances found for meeting ID {meeting_id}. Ensure the meeting is completed and try again.")
                return []
            
            # Use the UUID of the last (or desired) instance
            meeting_uuid = instances[-1].get('uuid')
            print(f"Using UUID for meeting instance: {meeting_uuid}")
        except Exception as e:
            print(f"Could not retrieve meeting instances: {e}")
            return []

        # Properly encode the UUID (once, not double-encoded)
        meeting_uuid = urllib.parse.quote(meeting_uuid, safe='')

        # Build the endpoint correctly
        self.endpoint = f"/report/meetings/{meeting_uuid}/participants"
        url = self.base_url + self.endpoint
        self.headers = {
            'Authorization': f'Bearer {self.access_token}'
        }

        print(f"Fetching participants from URL: {url}")

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raises an exception for HTTP errors
            return response.json().get("participants", [])
        except requests.exceptions.HTTPError as e:
            print(f"Error fetching participants for meeting {meeting_uuid}: {response.text}")
            if response.status_code == 404:
                print("Meeting not found: This could be due to incorrect meeting UUID or the meeting not existing/completed.")
            elif response.status_code == 3001:
                print(f"Meeting does not exist: {meeting_uuid}. Check if the UUID is correct and if the meeting is completed.")
            raise e

    
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
                print("Bad Request")
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
        meeting_df['start_time'] = pd.to_datetime(meeting_df['start_time']).dt.tz_convert('Africa/Lagos')

        # Extract the meeting date
        meeting_df['meeting_date'] = meeting_df['start_time'].dt.date

        # If meeting_date is not provided, use the current date in the desired timezone
        if meeting_date:
            current_date = pd.to_datetime(meeting_date).date()
        else:
            desired_timezone = pytz.timezone('Africa/Lagos')
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
        session_details = []
        for i in range(len(filtered_row)):
            meeting_to_process = filtered_row.iloc[i]
            meeting_id = meeting_to_process['id']
            topic = meeting_to_process['topic']
            meeting_date = meeting_to_process['meeting_date']
            meeting_duration = meeting_to_process['duration']
            start_time = meeting_to_process['start_time']
            host_name = meeting_to_process['host_id']  # Assuming host name is available in host_id or another field

            # Fetch participants for each meeting
            attendance = self.get_meeting_participants(meeting_id)

            # Combine meeting details with participant data
            attendance_combined = [{**item, 'Session Title': topic, 'Session Date and Time': start_time,
                                    'Session ID': meeting_id, 'Host Name': host_name, 
                                    'meeting_date': meeting_date, 'meeting_duration': meeting_duration, 
                                    'start_time': start_time}
                                for item in attendance]
            attendance_combined = pd.DataFrame(attendance_combined)

            # Append the combined data to the result DataFrame
            result = pd.concat([result, attendance_combined], ignore_index=True)

            # Create detailed session information
            session_info = {
                'Session Title': topic,
                'Session Date and Time': start_time,
                'Session ID': meeting_id,
                'Host Name': host_name
            }
            session_details.append(session_info)

        print("Session Details:")
        for session in session_details:
            print(f"Session Title: {session['Session Title']}")
            print(f"Session Date and Time: {session['Session Date and Time']}")
            print(f"Session ID: {session['Session ID']}")
            print(f"Host Name: {session['Host Name']}")
            print("\n")

        return result

class HubSpot:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.hubapi.com"

    def _create_headers(self):
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }

    def update_contact_property(self, contact_id, properties):
        url = f"{self.base_url}/crm/v3/objects/contacts/{contact_id}"
        data = {
            "properties": properties
        }
        response = requests.patch(url, headers=self._create_headers(), json=data)
        response.raise_for_status()
        return response.json()

    def create_contact(self, email, properties):
        url = f"{self.base_url}/crm/v3/objects/contacts"
        data = {
            "properties": {
                "email": email,
                **properties
            }
        }
        response = requests.post(url, headers=self._create_headers(), json=data)
        response.raise_for_status()
        return response.json()

    def find_contact_by_email(self, email):
        url = f"{self.base_url}/crm/v3/objects/contacts/search"
        data = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "EQ",
                    "value": email
                }]
            }]
        }
        response = requests.post(url, headers=self._create_headers(), json=data)
        response.raise_for_status()
        results = response.json().get('results', [])
        return results[0] if results else None




def convert_to_midnight_utc(timestamp):
    dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    midnight_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight_dt.timestamp() * 1000)

def format_datetime_for_hubspot(dt):
    if pd.notnull(dt):
        if isinstance(dt, str):
            try:
                # Assuming join and leave times are in ISO 8601 format; adjust as necessary
                dt = datetime.fromisoformat(dt)
            except ValueError:
                print(f"Unable to parse date: {dt}")
                return None
        return dt.strftime('%Y-%m-%d %H:%M:%S')  # Format as a string
    return None

def calculate_total_duration(join_time_str, leave_time_str):
    # Parse join and leave times
    if join_time_str and leave_time_str:
        try:
            join_time = datetime.fromisoformat(join_time_str)
            leave_time = datetime.fromisoformat(leave_time_str)
            # Calculate the duration in minutes
            total_duration = (leave_time - join_time).total_seconds() / 60  # Duration in minutes
            return round(total_duration, 2)  # Round to two decimal places for precision
        except ValueError:
            print(f"Error parsing join or leave times: join_time={join_time_str}, leave_time={leave_time_str}")
    return None

def send_zoom_data_to_hubspot(zoom_instance, hubspot_instance):
    # Extract meeting attendance data from Zoom
    data = zoom_instance.get_meeting_attendance()

    if data.empty:
        print("No attendance data to send to HubSpot.")
        return

    # Iterate over each participant in the Zoom data
    for index, row in data.iterrows():
        email = row.get('user_email')
        participant_name = row.get('name')
        session_title = row.get('Session Title')
        session_date = row.get('Session Date and Time')

        # Ensure session_date is set to midnight UTC
        if pd.notnull(session_date):
            session_date = session_date.replace(hour=0, minute=0, second=0, microsecond=0)
            session_date_timestamp = int(session_date.timestamp() * 1000)
            session_date_timestamp = convert_to_midnight_utc(session_date_timestamp)

        # Additional participant details
        join_time_str = row.get('join_time')
        leave_time_str = row.get('leave_time')
        join_time = format_datetime_for_hubspot(join_time_str)
        leave_time = format_datetime_for_hubspot(leave_time_str)
        total_duration = calculate_total_duration(join_time_str, leave_time_str)  # Calculate duration

        # Find or create the contact in HubSpot
        if email:
            contact = hubspot_instance.find_contact_by_email(email)
            properties = {
                'firstname': participant_name.split()[0] if participant_name else '',
                'lastname': participant_name.split()[-1] if participant_name else '',
                'last_zoom_session_title': session_title,
                'last_zoom_session_date': session_date_timestamp,
                'zoom_participant_join_time': join_time,
                'zoom_participant_leave_time': leave_time,
                'zoom_participant_total_duration': total_duration  # Add calculated duration
            }

            if contact:
                contact_id = contact['id']
                # Update the contact with meeting data
                hubspot_instance.update_contact_property(contact_id, properties)
                print(f"Updated HubSpot contact {email} with session data.")
            else:
                # If the contact does not exist, create it
                hubspot_instance.create_contact(email, properties)
                print(f"Created new HubSpot contact for {email}.")

# Replace the credentials with your actual Zoom and HubSpot API credentials
zoom_instance = Zoom(client_id='ujqRRTR5Qey_e7oIAF3oMw', client_secret='9qfonnzQwiLDCsNO9xjrYR47BJOlg9SQ', account_id='WE69OOT2TFaXpWXLtGo7gg')
hubspot_instance = HubSpot(api_key='')

# Call the function to integrate and send data
send_zoom_data_to_hubspot(zoom_instance, hubspot_instance)

