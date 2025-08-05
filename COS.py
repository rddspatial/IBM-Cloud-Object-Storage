import ibm_boto3
from ibm_botocore.client import Config
import json
from datetime import datetime
import pandas as pd


# ---------------------------
# IBM COS CONFIGURATION
# ---------------------------
# These information can be found in service credential (HMAC enabled while creating a new service credential) in COS
COS_ENDPOINT = ""          # Example: https://s3.eu-de.cloud-object-storage.appdomain.cloud (This should match with the region where the bucket is created
COS_API_KEY_ID = ""         # Your IBM Cloud API Key
COS_INSTANCE_CRN = ""
BUCKET_NAME = "bucket-itsm-tickets-rd"
OBJECT_NAME = "tickets.json"                  # Object to store tickets

# Initialize COS client
cos = ibm_boto3.client(
    service_name='s3',
    ibm_api_key_id=COS_API_KEY_ID,
    ibm_service_instance_id=COS_INSTANCE_CRN,
    config=Config(signature_version="oauth"),
    endpoint_url=COS_ENDPOINT
)


# ---------------------------
# Helper Functions
# ---------------------------

def _get_all_tickets():
    """Internal: Fetch all tickets as a list"""
    try:
        file = cos.get_object(Bucket=BUCKET_NAME, Key=OBJECT_NAME)['Body'].read().decode('utf-8')
        return json.loads(file)
    except cos.exceptions.NoSuchKey:
        return []  # Return empty list if file doesn't exist yet


def append_ticket(ticket_id, category, subcategory, metadata):
    """Append a new ticket to the COS JSON file"""
    tickets = _get_all_tickets()
    
    new_ticket = {
        "ticket_id": ticket_id,
        "timestamp": datetime.utcnow().isoformat(),
        "category": category,
        "subcategory": subcategory,
        "metadata": metadata
    }
    
    tickets.append(new_ticket)
    
    # Upload updated list to COS
    cos.put_object(
        Bucket=BUCKET_NAME,
        Key=OBJECT_NAME,
        Body=json.dumps(tickets, indent=2)
    )
    print(f"Ticket {ticket_id} appended successfully.")


def read_all_tickets():
    """Read all tickets"""
    tickets = _get_all_tickets()
    return tickets


def read_ticket_by_id(ticket_id):
    """Read a specific ticket by ticket_id"""
    tickets = _get_all_tickets()
    for ticket in tickets:
        if ticket["ticket_id"] == ticket_id:
            return ticket
    return None

def delete_ticket(ticket_id):
    """Delete a ticket by ticket_id"""
    tickets = _get_all_tickets()
    updated_tickets = [t for t in tickets if t["ticket_id"] != ticket_id]
    
    if len(updated_tickets) == len(tickets):
        print(f"No ticket found with ID {ticket_id}.")
        return False
    
    # Upload updated list to COS
    cos.put_object(
        Bucket=BUCKET_NAME,
        Key=OBJECT_NAME,
        Body=json.dumps(updated_tickets, indent=2)
    )
    print(f"Ticket {ticket_id} deleted successfully.")
    return True

def delete_all_tickets():
    """Delete all tickets but keep an empty JSON file"""
    cos.put_object(
        Bucket=BUCKET_NAME,
        Key=OBJECT_NAME,
        Body=json.dumps([], indent=2)
    )
    print("All tickets have been deleted, file reset to empty list.")


# ---------------------------
# Example Usage
# ---------------------------
if __name__ == "__main__":
    #Delete all
    delete_all_tickets()
    # Append a new ticket
    append_ticket(
        ticket_id="T123",
        category="IT",
        subcategory="Network",
        metadata={"user": "john.doe", "priority": "high"}
    )
    print('ticket stored successfully')
    # Read all tickets
    print("All Tickets:", read_all_tickets())

    # Read a specific ticket
   # print("Ticket T123:", read_ticket_by_id("T123"))

   
