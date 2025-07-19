import os
import requests
import msal
from tabulate import tabulate
import psycopg2
import logging
import sys
from datetime import import datetime
from io import StringIO

#logging
log_stream = StringIO()
logging.basicConfig(stream=log_stream, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Capture stdout
output_stream = StringIO()

# DB Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

#Ignoring default users
DEFAULT_USERS = {'postgres', 'rdsadmin'}

#Azure Ad Group name
AZURE_GROUP_NAME = "test"

# DB Configuration
AZURE_CONFIG = {
    'tenant_id': os.getenv('AZURE_TENANT_ID'),
    'client_id': os.getenv('AZURE_CLIENT_ID'),
    'client_secret': os.getenv('AZURE_CLIENT_SECRET'),
    'graph_api_url': 'https://graph.microsoft.com/v1.0'
}

def get_azure_token():
    """Obtaining an access token for Azure AD using msal"""
    logger.info("Obtaining AzureAD token")
    authority = f"https://login.microsoftonline.com/{AZURE_CONFIG['tenant_id']}"
    app = msal.ConfidentialClientApplication(
        AZURE_CONFIG['client_id'],
        authority=authority,
        client_credential=AZURE_CONFIG['client_secret']
    )
    
    token_response = app.acquire_token_for_client(scopes=['https://graph.microsoft.com/.default'])
    
    if 'access_token' not in token_response:
        logger.error("Failed to obtain access token")
        raise Exception("Failed to obtain access token")
    
    logger.info("Successfully obtained Azure AD token")
    return token_response['access_token']

def get_group_id(access_token, group_name):
    """Get the Group ID of the specific group"""
    logger.info(f"Fetching group ID for {group_name}")
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f"{AZURE_CONFIG['graph_api_url']}/groups",
        headers=headers,
        params={'$filter': f"displayName eq '{group_name}'", '$select': 'id'}
    )
    
    response.raise_for_status()
    
    groups = response.json().get('value', [])
    if not groups:
        logger.error(f"Group '{group_name}' not found")
        raise Exception(f"Group '{group_name}' not found")
    
    logger.info(f"Successfully fetched group ID for {group_name}")
    return groups[0]['id']

def get_group_member(access_token, group_id):
    """Get the member of the specified group"""
    logger.info(f"Fetching member for group ID: {group_id}")
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    members = []
    next_link = f"{AZURE_CONFIG['graph_api_url']}/groups/{group_id}/members?$select=onPremisesSamAccountName"
    
    while next_link:
        logger.info(f"Fetching data from: {next_link}")
        response = requests.get(next_link, headers=headers)
        response.raise_for_status()
        data = response.json()
        members.extend(data.get('value', []))
        next_link = data.get('@odata.nextLink')
    
    logger.info(f"Total members fetched: {len(members)}")
    
    valid_members = {member['onPremisesSamAccountName'].lower() for member in members if 'onPremisesSamAccountName' in member}
    logger.info(f"Valid members with onPremisesSamAccountName: {len(valid_members)}")
    
    return valid_members

def fetch_postgres_users():
    """Fetch username from the database"""
    logger.info("Fetching users from PostgreSQL database")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT username FROM pg_catalog.pg_user ORDER BY username;")
                users = [user[0][6:] if user[0].startswith('test') else user[0] for user in cur.fetchall() if user[0] not in DEFAULT_USERS]
                logger.info(f"Fetched {len(users)} users from PostgreSQL")
                return users
    except psycopg2.Error as e:
        print(f"Database error {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected Error while fetching PostgreSQL users: {e}")
        return []

def write_report(table_data, headers, summary_info, users_to_delete):
    """Write a combined report with user comparison and summary in HTML format"""
    logger.info("Writing user synchronization report")
    
    # Start HTML structure
    report = """
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; color: #333; }
            .header { background-color: #0078d4; color: white; padding: 15px; border-radius: 5px; }
            .summary { background-color: #f8f9fa; padding: 15px; margin: 15px 0; border-left: 4px solid #0078d4; }
            table { border-collapse: collapse; width: 100%; margin: 15px 0; }
            th { background-color: #0078d4; color: white; padding: 10px; text-align: left; }
            td { padding: 8px; border-bottom: 1px solid #ddd; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .delete-section { background-color: #fff3cd; padding: 15px; margin: 15px 0; border-left: 4px solid #ffc107; }
        </style>
    </head>
    <body>
        <div class="header">
            <h2>User Synchronization Report for QA TFB GHub Migration</h2>
        </div>
    """
    
    # Add summary section
    report += '<div class="summary"><h3>Summary</h3>'
    for line in summary_info:
        report += f'<p>{line}</p>'
    report += '</div>'
    
    # Add user comparison table
    report += '<h3>User Comparison</h3>'
    report += '<table>'
    
    # Table headers
    report += '<tr>'
    for header in headers:
        report += f'<th>{header}</th>'
    report += '</tr>'
    
    # Table data
    for row in table_data:
        report += '<tr>'
        for cell in row:
            report += f'<td>{cell}</td>'
        report += '</tr>'
    report += '</table>'
    
    # Add users to delete section
    if users_to_delete:
        report += '<div class="delete-section">'
        report += '<h3>Users that need to be deleted from RDS:</h3>'
        for user in sorted(users_to_delete):
            report += f'<p>• {user}</p>'
        report += '</div>'
    else:
        report += '<div class="summary"><p><strong>No users need to be deleted from RDS.</strong></p></div>'
    
    # Default users section
    report += '<div class="summary">'
    report += f'<h3>DEFAULT_USERS Count:</h3>'
    report += f'<p><strong>Total DEFAULT_USERS: {len(DEFAULT_USERS)}</strong></p>'
    for user in sorted(DEFAULT_USERS):
        report += f'<p>• {user}</p>'
    report += '</div>'
    
    # Close HTML
    report += '</body></html>'
    
    logger.info("Report written successfully")
    return report

def main():
    try:
        logger.info("Starting user synchronization process")
        
        # Check if all required environment variables are set
        required_env_vars = [
            'DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD',
            'AZURE_TENANT_ID', 'AZURE_CLIENT_ID', 'AZURE_CLIENT_SECRET'
        ]
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        #Get Azure token
        azure_token = get_azure_token()
        
        #Get the group ID from "Users users"
        group_id = get_group_id(azure_token, AZURE_GROUP_NAME)
        
        #Get group members
        azure_users = get_group_member(azure_token, group_id)
        
        #Fetch postgres user
        postgres_users = fetch_postgres_users()
        
        #Display results
        logger.info("Preparing user comparison data")
        table_data = [
            [
                user,
                "Yes" if user in azure_users else "No",
                "Yes", #All User are in RDS since we're only showing RDS users
                "Valid user" if user in azure_users else "Needs to be deleted"
            ]
            for user in sorted(postgres_users)
        ]
        
        #display result
        headers = ["NTID", f"In Azure Group ({AZURE_GROUP_NAME})", "In RDS", "Status"]
        
        # Prepare summary info
        valid_users = [user for user in postgres_users if user in azure_users]
        users_to_delete = [user for user in postgres_users if user not in azure_users]
        
        summary_info = [
            f"Report generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total users in RDS: {len(postgres_users)}",
            f"Users in Azure AD Group '{AZURE_GROUP_NAME}': {len(azure_users)}",
            f"Valid users (in both RDS and Azure AD): {len(valid_users)}",
            f"Users that need to be deleted from RDS: {len(users_to_delete)}"
        ]
        
        # generate report
        report_content = write_report(table_data, headers, summary_info, users_to_delete)
        
        #Print report to output stream
        print(report_content, file=output_stream)
        
        #Write report to file
        with open("user_sync_report.txt", "w") as f:
            f.write(report_content)
        
        logger.info("Report generated successfully and written to user_sync_report.txt")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error occurred: {str(e)}")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    
    # Print captured output and logs
    print(output_stream.getvalue())
    print("\nLogs:")
    print(log_stream.getvalue())

if __name__ == "__main__":
    main()
