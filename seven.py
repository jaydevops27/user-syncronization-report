import os
import requests
import msal
from tabulate import tabulate
import psycopg2
import logging
import sys
from datetime import datetime
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
                cur.execute("SELECT usename FROM pg_catalog.pg_user ORDER BY usename;")
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
    """Write a combined report with user comparison and summary"""
    logger.info("Writing user synchronization report")
    report = StringIO()
    
    # HTML Header with T-Mobile styling
    report.write("""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { 
    font-family: 'Segoe UI', Arial, sans-serif; 
    background: #f5f5f5; padding: 20px; color: #333; line-height: 1.5;
}
.container { max-width: 1200px; margin: 0 auto; }
.main-header { 
    background: #e20074; color: white; padding: 20px 25px; border-radius: 8px;
    margin-bottom: 20px; cursor: pointer; box-shadow: 0 2px 8px rgba(226,0,116,0.2);
    transition: all 0.3s ease;
}
.main-header:hover { 
    background: #c8006a; box-shadow: 0 4px 12px rgba(226,0,116,0.3);
}
.main-header h2 { font-size: 24px; font-weight: 600; margin: 0; }
.main-content { display: none; }
.main-content.show { display: block; }
.section { 
    background: white; border-radius: 8px; margin: 15px 0; 
    box-shadow: 0 2px 8px rgba(0,0,0,0.1); overflow: hidden;
}
.section-header { 
    background: #f8f9fa; padding: 15px 20px; cursor: pointer; 
    font-weight: 600; border-bottom: 1px solid #e9ecef;
    transition: background-color 0.3s ease;
}
.section-header:hover { background: #e9ecef; }
.section-content { padding: 20px; display: none; }
.section-content.show { display: block; }
.summary-grid { 
    display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); 
    gap: 15px; margin: 15px 0;
}
.summary-item { 
    background: white; border: 2px solid #e20074; padding: 20px; 
    border-radius: 8px; text-align: center;
    box-shadow: 0 2px 8px rgba(226,0,116,0.1);
}
.summary-item strong { 
    color: #e20074; font-size: 14px; display: block; margin-bottom: 8px;
    text-transform: uppercase; letter-spacing: 0.5px;
}
.stats { 
    font-size: 32px; font-weight: 700; color: #333; margin: 8px 0;
}
table { 
    width: 100%; border-collapse: collapse; margin: 15px 0;
    border: 1px solid #e9ecef; border-radius: 8px; overflow: hidden;
}
th { 
    background: #e20074; color: white; padding: 12px 15px; 
    text-align: left; font-weight: 600; font-size: 14px;
}
td { 
    padding: 10px 15px; border-bottom: 1px solid #e9ecef;
}
tr:nth-child(even) td { background: #f8f9fa; }
tr:hover td { background: #fff0f8; }
.delete { 
    background: #fff3e0; padding: 15px; border-radius: 6px; 
    border-left: 4px solid #ff9800; margin: 10px 0;
}
.delete p { margin: 5px 0; }
.toggle { 
    float: right; font-size: 18px; color: #e20074; 
    transition: transform 0.3s ease;
}
.toggle.rotate { transform: rotate(180deg); }
.detail-text { 
    background: #f8f9fa; padding: 15px; border-radius: 6px; 
    margin: 10px 0; border-left: 4px solid #e20074;
}
.detail-text p { margin: 5px 0; }
@media (max-width: 768px) {
    .summary-grid { grid-template-columns: 1fr; }
    .main-header h2 { font-size: 20px; }
    body { padding: 10px; }
}
</style></head><body>
<div class="container">
<div class="main-header" onclick="toggleMain()">
<h2>📊 User Synchronization Report for QA TFB GHub Migration <span class="toggle" id="mainToggle">▼</span></h2>
</div>

<div class="main-content" id="mainContent">
""")
    
    # Summary Section
    report.write('<div class="section">')
    report.write('<div class="section-header" onclick="toggleSection(\'summary\')">📈 Executive Summary <span class="toggle" id="summaryToggle">▼</span></div>')
    report.write('<div id="summary" class="section-content show">')
    report.write('<div class="summary-grid">')
    
    # Parse summary info for key metrics
    total_rds = total_azure = valid_users = users_delete = 0
    for line in summary_info:
        if "Total users in RDS:" in line:
            total_rds = line.split(':')[1].strip()
        elif "Users in Azure AD Group" in line:
            total_azure = line.split(':')[1].strip()
        elif "Valid users" in line:
            valid_users = line.split(':')[1].strip()
        elif "Users that need to be deleted" in line:
            users_delete = line.split(':')[1].strip()
    
    report.write(f'<div class="summary-item"><strong>RDS Database</strong><div class="stats">{total_rds}</div>Total Users</div>')
    report.write(f'<div class="summary-item"><strong>Azure AD Group</strong><div class="stats">{total_azure}</div>Active Members</div>')
    report.write(f'<div class="summary-item"><strong>Synchronized</strong><div class="stats">{valid_users}</div>Valid Users</div>')
    report.write(f'<div class="summary-item"><strong>Action Required</strong><div class="stats">{users_delete}</div>Users to Remove</div>')
    report.write('</div>')
    
    # Full summary details
    report.write('<div class="detail-text">')
    for line in summary_info:
        report.write(f"<p>• {line}</p>")
    report.write('</div></div></div>')
    
    # User Comparison Table
    report.write('<div class="section">')
    report.write('<div class="section-header" onclick="toggleSection(\'comparison\')">👥 User Comparison Details <span class="toggle" id="comparisonToggle">▼</span></div>')
    report.write('<div id="comparison" class="section-content">')
    report.write('<table>')
    report.write('<tr>')
    for header in headers:
        report.write(f'<th>{header}</th>')
    report.write('</tr>')
    for row in table_data:
        report.write('<tr>')
        for cell in row:
            report.write(f'<td>{cell}</td>')
        report.write('</tr>')
    report.write('</table></div></div>')
    
    # Users to Delete Section
    if users_to_delete:
        report.write('<div class="section">')
        report.write('<div class="section-header" onclick="toggleSection(\'delete\')">⚠️ Users to Delete <span class="toggle" id="deleteToggle">▼</span></div>')
        report.write('<div id="delete" class="section-content">')
        report.write('<div class="delete">')
        report.write('<p><strong>The following users need to be removed from the RDS database:</strong></p>')
        for user in sorted(users_to_delete):
            report.write(f"<p>• {user}</p>")
        report.write('</div></div></div>')
    
    # Default Users Section
    report.write('<div class="section">')
    report.write('<div class="section-header" onclick="toggleSection(\'defaults\')">⚙️ System Default Users <span class="toggle" id="defaultsToggle">▼</span></div>')
    report.write('<div id="defaults" class="section-content">')
    report.write('<div class="detail-text">')
    report.write(f"<p><strong>Total Default Users:</strong> {len(DEFAULT_USERS)}</p>")
    report.write('<p>System accounts excluded from synchronization:</p>')
    for user in sorted(DEFAULT_USERS):
        report.write(f"<p>• {user}</p>")
    report.write('</div></div></div>')
    
    # Close main content and add JavaScript
    report.write('</div></div>')
    
    report.write("""
<script>
function toggleMain() {
    const content = document.getElementById('mainContent');
    const toggle = document.getElementById('mainToggle');
    
    if (content.classList.contains('show')) {
        content.classList.remove('show');
        toggle.classList.remove('rotate');
    } else {
        content.classList.add('show');
        toggle.classList.add('rotate');
    }
}

function toggleSection(sectionId) {
    const content = document.getElementById(sectionId);
    const toggle = document.getElementById(sectionId + 'Toggle');
    
    if (content.classList.contains('show')) {
        content.classList.remove('show');
        toggle.classList.remove('rotate');
    } else {
        content.classList.add('show');
        toggle.classList.add('rotate');
    }
}
</script>
</body></html>""")
    
    logger.info("Report written successfully")
    return report.getvalue()

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
        with open("user_sync_report.html", "w") as f:
            f.write(report_content)
        
        logger.info("Report generated successfully and written to user_sync_report.html")
        
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
