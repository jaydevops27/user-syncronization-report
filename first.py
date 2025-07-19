import os
import requests
import msal
from tabulate import tabulate
import psycopg2
import logging
import sys
from datetime import datetime
from io import StringIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

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
    """Get the Group ID of the specified group"""
    logger.info(f"Fetching group ID for {group_name}")
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    response = requests.get(
        f"{AZURE_CONFIG['graph_api_url']}/groups",
        headers=headers,
        params={'$filter': f'displayName eq \'{group_name}\'', '$select': 'id'}
    )
    
    response.raise_for_status()
    
    groups = response.json().get('value', [])
    if not groups:
        logger.error(f"Group '{group_name}' not found")
        raise Exception(f"Group '{group_name}' not found")
    
    logger.info(f"Successfully fetched group ID for {group_name}")
    return groups[0]['id']

def get_group_members(access_token, group_id):
    """Get the members of the specified group"""
    logger.info(f"Fetching members for group ID: {group_id}")
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
    """Fetch usernames from the database"""
    logger.info("Fetching users from PostgreSQL database")
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT username FROM pg_catalog.pg_user ORDER BY username;")
                users = [user[0] for user in cur.fetchall() if user[0] not in DEFAULT_USERS]
                logger.info(f"Fetched {len(users)} users from PostgreSQL")
                return users
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error while fetching PostgreSQL users: {e}")
        return []

def generate_html_report(table_data, headers, summary_info, users_to_delete):
    """Generate a beautiful HTML report"""
    
    # Generate timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>User Synchronization Report</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                line-height: 1.6;
                margin: 0;
                padding: 20px;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background: white;
                border-radius: 15px;
                box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #2c3e50 0%, #34495e 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 2.5rem;
                font-weight: 300;
                letter-spacing: 2px;
            }}
            .header p {{
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 1.1rem;
            }}
            .content {{
                padding: 40px;
            }}
            .summary {{
                background: linear-gradient(135deg, #74b9ff 0%, #0984e3 100%);
                color: white;
                padding: 25px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 10px 20px rgba(116, 185, 255, 0.3);
            }}
            .summary h2 {{
                margin: 0 0 20px 0;
                font-size: 1.8rem;
                font-weight: 300;
            }}
            .summary-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 15px;
            }}
            .summary-item {{
                background: rgba(255, 255, 255, 0.1);
                padding: 15px;
                border-radius: 8px;
                backdrop-filter: blur(10px);
            }}
            .summary-item strong {{
                display: block;
                font-size: 1.1rem;
                margin-bottom: 5px;
            }}
            .table-section {{
                margin: 30px 0;
            }}
            .table-section h2 {{
                color: #2c3e50;
                border-bottom: 3px solid #3498db;
                padding-bottom: 10px;
                margin-bottom: 20px;
                font-weight: 300;
            }}
            .table-container {{
                overflow-x: auto;
                border-radius: 10px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
            }}
            th {{
                background: linear-gradient(135deg, #636e72 0%, #2d3436 100%);
                color: white;
                padding: 15px;
                text-align: left;
                font-weight: 500;
                text-transform: uppercase;
                letter-spacing: 1px;
                font-size: 0.9rem;
            }}
            td {{
                padding: 12px 15px;
                border-bottom: 1px solid #ecf0f1;
            }}
            tr:nth-child(even) {{
                background-color: #f8f9fa;
            }}
            tr:hover {{
                background-color: #e3f2fd;
                transition: background-color 0.3s ease;
            }}
            .status-yes {{
                background: linear-gradient(135deg, #00b894 0%, #00a085 100%);
                color: white;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 0.85rem;
                font-weight: 500;
                display: inline-block;
            }}
            .status-no {{
                background: linear-gradient(135deg, #e17055 0%, #d63031 100%);
                color: white;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 0.85rem;
                font-weight: 500;
                display: inline-block;
            }}
            .status-delete {{
                background: linear-gradient(135deg, #fd79a8 0%, #e84393 100%);
                color: white;
                padding: 6px 12px;
                border-radius: 20px;
                font-size: 0.85rem;
                font-weight: 500;
                display: inline-block;
            }}
            .alert {{
                background: linear-gradient(135deg, #ff7675 0%, #d63031 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                margin: 20px 0;
                box-shadow: 0 10px 20px rgba(255, 118, 117, 0.3);
            }}
            .alert h3 {{
                margin: 0 0 15px 0;
                font-weight: 300;
            }}
            .user-list {{
                background: rgba(255, 255, 255, 0.1);
                padding: 15px;
                border-radius: 8px;
                backdrop-filter: blur(10px);
            }}
            .user-item {{
                background: rgba(255, 255, 255, 0.2);
                margin: 5px 0;
                padding: 8px 12px;
                border-radius: 5px;
                font-family: 'Courier New', monospace;
            }}
            .no-users {{
                background: linear-gradient(135deg, #00b894 0%, #00a085 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                text-align: center;
                margin: 20px 0;
                box-shadow: 0 10px 20px rgba(0, 184, 148, 0.3);
            }}
            .footer {{
                background: #ecf0f1;
                padding: 20px;
                text-align: center;
                color: #636e72;
                font-size: 0.9rem;
            }}
            .default-users {{
                background: linear-gradient(135deg, #a29bfe 0%, #6c5ce7 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                margin: 20px 0;
                box-shadow: 0 10px 20px rgba(162, 155, 254, 0.3);
            }}
            .default-users h3 {{
                margin: 0 0 15px 0;
                font-weight: 300;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🔄 User Synchronization Report</h1>
                <p>QA TFB gHub Migration Analysis</p>
            </div>
            
            <div class="content">
                <div class="summary">
                    <h2>📊 Executive Summary</h2>
                    <div class="summary-grid">
    """
    
    # Add summary information
    for line in summary_info:
        if line.strip():
            html_content += f"""
                        <div class="summary-item">
                            <strong>{line}</strong>
                        </div>
            """
    
    html_content += """
                    </div>
                </div>
                
                <div class="table-section">
                    <h2>👥 User Comparison Details</h2>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
    """
    
    # Add table headers
    for header in headers:
        html_content += f"<th>{header}</th>"
    
    html_content += """
                                </tr>
                            </thead>
                            <tbody>
    """
    
    # Add table rows
    for row in table_data:
        html_content += "<tr>"
        for i, cell in enumerate(row):
            if i == 1:  # "In Azure Group" column
                status_class = "status-yes" if cell == "Yes" else "status-no"
                html_content += f'<td><span class="{status_class}">{cell}</span></td>'
            elif i == 2:  # "In RDS" column
                status_class = "status-yes" if cell == "Yes" else "status-no"
                html_content += f'<td><span class="{status_class}">{cell}</span></td>'
            elif i == 3:  # "Status" column
                if cell == "Needs to be deleted":
                    html_content += f'<td><span class="status-delete">{cell}</span></td>'
                else:
                    status_class = "status-yes"
                    html_content += f'<td><span class="{status_class}">{cell}</span></td>'
            else:
                html_content += f"<td>{cell}</td>"
        html_content += "</tr>"
    
    html_content += """
                            </tbody>
                        </table>
                    </div>
                </div>
    """
    
    # Add users to delete section
    if users_to_delete:
        html_content += f"""
                <div class="alert">
                    <h3>⚠️ Action Required: Users to be Deleted from RDS ({len(users_to_delete)} users)</h3>
                    <div class="user-list">
        """
        for user in sorted(users_to_delete):
            html_content += f'<div class="user-item">🗑️ {user}</div>'
        html_content += """
                    </div>
                </div>
        """
    else:
        html_content += """
                <div class="no-users">
                    <h3>✅ Great News!</h3>
                    <p>No users need to be deleted from RDS. All PostgreSQL users are properly synchronized with Azure AD.</p>
                </div>
        """
    
    # Add default users information
    html_content += f"""
                <div class="default-users">
                    <h3>ℹ️ Default Users (Excluded from Analysis)</h3>
                    <p>The following default PostgreSQL users are automatically excluded from this analysis:</p>
                    <div class="user-list">
    """
    for user in sorted(DEFAULT_USERS):
        html_content += f'<div class="user-item">🔒 {user}</div>'
    
    html_content += f"""
                    </div>
                    <p><strong>Total excluded:</strong> {len(DEFAULT_USERS)} users</p>
                </div>
                
            </div>
            
            <div class="footer">
                <p>Report generated on {timestamp} | Azure Group: {AZURE_GROUP_NAME} | Database: {DB_CONFIG.get('dbname', 'N/A')}</p>
                <p>This is an automated report from the User Synchronization System</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html_content

def write_report(table_data, headers, summary_info, users_to_delete):
    """Write a combined report with user comparison and summary"""
    logger.info("Writing user synchronization report")
    report = StringIO()
    
    report.write("User Synchronization Report for qa TFB gHub migration\n")
    report.write("=" * 70 + "\n\n")
    
    # Write summary information
    report.write("Summary:\n")
    report.write("-" * 8 + "\n")
    for line in summary_info:
        report.write(line + "\n")
    report.write("\n")
    
    # Write user comparison table
    report.write("User Comparison:\n")
    report.write("-" * 16 + "\n")
    report.write(tabulate(table_data, headers=headers, tablefmt="grid"))
    report.write("\n\n")
    
    # Write users that need to be deleted
    if users_to_delete:
        report.write("Users that need to be deleted from RDS:\n")
        for user in sorted(users_to_delete):
            report.write(f"- {user}\n")
    else:
        report.write("No users need to be deleted from RDS.\n")
    
    # Write default users info
    report.write(f"\nDEFAULT_USERS Count:\n")
    report.write(f"Total DEFAULT_USERS: {len(DEFAULT_USERS)}\n")
    for user in sorted(DEFAULT_USERS):
        report.write(f"- {user}\n")
    
    logger.info("Report written successfully")
    return report.getvalue()

def send_email_report(html_content, text_content, users_to_delete):
    """Send email with both HTML and text versions of the report"""
    logger.info("Preparing to send email report")
    
    # Email configuration
    smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    sender_email = os.getenv('SENDER_EMAIL')
    sender_password = os.getenv('SENDER_PASSWORD')
    recipient_emails = os.getenv('RECIPIENT_EMAILS', '').split(',')
    
    if not all([sender_email, sender_password, recipient_emails[0]]):
        logger.error("Email configuration incomplete. Please set SENDER_EMAIL, SENDER_PASSWORD, and RECIPIENT_EMAILS environment variables.")
        return
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = sender_email
        msg['To'] = ', '.join(recipient_emails)
        
        # Dynamic subject based on content
        if users_to_delete:
            msg['Subject'] = f"🚨 User Sync Report - {len(users_to_delete)} Users Need Deletion - {datetime.now().strftime('%Y-%m-%d')}"
        else:
            msg['Subject'] = f"✅ User Sync Report - All Users Synchronized - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Create text and HTML parts
        text_part = MIMEText(text_content, 'plain')
        html_part = MIMEText(html_content, 'html')
        
        # Attach parts
        msg.attach(text_part)
        msg.attach(html_part)
        
        # Also attach the text report as a file for backup
        attachment = MIMEBase('application', 'octet-stream')
        attachment.set_payload(text_content.encode())
        encoders.encode_base64(attachment)
        attachment.add_header(
            'Content-Disposition',
            f'attachment; filename=user_sync_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt'
        )
        msg.attach(attachment)
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        
        logger.info(f"Email report sent successfully to {', '.join(recipient_emails)}")
        
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")

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
        azure_users = get_group_members(azure_token, group_id)
        
        #Fetch postgres users
        postgres_users = fetch_postgres_users()
        
        #Display results
        logger.info("Preparing user comparison data")
        table_data = []
        for user in sorted(postgres_users):
            table_data.append([
                user,
                "Yes" if user in azure_users else "No",
                "Yes",  # All users are in RDS since we're only showing RDS users
                "Valid user" if user in azure_users else "Needs to be deleted"
            ])
        
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
        
        # Generate both HTML and text reports
        html_content = generate_html_report(table_data, headers, summary_info, users_to_delete)
        text_content = write_report(table_data, headers, summary_info, users_to_delete)
        
        # Send email with both formats
        send_email_report(html_content, text_content, users_to_delete)
        
        # Print to console for CI/CD logs
        print(text_content, file=output_stream)
        
        # Write text report to file for CI/CD artifacts
        with open("user_sync_report.txt", "w") as f:
            f.write(text_content)
        
        logger.info("Report generated successfully and sent via email")
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Network error occurred: {str(e)}")
    except ValueError as e:
        logging.error(f"Configuration error: {str(e)}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    
    # Print captured output and logs
    print(output_stream.getvalue())
    print("\nLogs:")
    print(log_stream.getvalue())

if __name__ == "__main__":
    main()
