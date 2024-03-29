import os
import zipfile
from datetime import datetime, timezone
import mysql.connector
from mysql.connector import Error
import csv
import shutil
from ldap3 import Server, Connection, ALL, NTLM
import logging
from dateutil import parser

def unzip_files_in_folder(source_folder, destination_folder):
    for item in os.listdir(source_folder):
        if item.endswith('.zip'):
            zip_path = os.path.join(source_folder, item)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(destination_folder)
                print(f"Unzipped {item} into {destination_folder}")
            os.remove(zip_path)
            print(f"Deleted {item} after extraction.")

def connect_to_database(host, database, user, password):
    try:
        connection = mysql.connector.connect(host=host, database=database, user=user, password=password)
        if connection.is_connected():
            print("Successfully connected to the database.")
            return connection
    except Error as e:
        print("Error while connecting to MySQL:", e)
        return None

def create_table_if_not_exists(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS printer_logs (
            Printer VARCHAR(255),
            User VARCHAR(255),  -- Ensure this matches the column used in the insert statement
            Document VARCHAR(255),
            TotalPages INT,
            Department VARCHAR(255),
            PrintTime DATETIME
        );
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table checked/created successfully.")
    except Error as e:
        print("Error creating table in MySQL:", e)


def insert_data_to_database(connection, file_path):
    cursor = connection.cursor()
    insert_stmt = """
    INSERT INTO printer_logs (Printer, User, Document, TotalPages, Department, PrintTime)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    with open(file_path, mode='r', newline='', encoding='utf-8-sig') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        for row in csv_reader:
            try:
                
                print_time = parser.isoparse(row['PrintTime'])
                print_time_str = print_time.strftime('%Y-%m-%d %H:%M:%S')

              
                data_tuple = (
                    row['Printer'], row['User'], row['Document'], int(row['TotalPages']),
                    row.get('Department', None),  
                    print_time_str
                )
                cursor.execute(insert_stmt, data_tuple)
            except Exception as e:
                print(f"Error processing row: {row} with error: {e}")

    connection.commit()
    print(f"Data inserted from {file_path} into the database.")
    cursor.close()



def move_file_to_processed_folder(original_path, processed_folder):
    if not os.path.exists(processed_folder):
        os.makedirs(processed_folder)
    
    filename = os.path.basename(original_path)
    new_filename = f"{filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    processed_file_path = os.path.join(processed_folder, new_filename)
    
    shutil.move(original_path, processed_file_path)
    print(f"Moved {original_path} to {processed_file_path}")

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')


def connect_to_active_directory(server_address, username, password, domain):
    server = Server(server_address, get_info=ALL)

    # Attempt to establish a connection using simple binding
    try:
        full_username = f'{domain}\\{username}'
        connection = Connection(server, user=full_username, password=password)
        if not connection.bind():
            logging.error("Failed to bind to AD: %s", connection.result)
            return None
        logging.info("Successfully connected to Active Directory using simple binding.")
        return connection
    except Exception as e:
        logging.error("Error connecting to Active Directory: %s", e)
        return None

def search_active_directory(connection, search_base, search_filter, attributes):
    connection.search(search_base, search_filter, attributes=attributes)
    return connection.entries

def get_users_from_ad(connection):
    ous = [
        {"ou": "OU=Administration,OU=FMF End Users,DC=flour,DC=local", "department": "Administration"},
        {"ou": "OU=Atpack,OU=FMF End Users,DC=flour,DC=local", "department": "Atpack"},
        {"ou": "OU=Audit,OU=FMF End Users,DC=flour,DC=local", "department": "Audit"},
        {"ou": "OU=BCF,OU=FMF End Users,DC=flour,DC=local", "department": "BCF"},
        {"ou": "OU=Costing,OU=FMF End Users,DC=flour,DC=local", "department": "Costing"},
        {"ou": "OU=Credit Dept,OU=FMF End Users,DC=flour,DC=local", "department": "Credit"},
        {"ou": "OU=Customs,OU=FMF End Users,DC=flour,DC=local", "department": "Customs"},
        {"ou": "OU=Exports,OU=FMF End Users,DC=flour,DC=local", "department": "Exports"},
        {"ou": "OU=Finance,OU=FMF End Users,DC=flour,DC=local", "department": "Finance"},
        {"ou": "OU=FMF Warehouse,OU=FMF End Users,DC=flour,DC=local", "department": "FMF Warehouse"},
        {"ou": "OU=GL,OU=FMF End Users,DC=flour,DC=local", "department": "FMF"},
        {"ou": "OU=HPS,OU=FMF End Users,DC=flour,DC=local", "department": "HPS"},
        {"ou": "OU=HR,OU=FMF End Users,DC=flour,DC=local", "department": "HR"},
        {"ou": "OU=IT,OU=FMF End Users,DC=flour,DC=local", "department": "IT"},
        {"ou": "OU=Maintainence,OU=FMF End Users,DC=flour,DC=local", "department": "Maintainence"},
        {"ou": "OU=Managers,OU=FMF End Users,DC=flour,DC=local", "department": "Managers"},
        {"ou": "OU=Marketing,OU=FMF End Users,DC=flour,DC=local", "department": "Marketing"},
        {"ou": "OU=NDW Warehouse,OU=FMF End Users,DC=flour,DC=local", "department": "NDW Warehouse"},
        {"ou": "OU=Payments,OU=FMF End Users,DC=flour,DC=local", "department": "Payments"},
        {"ou": "OU=Printer Group,OU=FMF End Users,DC=flour,DC=local", "department": "Printer Group"},
        {"ou": "OU=Properties,OU=FMF End Users,DC=flour,DC=local", "department": "Properties"},
        {"ou": "OU=Purchasing,OU=FMF End Users,DC=flour,DC=local", "department": "Purchasing"},
        {"ou": "OU=QA,OU=FMF End Users,DC=flour,DC=local", "department": "QA"},
        {"ou": "OU=Sales,OU=FMF End Users,DC=flour,DC=local", "department": "Sales"},
        {"ou": "OU=Security,OU=FMF End Users,DC=flour,DC=local", "department": "Security"},
        {"ou": "OU=Snax,OU=FMF End Users,DC=flour,DC=local", "department": "Snax"},
        {"ou": "OU=Veisari,OU=FMF End Users,DC=flour,DC=local", "department": "Veisari"},
        # Add more OUs as needed
    ]
    search_filter = "(&(objectClass=user)(objectCategory=person))"
    attributes = ['samAccountName']

    user_data = []
    for ou in ous:
        users = search_active_directory(connection, ou["ou"], search_filter, attributes)
        for user in users:
            user_data.append({
                "User": user.samAccountName.value,
                "Department": ou["department"]
            })
    return user_data

def update_departments_with_ad_info(connection, ad_users):
    for user in ad_users:
        update_department(connection, user["User"], user["Department"])


def update_department(connection, username, department):
    try:
        cursor = connection.cursor()
        update_query = """
        UPDATE printer_logs
        SET Department = %s
        WHERE User = %s;
        """
        cursor.execute(update_query, (department, username))
        connection.commit()
    except Error as e:
        print(f"Error updating department for {username}: {e}")



# Main script execution
if __name__ == "__main__":
    # Path to the directory with CSV files
    csv_folder = r'C:\PrintLogs'
    processed_folder = r'C:\Users\sohaila\Desktop\Scripts\PrinterRecords\print-logs\processed'
    

    source_folder = os.path.dirname(os.path.abspath(__file__))
    unzip_files_in_folder(source_folder, source_folder)
    
    # Database credentials and details
    host = 'localhost'
    database = 'billsandrecords'
    user = 'root'
    password = 'root'


    ad_server = '192.168.22.4'  # Replace with your AD server's IP address or hostname
    ad_username = 'sohaila'  # Replace with your AD username
    ad_password = 'Password02'  # Replace with your AD password
    ad_domain = 'FLOUR' 

    # Connect to the database
    connection = connect_to_database(host, database, user, password)

    if connection:
        create_table_if_not_exists(connection)

        for file in os.listdir(csv_folder):
            if file.endswith('.csv'):
                file_path = os.path.join(csv_folder, file)
                insert_data_to_database(connection, file_path)
                processed_file_path = move_file_to_processed_folder(file_path, processed_folder)
                print(f"Moved processed file to: {processed_file_path}")

        if not connection.is_connected():
            connection = connect_to_database(host, database, user, password)

        ad_connection = connect_to_active_directory(ad_server, ad_username, ad_password, ad_domain)
        if ad_connection:
            print("Connected to AD.")
            ad_users = get_users_from_ad(ad_connection)
            update_departments_with_ad_info(connection, ad_users)
            ad_connection.unbind()
        else:
            print("Failed to connect to AD.")
        
        # Close the database connection after all updates are done
        connection.close()
    else:
        print("Failed to connect to the database.")


