import os
import sys
import json
import time
import logging
import requests
import pyodbc
from datetime import datetime
from decimal import Decimal

# Custom JSON encoder to handle Decimal objects
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


# Setup logging with better formatting
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Simplified format for command prompt
    handlers=[
        logging.FileHandler('sync.log', mode='w'),  # Overwrite log file each time
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = 'config.json'


def print_header():
    """Print a nice header for the application"""
    print("\n" + "=" * 70)
    print("              🚀 OMEGA DATABASE SYNC TOOL 🚀")
    print("=" * 70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")


def print_progress_bar(current, total, prefix='Progress', bar_length=40):
    """Print a progress bar"""
    percent = float(current) * 100 / total
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    print(f'\r{prefix}: |{bar}| {percent:.1f}% ({current}/{total})', end='', flush=True)
    if current == total:
        print()  # New line when complete


def load_config():
    """Load configuration from config.json file"""
    try:
        print("📋 Loading configuration file...")
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        print("✅ Configuration loaded successfully\n")
        return config
    except FileNotFoundError:
        print(f"❌ ERROR: Configuration file '{CONFIG_FILE}' not found!")
        print("   Please ensure config.json exists in the same folder.")
        input("\nPress Enter to exit...")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"❌ ERROR: Invalid JSON format in '{CONFIG_FILE}'!")
        print("   Please check your configuration file syntax.")
        input("\nPress Enter to exit...")
        sys.exit(1)


def connect_to_database(config):
    """Connect to SQL Anywhere database using ODBC"""
    try:
        print("🔌 Connecting to database...")
        dsn = config['database']['dsn']
        username = config['database']['username']
        password = config['database']['password']

        print(f"   → DSN: {dsn}")
        print(f"   → User: {username}")
        
        conn_str = f"DSN={dsn};UID={username};PWD={password}"
        conn = pyodbc.connect(conn_str)

        print("✅ Database connection successful!\n")
        return conn
    except pyodbc.Error as e:
        print(f"❌ Database connection failed!")
        print(f"   Error: {e}")
        print("   Please check your database configuration and ensure:")
        print("   • Database server is running")
        print("   • DSN is configured correctly")
        print("   • Username and password are correct")
        input("\nPress Enter to exit...")
        sys.exit(1)


def execute_query(conn, query):
    """Execute SQL query and return results as a list of dictionaries"""
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        cursor.close()
        return results
    except pyodbc.Error as e:
        print(f"❌ Query execution failed: {e}")
        return []


def fetch_data(conn):
    """Fetch data from all required tables"""
    print("📊 FETCHING DATA FROM DATABASE")
    print("-" * 50)
    
    data = {}
    tables = [
        ("products", "SELECT code, name, product, brand, unit, taxcode, defect, company FROM acc_product"),
        ("batches", "SELECT productcode, cost, salesprice, bmrp, barcode, secondprice, thirdprice FROM acc_productbatch"),
        ("customers", "SELECT code, name, super_code, address, phone, phone2 FROM acc_master WHERE super_code = 'DEBTO'"),
        ("users", "SELECT id, pass, role FROM acc_users")
    ]
    
    total_records = 0
    
    for i, (table_name, query) in enumerate(tables, 1):
        print(f"{i}. Fetching {table_name}...", end=" ", flush=True)
        
        if table_name == "users":
            results = execute_query(conn, query)
            # Transform 'pass' to 'pass_field' for Django compatibility
            for user in results:
                if 'pass' in user:
                    user['pass_field'] = user.pop('pass')
            data["users"] = results
        elif table_name == "customers":
            results = execute_query(conn, query)
            data["masters"] = results  # Django expects 'masters' key
        else:
            results = execute_query(conn, query)
            data[table_name] = results
        
        print(f"✅ {len(results):,} records")
        total_records += len(results)
    
    print("-" * 50)
    print(f"📈 TOTAL RECORDS TO SYNC: {total_records:,}")
    print()
    
    return data


def clear_and_upload_data(data, config):
    """Clear existing data and upload new data to web API"""
    try:
        api_base_url = config['api']['url']
        api_key = config['api']['key']
        
        print(f"🌐 API Server: {api_base_url}")
        print()

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

        tables = {
            "products": "/api/sync/products",
            "batches": "/api/sync/productbatches", 
            "masters": "/api/sync/masters",
            "users": "/api/sync/users"
        }

        clear_endpoints = {
            "products": "/api/sync/products/clear",
            "batches": "/api/sync/productbatches/clear",
            "masters": "/api/sync/masters/clear", 
            "users": "/api/sync/users/clear"
        }

        # Step 1: Clear existing data
        print("🗑️  CLEARING EXISTING DATA")
        print("-" * 50)
        
        for i, (table_name, clear_endpoint) in enumerate(clear_endpoints.items(), 1):
            if table_name in data:
                print(f"{i}. Clearing {table_name}...", end=" ", flush=True)
                clear_url = f"{api_base_url}{clear_endpoint}"
                
                try:
                    clear_response = requests.delete(clear_url, headers=headers, timeout=30)
                    if clear_response.status_code != 200:
                        clear_response = requests.post(clear_url, headers=headers, timeout=30)
                    
                    if clear_response.status_code == 200:
                        print("✅ Success")
                    else:
                        print(f"❌ Failed ({clear_response.status_code})")
                        return False
                except Exception as e:
                    print(f"❌ Error: {str(e)}")
                    return False

        print()
        
        # Step 2: Upload new data
        print("📤 UPLOADING NEW DATA")
        print("-" * 50)
        
        def chunk_data(data_list, chunk_size=500):  # Increased chunk size
            for i in range(0, len(data_list), chunk_size):
                yield data_list[i:i + chunk_size]

        table_names = ["products", "batches", "masters", "users"]
        
        for table_index, table_name in enumerate(table_names, 1):
            if table_name in data:
                table_data = data[table_name]
                if not table_data:
                    print(f"{table_index}. {table_name.title()}: No data to upload")
                    continue
                    
                print(f"{table_index}. Uploading {len(table_data):,} {table_name}...")
                
                post_url = f"{api_base_url}{tables[table_name]}"
                chunks = list(chunk_data(table_data, chunk_size=500))
                
                for chunk_index, chunk in enumerate(chunks, 1):
                    print_progress_bar(chunk_index - 1, len(chunks), f"   Chunk {chunk_index}/{len(chunks)}")
                    
                    success = False
                    for retry in range(3):  # 3 retries
                        try:
                            response = requests.post(
                                post_url,
                                data=json.dumps(chunk, cls=DecimalEncoder),
                                headers=headers,
                                timeout=180  # 3 minutes timeout
                            )
                            if response.status_code == 200:
                                success = True
                                break
                            else:
                                print(f"\n   ⚠️  Retry {retry + 1}/3 (Status: {response.status_code})")
                                time.sleep(2)
                        except Exception as e:
                            print(f"\n   ⚠️  Retry {retry + 1}/3 (Error: {str(e)})")
                            time.sleep(2)
                    
                    print_progress_bar(chunk_index, len(chunks), f"   Chunk {chunk_index}/{len(chunks)}")
                    
                    if not success:
                        print(f"\n❌ Failed to upload {table_name} after 3 attempts")
                        return False
                
                print(f"   ✅ {table_name.title()} uploaded successfully!")
                print()

        return True

    except Exception as e:
        print(f"❌ API Error: {str(e)}")
        return False


def main():
    """Main function to run the sync process"""
    try:
        print_header()
        
        # Load configuration
        config = load_config()
        
        # Connect to database  
        conn = connect_to_database(config)
        
        # Fetch data
        data = fetch_data(conn)
        
        # Upload data
        success = clear_and_upload_data(data, config)
        
        # Close connection
        conn.close()
        print("🔌 Database connection closed")
        print()

        if success:
            print("=" * 70)
            print("           🎉 SYNC COMPLETED SUCCESSFULLY! 🎉")
            print("=" * 70)
            print("✅ All data has been synchronized to the web application")
            print(f"✅ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            print()
            print("This window will close automatically in 5 seconds...")
            
            for i in range(5, 0, -1):
                print(f"Closing in {i}...", end="\r", flush=True)
                time.sleep(1)
            sys.exit(0)
        else:
            print("=" * 70)
            print("            ❌ SYNC FAILED! ❌")
            print("=" * 70)
            print("Please check the errors above and try again.")
            print("Common solutions:")
            print("• Check internet connection")
            print("• Verify API server is running")
            print("• Check configuration settings")
            print("=" * 70)
            print()
            input("Press Enter to close...")
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n\n⚠️  Sync cancelled by user")
        input("Press Enter to close...")
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 70)
        print("            💥 UNEXPECTED ERROR! 💥")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print("\nPlease contact technical support with this error message.")
        print("=" * 70)
        input("\nPress Enter to close...")
        sys.exit(1)


if __name__ == "__main__":
    main()