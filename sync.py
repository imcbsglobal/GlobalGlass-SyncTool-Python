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
    """Clear existing data and upload new data to web API using improved chunking"""
    try:
        api_base_url = config['api']['url']
        api_key = config['api']['key']
        
        print(f"🌐 API Server: {api_base_url}")
        print()

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

        # Define endpoints - using separate clear and chunk endpoints
        endpoints = {
            "products": {
                "clear": "/api/clear/products",
                "chunk": "/api/sync/products/chunk"
            },
            "batches": {
                "clear": "/api/clear/productbatches", 
                "chunk": "/api/sync/productbatches/chunk"
            },
            "masters": {
                "clear": "/api/clear/masters",
                "chunk": "/api/sync/masters/chunk"
            },
            "users": {
                "clear": "/api/clear/users",
                "chunk": "/api/sync/users/chunk"
            }
        }
        
        def chunk_data(data_list, chunk_size=500):
            for i in range(0, len(data_list), chunk_size):
                yield data_list[i:i + chunk_size]

        def make_request_with_retry(url, data=None, method='POST', retries=3):
            """Make HTTP request with retry logic"""
            for retry in range(retries):
                try:
                    if method == 'DELETE':
                        response = requests.delete(url, headers=headers, timeout=60)
                    else:
                        response = requests.post(
                            url,
                            data=json.dumps(data, cls=DecimalEncoder) if data else None,
                            headers=headers,
                            timeout=180
                        )
                    
                    if response.status_code in [200, 204]:
                        return True, response
                    else:
                        print(f"\n   ⚠️  Retry {retry + 1}/{retries} (Status: {response.status_code})")
                        if retry < retries - 1:
                            time.sleep(2)
                        
                except Exception as e:
                    print(f"\n   ⚠️  Retry {retry + 1}/{retries} (Error: {str(e)})")
                    if retry < retries - 1:
                        time.sleep(2)
            
            return False, None

        table_names = ["products", "batches", "masters", "users"]
        
        # Step 1: Clear all tables first
        print("🗑️  CLEARING EXISTING DATA")
        print("-" * 50)
        
        for table_index, table_name in enumerate(table_names, 1):
            if table_name in data and data[table_name]:
                print(f"{table_index}. Clearing {table_name}...", end=" ", flush=True)
                
                clear_url = f"{api_base_url}{endpoints[table_name]['clear']}"
                success, response = make_request_with_retry(clear_url, method='DELETE')
                
                if success:
                    print("✅ Cleared")
                else:
                    print(f"❌ Failed to clear {table_name}")
                    return False
        
        print()
        
        # Step 2: Upload all data in chunks
        print("📤 UPLOADING NEW DATA (CHUNKED)")
        print("-" * 50)
        
        for table_index, table_name in enumerate(table_names, 1):
            if table_name in data:
                table_data = data[table_name]
                if not table_data:
                    print(f"{table_index}. {table_name.title()}: No data to upload")
                    continue
                    
                print(f"{table_index}. Uploading {len(table_data):,} {table_name}...")
                
                chunk_url = f"{api_base_url}{endpoints[table_name]['chunk']}"
                chunks = list(chunk_data(table_data, chunk_size=500))
                
                for chunk_index, chunk in enumerate(chunks, 1):
                    print_progress_bar(chunk_index - 1, len(chunks), f"   Chunk {chunk_index}/{len(chunks)}")
                    
                    success, response = make_request_with_retry(chunk_url, chunk)
                    
                    print_progress_bar(chunk_index, len(chunks), f"   Chunk {chunk_index}/{len(chunks)}")
                    
                    if not success:
                        print(f"\n❌ Failed to upload chunk {chunk_index} of {table_name}")
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