#!/usr/bin/env python3
"""
Diagnostics script for troubleshooting sync issues
Run this before running sync.py to test connectivity and API endpoints
"""

import os
import sys
import json
import requests
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('diagnostics.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CONFIG_FILE = 'config.json'


def load_config():
    """Load configuration from config.json file"""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file {CONFIG_FILE} not found")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in {CONFIG_FILE}")
        sys.exit(1)


def test_api_endpoints(config):
    """Test API endpoints to ensure they are accessible and working correctly"""
    try:
        api_base_url = config['api']['url']
        api_key = config['api']['key']

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {api_key}'
        }

        logger.info(f"Testing API connection to {api_base_url}...")

        # List of endpoints to test
        endpoints = [
            ("/api/sync/products/clear", "DELETE", "Clear products"),
            ("/api/sync/products", "POST", "Sync products"),
            ("/api/sync/productbatches/clear", "DELETE", "Clear product batches"),
            ("/api/sync/productbatches", "POST", "Sync product batches"),
            ("/api/sync/masters/clear", "DELETE", "Clear masters"),
            ("/api/sync/masters", "POST", "Sync masters"),
            ("/api/sync/users/clear", "DELETE", "Clear users"),
            ("/api/sync/users", "POST", "Sync users")
        ]

        results = []

        # Test each endpoint with a simple request
        for endpoint, method, description in endpoints:
            try:
                url = f"{api_base_url}{endpoint}"
                logger.info(f"Testing {method} {url} ({description})...")

                if method == "DELETE":
                    # For DELETE endpoints, we just test if they respond
                    # We don't actually want to delete data in diagnostics mode
                    response = requests.options(
                        url, headers=headers, timeout=5)
                    # Just check if the server is responsive
                    status = "AVAILABLE" if response.status_code < 500 else "ERROR"
                elif method == "POST":
                    # For POST endpoints, we just test if they respond to a GET (will likely return 405 Method Not Allowed)
                    response = requests.options(
                        url, headers=headers, timeout=5)
                    # Just check if the server is responsive
                    status = "AVAILABLE" if response.status_code < 500 else "ERROR"

                # Check for Django debug page
                is_debug = False
                if hasattr(response, 'text') and "<code>DEBUG = True</code>" in response.text:
                    is_debug = True

                results.append({
                    "endpoint": endpoint,
                    "method": method,
                    "status_code": response.status_code,
                    "status": status,
                    "debug_mode": is_debug
                })

                logger.info(
                    f"  - Status: {status} (Code: {response.status_code})")
                if is_debug:
                    logger.warning(
                        f"  - DJANGO DEBUG MODE DETECTED on {endpoint}")

            except requests.exceptions.Timeout:
                logger.error(f"  - Timeout accessing {url}")
                results.append({
                    "endpoint": endpoint,
                    "method": method,
                    "status": "TIMEOUT",
                    "debug_mode": False
                })
            except requests.exceptions.ConnectionError:
                logger.error(f"  - Connection error accessing {url}")
                results.append({
                    "endpoint": endpoint,
                    "method": method,
                    "status": "CONNECTION ERROR",
                    "debug_mode": False
                })
            except Exception as e:
                logger.error(f"  - Unexpected error: {str(e)}")
                results.append({
                    "endpoint": endpoint,
                    "method": method,
                    "status": f"ERROR: {str(e)}",
                    "debug_mode": False
                })

        # Summary
        logger.info("\n===== DIAGNOSTICS SUMMARY =====")
        logger.info(f"API Base URL: {api_base_url}")

        # Count issues
        errors = [r for r in results if r["status"] != "AVAILABLE"]
        debug_mode = any(r["debug_mode"] for r in results)

        if errors:
            logger.warning(f"Found {len(errors)} endpoint issues:")
            for error in errors:
                logger.warning(
                    f"  - {error['method']} {error['endpoint']}: {error['status']}")
        else:
            logger.info("All API endpoints appear to be available.")

        if debug_mode:
            logger.warning("DJANGO DEBUG MODE IS ENABLED on the server!")
            logger.warning(
                "This can cause HTML error pages instead of proper JSON responses.")
            logger.warning(
                "Recommendation: Set DEBUG = False in your Django settings.py file.")

        logger.info("==============================\n")

        return len(errors) == 0

    except Exception as e:
        logger.error(f"API diagnostics failed: {e}")
        return False


def main():
    """Main function to run diagnostics"""
    try:
        logger.info("Starting API diagnostics...")

        # Load configuration
        config = load_config()

        # Test API endpoints
        success = test_api_endpoints(config)

        if success:
            logger.info(
                "Diagnostics completed successfully. All endpoints seem to be working.")
        else:
            logger.warning(
                "Diagnostics completed with warnings or errors. Please review the log.")

        logger.info("Press Enter to exit...")
        input()
        return 0 if success else 1

    except Exception as e:
        logger.error(f"Unexpected error during diagnostics: {e}")
        logger.info("Press Enter to exit...")
        input()
        return 1


if __name__ == "__main__":
    sys.exit(main())
