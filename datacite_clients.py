#!/usr/bin/env python3
"""
DataCite Clients API Data Retrieval Script

This script retrieves all client data from the DataCite API.
The API uses pagination, so this script handles multiple requests
to collect all available client records.

Create the JSON file then ingest it into databricks by uploading the JSON file via the Data Ingestion UI.
"""

import requests
import json
import time
from typing import List, Dict, Any, Optional


class DataCiteClientsAPI:
    """Class to handle DataCite Clients API requests."""

    def __init__(self, base_url: str = "https://api.datacite.org"):
        self.base_url = base_url
        self.clients_endpoint = f"{base_url}/clients"
        self.session = requests.Session()

        # Set headers for better API interaction
        self.session.headers.update({
            'User-Agent': 'DataCite-Clients-Scraper/1.0',
            'Accept': 'application/json'
        })

    def get_clients_page(self, page: int = 1, page_size: int = 1000) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single page of clients data.

        Args:
            page: Page number (1-based)
            page_size: Number of records per page (max 1000)

        Returns:
            JSON response as dictionary, or None if request fails
        """
        params = {
            'page[number]': page,
            'page[size]': min(page_size, 1000)  # API max is 1000
        }

        try:
            response = self.session.get(self.clients_endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            return None

    def get_all_clients(self, delay: float = 0.1) -> List[Dict[str, Any]]:
        """
        Retrieve all clients data by paginating through all pages.

        Args:
            delay: Delay between requests in seconds (to be respectful)

        Returns:
            List of all client records
        """
        all_clients = []
        page = 1

        print("Starting to fetch DataCite clients data...")

        while True:
            print(f"Fetching page {page}...")

            data = self.get_clients_page(page)
            if not data:
                print(f"Failed to fetch page {page}. Stopping.")
                break

            # Extract clients from the response
            clients = data.get('data', [])
            if not clients:
                print(f"No more clients found on page {page}. Finished.")
                break

            all_clients.extend(clients)

            # Check pagination info
            meta = data.get('meta', {})
            total_pages = meta.get('totalPages', 1)
            total_count = meta.get('total', len(all_clients))

            print(f"Page {page}/{total_pages} - Got {len(clients)} clients "
                  f"(Total so far: {len(all_clients)}/{total_count})")

            # Check if we've reached the last page
            if page >= total_pages:
                print("Reached last page. Finished.")
                break

            page += 1

            # Be respectful to the API
            if delay > 0:
                time.sleep(delay)

        print(f"Successfully retrieved {len(all_clients)} total clients.")
        return all_clients

    def save_to_file(self, clients: List[Dict[str, Any]], filename: str = "datacite_clients.json"):
        """
        Save clients data to a JSON file.

        Args:
            clients: List of client records
            filename: Output filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(clients, f, indent=2, ensure_ascii=False)
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to file: {e}")


def main():
    """Main function to run the data collection."""

    # Initialize the API client
    api = DataCiteClientsAPI()

    # Get all clients data
    clients = api.get_all_clients(delay=0.1)  # 100ms delay between requests

    if clients:
        # Save to JSON file
        api.save_to_file(clients, "datacite_clients.json")

        # Print some summary statistics
        print(f"\nSummary:")
        print(f"Total clients retrieved: {len(clients)}")

        if clients:
            print(f"First client ID: {clients[0].get('id', 'N/A')}")
            print(f"Sample client attributes: {list(clients[0].get('attributes', {}).keys())}")

            # Count clients by provider if available
            providers = {}
            for client in clients:
                provider_id = client.get('relationships', {}).get('provider', {}).get('data', {}).get('id')
                if provider_id:
                    providers[provider_id] = providers.get(provider_id, 0) + 1

            if providers:
                print(f"Number of different providers: {len(providers)}")
                print(f"Top 5 providers by client count:")
                for provider, count in sorted(providers.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {provider}: {count} clients")
    else:
        print("No clients data retrieved.")


if __name__ == "__main__":
    main()