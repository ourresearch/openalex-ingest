#!/usr/bin/env python3
"""
DataCite Providers API Data Retrieval Script

This script retrieves all provider data from the DataCite API.
The API uses pagination, so this script handles multiple requests
to collect all available provider records.

Create the JSON file then ingest it into databricks by uploading the JSON file via the Data Ingestion UI.
"""

import requests
import json
import time
from typing import List, Dict, Any, Optional


class DataCiteProvidersAPI:
    """Class to handle DataCite Providers API requests."""

    def __init__(self, base_url: str = "https://api.datacite.org"):
        self.base_url = base_url
        self.providers_endpoint = f"{base_url}/providers"
        self.session = requests.Session()

        # Set headers for better API interaction
        self.session.headers.update({
            'User-Agent': 'DataCite-Providers-Scraper/1.0',
            'Accept': 'application/json'
        })

    def get_providers_page(self, page: int = 1, page_size: int = 1000) -> Optional[Dict[str, Any]]:
        """
        Retrieve a single page of providers data.

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
            response = self.session.get(self.providers_endpoint, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            return None

    def get_all_providers(self, delay: float = 0.1) -> List[Dict[str, Any]]:
        """
        Retrieve all providers data by paginating through all pages.

        Args:
            delay: Delay between requests in seconds (to be respectful)

        Returns:
            List of all provider records
        """
        all_providers = []
        page = 1

        print("Starting to fetch DataCite providers data...")

        while True:
            print(f"Fetching page {page}...")

            data = self.get_providers_page(page)
            if not data:
                print(f"Failed to fetch page {page}. Stopping.")
                break

            # Extract providers from the response
            providers = data.get('data', [])
            if not providers:
                print(f"No more providers found on page {page}. Finished.")
                break

            all_providers.extend(providers)

            # Check pagination info
            meta = data.get('meta', {})
            total_pages = meta.get('totalPages', 1)
            total_count = meta.get('total', len(all_providers))

            print(f"Page {page}/{total_pages} - Got {len(providers)} providers "
                  f"(Total so far: {len(all_providers)}/{total_count})")

            # Check if we've reached the last page
            if page >= total_pages:
                print("Reached last page. Finished.")
                break

            page += 1

            # Be respectful to the API
            if delay > 0:
                time.sleep(delay)

        print(f"Successfully retrieved {len(all_providers)} total providers.")
        return all_providers

    def save_to_file(self, providers: List[Dict[str, Any]], filename: str = "datacite_providers.json"):
        """
        Save providers data to a JSON file.

        Args:
            providers: List of provider records
            filename: Output filename
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(providers, f, indent=2, ensure_ascii=False)
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving to file: {e}")


def main():
    """Main function to run the data collection."""

    # Initialize the API client
    api = DataCiteProvidersAPI()

    # Get all providers data
    providers = api.get_all_providers(delay=0.1)  # 100ms delay between requests

    if providers:
        # Save to JSON file
        api.save_to_file(providers, "datacite_providers.json")

        # Print some summary statistics
        print(f"\nSummary:")
        print(f"Total providers retrieved: {len(providers)}")

        if providers:
            print(f"First provider ID: {providers[0].get('id', 'N/A')}")
            print(f"Sample provider attributes: {list(providers[0].get('attributes', {}).keys())}")

            # Count providers by region/country if available
            countries = {}
            regions = {}
            for provider in providers:
                attrs = provider.get('attributes', {})
                country = attrs.get('country')
                region = attrs.get('region')

                if country:
                    countries[country] = countries.get(country, 0) + 1
                if region:
                    regions[region] = regions.get(region, 0) + 1

            if countries:
                print(f"Number of different countries: {len(countries)}")
                print(f"Top 5 countries by provider count:")
                for country, count in sorted(countries.items(), key=lambda x: x[1], reverse=True)[:5]:
                    print(f"  {country}: {count} providers")

            if regions:
                print(f"Number of different regions: {len(regions)}")
                print(f"Providers by region:")
                for region, count in sorted(regions.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {region}: {count} providers")

            # Count by provider type if available
            types = {}
            for provider in providers:
                provider_type = provider.get('attributes', {}).get('memberType')
                if provider_type:
                    types[provider_type] = types.get(provider_type, 0) + 1

            if types:
                print(f"Providers by member type:")
                for ptype, count in sorted(types.items(), key=lambda x: x[1], reverse=True):
                    print(f"  {ptype}: {count} providers")

    else:
        print("No providers data retrieved.")


if __name__ == "__main__":
    main()