"""
MTA Data API Client for fetching transit data from NYC Open Data Portal
"""
import logging
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys
sys.path.append(str(__file__).rsplit('\\', 3)[0])
from config.settings import MTA_API_CONFIG

logger = logging.getLogger(__name__)


class MTADataClient:
    """
    Client for fetching MTA transit data from NYC Open Data APIs.
    
    Implements rate limiting, retry logic, and error handling.
    """
    
    # NYC Open Data API Endpoints
    ENDPOINTS = {
        'subway_stations': 'https://data.ny.gov/resource/39hk-dx4f.json',
        'subway_ridership': 'https://data.ny.gov/resource/wujg-7c2s.json',
        'hourly_ridership': 'https://data.ny.gov/resource/v3ua-egxu.json',
        'performance_subway': 'https://data.ny.gov/resource/y27x-cket.json',
        'subway_delays': 'https://data.ny.gov/resource/7kag-ynmv.json',
        'turnstile_usage': 'https://data.ny.gov/resource/qzve-kjga.json',
    }
    
    def __init__(self, app_token: str = None):
        """
        Initialize the MTA Data Client.
        
        Args:
            app_token: NYC Open Data app token for higher rate limits
        """
        self.app_token = app_token or MTA_API_CONFIG.get('app_token')
        self.timeout = MTA_API_CONFIG.get('timeout', 30)
        self.max_retries = MTA_API_CONFIG.get('max_retries', 3)
        self.batch_size = MTA_API_CONFIG.get('batch_size', 50000)
        
        # Set up session with retry logic
        self.session = self._create_session()
        
        logger.info("MTA Data Client initialized")
    
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry configuration."""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        
        # Set default headers
        headers = {
            'Accept': 'application/json',
            'User-Agent': 'MTA-Transit-Dashboard/1.0'
        }
        
        if self.app_token:
            headers['X-App-Token'] = self.app_token
        
        session.headers.update(headers)
        
        return session
    
    def _make_request(self, url: str, params: Dict = None) -> Optional[List[Dict]]:
        """
        Make an API request with error handling.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            JSON response data or None if failed
        """
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"Fetched {len(data)} records from {url}")
            return data
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching data: {e}")
            if response.status_code == 429:
                logger.warning("Rate limit hit, waiting before retry...")
                time.sleep(60)
            return None
            
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error: {e}")
            return None
            
        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout: {e}")
            return None
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error fetching data: {e}")
            return None
    
    def fetch_with_pagination(self, url: str, params: Dict = None, 
                              limit: int = 50000, max_records: int = None) -> List[Dict]:
        """
        Fetch data with pagination support.
        
        Args:
            url: API endpoint URL
            params: Base query parameters
            limit: Records per request
            max_records: Maximum total records to fetch
            
        Returns:
            Combined list of all fetched records
        """
        all_data = []
        offset = 0
        
        if params is None:
            params = {}
        
        while True:
            page_params = {
                **params,
                '$limit': limit,
                '$offset': offset
            }
            
            data = self._make_request(url, page_params)
            
            if not data:
                break
            
            all_data.extend(data)
            logger.info(f"Fetched {len(all_data)} records so far...")
            
            if len(data) < limit:
                # No more data available
                break
            
            if max_records and len(all_data) >= max_records:
                all_data = all_data[:max_records]
                break
            
            offset += limit
            time.sleep(0.5)  # Rate limiting
        
        logger.info(f"Total records fetched: {len(all_data)}")
        return all_data
    
    def get_subway_stations(self) -> List[Dict]:
        """
        Fetch subway station data.
        
        Returns:
            List of station records with location and metadata
        """
        logger.info("Fetching subway station data...")
        url = self.ENDPOINTS['subway_stations']
        
        data = self.fetch_with_pagination(url, limit=1000)
        
        if data:
            logger.info(f"Retrieved {len(data)} subway stations")
        
        return data or []
    
    def get_ridership_data(self, start_date: str = None, end_date: str = None,
                          max_records: int = 100000) -> List[Dict]:
        """
        Fetch subway ridership data.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            max_records: Maximum records to fetch
            
        Returns:
            List of ridership records
        """
        logger.info("Fetching ridership data...")
        url = self.ENDPOINTS['subway_ridership']
        
        params = {}
        
        if start_date:
            params['$where'] = f"transit_timestamp >= '{start_date}'"
            if end_date:
                params['$where'] += f" AND transit_timestamp <= '{end_date}'"
        
        data = self.fetch_with_pagination(url, params=params, max_records=max_records)
        
        if data:
            logger.info(f"Retrieved {len(data)} ridership records")
        
        return data or []
    
    def get_turnstile_data(self, start_date: str = None, 
                           max_records: int = 100000) -> List[Dict]:
        """
        Fetch turnstile usage data.
        
        Args:
            start_date: Start date filter
            max_records: Maximum records to fetch
            
        Returns:
            List of turnstile records
        """
        logger.info("Fetching turnstile data...")
        url = self.ENDPOINTS['turnstile_usage']
        
        params = {}
        if start_date:
            params['$where'] = f"date >= '{start_date}'"
        
        data = self.fetch_with_pagination(url, params=params, max_records=max_records)
        
        return data or []
    
    def get_performance_data(self, year: int = None, 
                             max_records: int = 50000) -> List[Dict]:
        """
        Fetch subway performance metrics.
        
        Args:
            year: Year to filter by
            max_records: Maximum records to fetch
            
        Returns:
            List of performance records
        """
        logger.info("Fetching performance data...")
        url = self.ENDPOINTS['performance_subway']
        
        params = {}
        if year:
            params['$where'] = f"period_year = {year}"
        
        data = self.fetch_with_pagination(url, params=params, max_records=max_records)
        
        return data or []
    
    def get_delay_data(self, start_date: str = None,
                       max_records: int = 50000) -> List[Dict]:
        """
        Fetch delay incident data.
        
        Args:
            start_date: Start date filter
            max_records: Maximum records to fetch
            
        Returns:
            List of delay records
        """
        logger.info("Fetching delay data...")
        url = self.ENDPOINTS['subway_delays']
        
        params = {}
        if start_date:
            params['$where'] = f"start_date >= '{start_date}'"
        
        data = self.fetch_with_pagination(url, params=params, max_records=max_records)
        
        return data or []
    
    def get_api_metadata(self, endpoint_name: str) -> Optional[Dict]:
        """
        Get metadata about an API endpoint.
        
        Args:
            endpoint_name: Name of the endpoint
            
        Returns:
            Endpoint metadata if available
        """
        if endpoint_name not in self.ENDPOINTS:
            logger.error(f"Unknown endpoint: {endpoint_name}")
            return None
        
        url = self.ENDPOINTS[endpoint_name]
        base_url = url.rsplit('/', 1)[0]
        
        # Try to fetch dataset metadata
        try:
            response = self.session.get(
                f"{base_url}.json",
                params={'$limit': 0},
                timeout=10
            )
            
            # Get row count from headers
            return {
                'endpoint': endpoint_name,
                'url': url,
                'available': response.status_code == 200
            }
        except Exception as e:
            logger.warning(f"Could not fetch metadata for {endpoint_name}: {e}")
            return None
    
    def test_endpoints(self) -> Dict[str, bool]:
        """
        Test connectivity to all API endpoints.
        
        Returns:
            Dictionary mapping endpoint names to availability status
        """
        results = {}
        
        for name, url in self.ENDPOINTS.items():
            try:
                response = self.session.get(url, params={'$limit': 1}, timeout=10)
                results[name] = response.status_code == 200
                logger.info(f"Endpoint '{name}': {'available' if results[name] else 'unavailable'}")
            except Exception as e:
                results[name] = False
                logger.warning(f"Endpoint '{name}' test failed: {e}")
        
        return results
    
    def close(self):
        """Close the session."""
        self.session.close()
        logger.info("MTA Data Client session closed")


def get_mta_client() -> MTADataClient:
    """Factory function to create MTA client instance."""
    return MTADataClient()


if __name__ == "__main__":
    # Test the API client
    logging.basicConfig(level=logging.INFO)
    
    client = get_mta_client()
    
    print("\n=== Testing MTA Data API Endpoints ===\n")
    results = client.test_endpoints()
    
    print("\nEndpoint Availability:")
    for endpoint, available in results.items():
        status = "✓ Available" if available else "✗ Unavailable"
        print(f"  {endpoint}: {status}")
    
    # Try fetching sample data
    print("\n=== Fetching Sample Data ===\n")
    
    stations = client.get_subway_stations()
    if stations:
        print(f"Sample station: {stations[0].get('stop_name', 'N/A')}")
    
    client.close()
