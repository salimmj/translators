"""CrossRef Translator for interacting with the CrossRef API."""

from typing import Optional, Dict, Any, List
import aiohttp
from datetime import datetime
import asyncio
from ratelimit import limits, sleep_and_retry

class CrossRefTranslator:
    """Translator for interacting with the CrossRef API."""
    
    BASE_URL = "https://api.crossref.org/works"
    
    def __init__(
        self,
        http_client: Optional[aiohttp.ClientSession] = None,
        mailto: Optional[str] = None,
        max_retries: int = 3
    ):
        """Initialize the CrossRef translator.
        
        Args:
            http_client: Optional HTTP client for making requests
            mailto: Email address for polite pool access
            max_retries: Maximum number of retries for failed requests
        """
        self.http_client = http_client
        self.mailto = mailto
        self.max_retries = max_retries
        self._rate_limit_remaining = None
        self._rate_limit_reset = None
    
    @sleep_and_retry
    @limits(calls=50, period=1)  # 50 requests per second max
    async def query_doi(self, doi: str) -> Dict[str, Any]:
        """Query CrossRef for metadata about a DOI.
        
        Args:
            doi: The DOI to query
            
        Returns:
            Dict containing the metadata for the DOI
            
        Raises:
            ValueError: If the DOI is invalid
            CrossRefAPIError: If the API request fails
            RateLimitError: If we've exceeded the rate limit
        """
        if not doi:
            raise ValueError("DOI cannot be empty")
        
        if not self.http_client:
            self.http_client = aiohttp.ClientSession()
        
        headers = {
            'User-Agent': f'Bibli/1.0 (mailto:{self.mailto or "support@bibli.com"})'
        }
        
        url = f"{self.BASE_URL}/{doi}"
        
        for attempt in range(self.max_retries):
            try:
                response = await self.http_client.get(url, headers=headers)
                
                # Handle rate limiting
                self._update_rate_limits(response)
                
                if response.status == 429:
                    reset_time = self._rate_limit_reset or "unknown"
                    raise RateLimitError(f"Rate limit exceeded. Reset at {reset_time}")
                
                if response.status == 404:
                    raise DOINotFoundError(f"DOI not found: {doi}")
                
                if response.status != 200:
                    raise CrossRefAPIError(
                        f"CrossRef API error: {response.status}"
                    )
                
                data = await response.json()
                return self._normalize_metadata(data['message'])
                
            except aiohttp.ClientError as e:
                if attempt == self.max_retries - 1:
                    raise CrossRefAPIError(f"Network error: {str(e)}")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
    
    def _normalize_metadata(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize CrossRef metadata to our standard format.
        
        Args:
            data: Raw metadata from CrossRef
            
        Returns:
            Dict with normalized metadata
        """
        # Helper function to safely get first item from a list
        def get_first(lst, default=None):
            return lst[0] if lst and len(lst) > 0 else default
        
        metadata = {
            'title': get_first(data.get('title', [])),
            'authors': [],
            'doi': data.get('DOI'),
            'url': data.get('URL'),
            'journal': get_first(data.get('container-title', [])),
            'issn': get_first(data.get('ISSN', [])),
            'issue': data.get('issue'),
            'volume': data.get('volume'),
            'year': None,
            'publisher': data.get('publisher'),
            'type': data.get('type'),
            'language': data.get('language')
        }
        
        # Extract authors
        for author in data.get('author', []):
            metadata['authors'].append({
                'firstName': author.get('given'),
                'lastName': author.get('family')
            })
        
        # Extract publication date, trying different fields
        date_fields = ['published-print', 'published-online', 'created']
        for field in date_fields:
            if field in data:
                date_parts = data[field].get('date-parts', [[None]])[0]
                if date_parts and date_parts[0]:
                    metadata['year'] = date_parts[0]
                    break
        
        return metadata
    
    def _update_rate_limits(self, response: aiohttp.ClientResponse) -> None:
        """Update rate limit information from response headers."""
        try:
            self._rate_limit_remaining = int(response.headers.get('X-Rate-Limit-Remaining', 0))
            reset_timestamp = int(response.headers.get('X-Rate-Limit-Reset', 0))
            self._rate_limit_reset = datetime.fromtimestamp(reset_timestamp)
        except (ValueError, TypeError):
            self._rate_limit_remaining = None
            self._rate_limit_reset = None

class CrossRefAPIError(Exception):
    """Raised when the CrossRef API returns an error."""
    pass

class DOINotFoundError(CrossRefAPIError):
    """Raised when a DOI is not found in CrossRef."""
    pass

class RateLimitError(Exception):
    """Raised when rate limits are exceeded."""
    pass 