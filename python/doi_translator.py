"""DOI Translator for resolving and validating DOIs."""

import re
from typing import Optional, Dict, Any
import aiohttp
from datetime import datetime
from ..services.proxy_service import ProxyService

class DOITranslator:
    """Translator for handling DOI resolution and validation."""
    
    # DOI regex pattern based on CrossRef's guidelines
    DOI_PATTERN = re.compile(r'^10\.\d{4,}/[-._;()/:\w]+$')
    
    def __init__(self, http_client: Optional[aiohttp.ClientSession] = None, proxy_service: Optional[ProxyService] = None):
        """Initialize the DOI translator.
        
        Args:
            http_client: Optional HTTP client for making requests
            proxy_service: Optional proxy service for institutional access
        """
        self.http_client = http_client
        self.proxy_service = proxy_service
        self._rate_limit_remaining = None
        self._rate_limit_reset = None
    
    @classmethod
    def with_institutional_access(cls, http_client: Optional[aiohttp.ClientSession] = None) -> 'DOITranslator':
        """Create a DOI translator with institutional access (defaults to UT)."""
        return cls(http_client=http_client, proxy_service=ProxyService.for_ut())
    
    def validate_doi(self, doi: str) -> bool:
        """Validate if a string is a properly formatted DOI."""
        if not doi:
            return False
        return bool(self.DOI_PATTERN.match(doi))
    
    async def resolve_doi(self, doi: str) -> Dict[str, Any]:
        """Resolve a DOI to its metadata using content negotiation.
        
        Args:
            doi: The DOI to resolve
            
        Returns:
            Dict containing the metadata for the DOI
            
        Raises:
            ValueError: If the DOI is invalid
            DOIResolutionError: If the DOI cannot be resolved
            RateLimitError: If we've exceeded the rate limit
        """
        if not self.validate_doi(doi):
            raise ValueError(f"Invalid DOI format: {doi}")
        
        if not self.http_client:
            self.http_client = aiohttp.ClientSession()
        
        headers = {
            'Accept': 'application/vnd.citationstyles.csl+json',
            'User-Agent': 'Bibli/1.0 (mailto:support@bibli.com)'
        }
        
        url = f'https://doi.org/{doi}'
        
        # Transform URL through proxy if available
        if self.proxy_service:
            url = self.proxy_service.transform_url(url)
        
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
                raise DOIResolutionError(
                    f"Failed to resolve DOI: {doi}. Status: {response.status}"
                )
            
            metadata = await response.json()
            
            # Add proxied URLs if available
            if self.proxy_service:
                if 'URL' in metadata:
                    metadata['proxied_url'] = self.proxy_service.transform_url(metadata['URL'])
                if 'link' in metadata:
                    metadata['proxied_link'] = self.proxy_service.transform_url(metadata['link'])
            
            return metadata
                
        except aiohttp.ClientError as e:
            raise DOIResolutionError(f"Network error resolving DOI: {str(e)}")
    
    def _update_rate_limits(self, response: aiohttp.ClientResponse) -> None:
        """Update rate limit information from response headers."""
        try:
            self._rate_limit_remaining = int(response.headers.get('X-Rate-Limit-Remaining', 0))
            reset_timestamp = int(response.headers.get('X-Rate-Limit-Reset', 0))
            self._rate_limit_reset = datetime.fromtimestamp(reset_timestamp)
        except (ValueError, TypeError):
            self._rate_limit_remaining = None
            self._rate_limit_reset = None

class DOIResolutionError(Exception):
    """Raised when a DOI cannot be resolved."""
    pass

class DOINotFoundError(DOIResolutionError):
    """Raised when a DOI is not found."""
    pass

class RateLimitError(Exception):
    """Raised when rate limits are exceeded."""
    pass 