import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import time
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OPDSClient:
    """Client for accessing Project Gutenberg's OPDS feed with rate limiting."""
    
    def __init__(self, base_url: str = 'https://www.gutenberg.org/ebooks/search.opds/'):
        """Initialize the OPDS client."""
        self.base_url = base_url
        
    def fetch_feed(self, path: str = '') -> str:
        """Fetch OPDS feed with rate limiting."""
        url = self.base_url + path
        logger.info(f"Fetching OPDS feed from {url}")
        
        time.sleep(2)  # Rate limiting between requests
        response = requests.get(url)
        response.raise_for_status()
        
        return response.text
    
    def parse_feed(self, feed_content: str) -> List[Dict]:
        """Parse OPDS feed content."""
        root = ET.fromstring(feed_content)
        entries = []
        
        ns = {
            'atom': 'http://www.w3.org/2005/Atom',
            'dc': 'http://purl.org/dc/terms/',
            'opds': 'http://opds-spec.org/2010/catalog'
        }
        
        for entry in root.findall('.//atom:entry', ns):
            book_data = {
                'id': None,
                'title': None,
                'authors': [],
                'updated': None,
                'summary': None,
                'links': []
            }
            
            # Extract basic metadata
            id_elem = entry.find('atom:id', ns)
            if id_elem is not None:
                book_data['id'] = id_elem.text.split('/')[-1]
            
            title = entry.find('atom:title', ns)
            if title is not None:
                book_data['title'] = title.text
            
            # Extract authors
            authors = entry.findall('atom:author/atom:name', ns)
            book_data['authors'] = [author.text for author in authors if author.text]
            
            # Extract update time
            updated = entry.find('atom:updated', ns)
            if updated is not None:
                book_data['updated'] = updated.text
            
            # Extract summary
            summary = entry.find('atom:summary', ns)
            if summary is not None:
                book_data['summary'] = summary.text
            
            # Extract links
            links = entry.findall('atom:link', ns)
            for link in links:
                link_data = {
                    'href': link.get('href'),
                    'type': link.get('type'),
                    'rel': link.get('rel')
                }
                book_data['links'].append(link_data)
            
            if book_data['id']:  # Only add if we got valid data
                entries.append(book_data)
        
        return entries
    
    def discover_new_books(self) -> List[Dict]:
        """Discover new books from OPDS feed."""
        logger.info(f'Starting book discovery at {datetime.now()}')
        
        try:
            feed = self.fetch_feed()
            entries = self.parse_feed(feed)
            logger.info(f'Discovered {len(entries)} books')
            return entries
            
        except Exception as e:
            logger.error(f'Book discovery failed: {str(e)}')
            return []
