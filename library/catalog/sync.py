import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import os
import time
import tarfile
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class GutenbergCatalog:
    """Handles synchronization with Project Gutenberg's XML/RDF catalog."""
    
    def __init__(self, cache_dir: str = 'cache'):
        """Initialize the catalog handler with a cache directory."""
        self.cache_dir = os.path.join(os.path.dirname(__file__), cache_dir)
        os.makedirs(self.cache_dir, exist_ok=True)
        
    def fetch_rdf_catalog(self) -> str:
        """Fetch the latest RDF catalog with rate limiting."""
        url = 'https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2'
        local_path = os.path.join(self.cache_dir, 'rdf-files.tar.bz2')
        
        logger.info(f"Fetching RDF catalog from {url}")
        time.sleep(2)  # Rate limiting between requests
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                time.sleep(0.1)  # Rate limiting within chunks
        
        return local_path
    
    def extract_catalog(self, archive_path: str) -> str:
        """Extract the RDF catalog archive."""
        extract_dir = os.path.join(self.cache_dir, 'rdf')
        os.makedirs(extract_dir, exist_ok=True)
        
        logger.info(f"Extracting catalog to {extract_dir}")
        with tarfile.open(archive_path, 'r:bz2') as tar:
            tar.extractall(path=extract_dir)
        
        return extract_dir
    
    def parse_metadata(self, rdf_file: str) -> Dict:
        """Parse RDF metadata from a file."""
        tree = ET.parse(rdf_file)
        root = tree.getroot()
        
        ns = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'dcterms': 'http://purl.org/dc/terms/',
            'pgterms': 'http://www.gutenberg.org/2009/pgterms/'
        }
        
        metadata = {
            'id': None,
            'title': None,
            'authors': [],
            'language': [],
            'subjects': [],
            'release_date': None,
            'downloads': None,
            'type': None
        }
        
        ebook = root.find('.//pgterms:ebook', ns)
        if ebook is not None:
            metadata['id'] = ebook.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about').split('/')[-1]
            
            title = ebook.find('.//dcterms:title', ns)
            if title is not None:
                metadata['title'] = title.text
            
            creators = ebook.findall('.//dcterms:creator', ns)
            for creator in creators:
                agent = creator.find('.//pgterms:agent', ns)
                if agent is not None:
                    name = agent.find('.//pgterms:name', ns)
                    if name is not None:
                        metadata['authors'].append(name.text)
            
            languages = ebook.findall('.//dcterms:language/rdf:Description/rdf:value', ns)
            metadata['language'] = [lang.text for lang in languages]
            
            subjects = ebook.findall('.//dcterms:subject/rdf:Description/rdf:value', ns)
            metadata['subjects'] = [subj.text for subj in subjects]
            
            release_date = ebook.find('.//dcterms:issued', ns)
            if release_date is not None:
                metadata['release_date'] = release_date.text
        
        return metadata
    
    def sync(self) -> List[Dict]:
        """Perform a full catalog sync."""
        logger.info(f'Starting catalog sync at {datetime.now()}')
        
        try:
            catalog_file = self.fetch_rdf_catalog()
            extract_dir = self.extract_catalog(catalog_file)
            
            metadata_list = []
            for root, _, files in os.walk(extract_dir):
                for file in files:
                    if file.endswith('.rdf'):
                        rdf_path = os.path.join(root, file)
                        try:
                            metadata = self.parse_metadata(rdf_path)
                            if metadata['id']:  # Only add if we got valid metadata
                                metadata_list.append(metadata)
                        except ET.ParseError as e:
                            logger.error(f'Error parsing {file}: {str(e)}')
                        time.sleep(0.1)  # Rate limiting between files
            
            logger.info(f'Processed {len(metadata_list)} catalog entries')
            return metadata_list
            
        except Exception as e:
            logger.error(f'Catalog sync failed: {str(e)}')
            return []
