import logging
from typing import Dict, List
from pathlib import Path
import json
import requests
from ..catalog.sync import GutenbergCatalog
from ..catalog.opds import OPDSClient
from .downloader import ContentDownloader
from .indexer import ContentIndexer
from .categorize import TextCategorizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PopularBooksProcessor:
    """Processes and downloads popular books from Project Gutenberg."""
    
    def __init__(self, base_dir: str = 'library'):
        """Initialize the popular books processor."""
        self.base_dir = Path(base_dir)
        self.catalog = GutenbergCatalog()
        self.opds = OPDSClient()
        self.downloader = ContentDownloader()
        self.indexer = ContentIndexer()
        self.categorizer = TextCategorizer()
    
    def get_download_count(self, book_id: str) -> int:
        """Get download count for a book from Project Gutenberg."""
        try:
            url = f'https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.rdf'
            response = requests.get(url)
            response.raise_for_status()
            
            # Extract download count from RDF metadata
            import xml.etree.ElementTree as ET
            root = ET.fromstring(response.content)
            ns = {'pgterms': 'http://www.gutenberg.org/2009/pgterms/'}
            downloads = root.find('.//pgterms:downloads', ns)
            return int(downloads.text) if downloads is not None else 0
            
        except Exception as e:
            logger.warning(f"Failed to get download count for book {book_id}: {str(e)}")
            return 0
    
    def process_popular_books(self, limit: int = 100):
        """Process and download the most popular books."""
        logger.info("Starting popular books processing")
        
        # Get catalog data
        metadata_list = self.catalog.sync()
        new_books = self.opds.discover_new_books()
        all_metadata = metadata_list + new_books
        
        # Filter and categorize books
        categorized_books = []
        for metadata in all_metadata:
            categories = self.categorizer.categorize(metadata)
            if categories:  # Only include books that match our categories
                metadata['categories'] = categories
                metadata['downloads'] = self.get_download_count(metadata['id'])
                categorized_books.append(metadata)
        
        # Sort by download count and take top N
        popular_books = sorted(
            categorized_books,
            key=lambda x: x.get('downloads', 0),
            reverse=True
        )[:limit]
        
        # Download and index popular books
        downloaded = []
        for book in popular_books:
            primary_category = book['categories'][0]
            result = self.downloader.download_book(book['id'], primary_category)
            if result:
                result['downloads'] = book.get('downloads', 0)
                result['title'] = book.get('title', '')
                self.indexer.update_index(result)
                downloaded.append(result)
        
        # Save popularity statistics
        stats = {
            'total_processed': len(all_metadata),
            'total_categorized': len(categorized_books),
            'total_downloaded': len(downloaded),
            'categories': {}
        }
        
        for book in downloaded:
            category = Path(book['path']).parent.name
            if category not in stats['categories']:
                stats['categories'][category] = []
            stats['categories'][category].append({
                'id': book['id'],
                'title': book['title'],
                'downloads': book['downloads'],
                'format': book['format']
            })
        
        stats_file = self.base_dir / 'content' / 'popular_books.json'
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info(f"Downloaded {len(downloaded)} popular books")
        return stats

if __name__ == '__main__':
    processor = PopularBooksProcessor()
    processor.process_popular_books(limit=100)
