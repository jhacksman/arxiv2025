import logging
from typing import Dict, List
from pathlib import Path
import json

from ..catalog.sync import GutenbergCatalog
from ..catalog.opds import OPDSClient
from .downloader import ContentDownloader
from .indexer import ContentIndexer
from .categorize import TextCategorizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextProcessor:
    """Processes and organizes computer science and related texts."""
    
    def __init__(self, base_dir: str = 'library'):
        """Initialize the text processor."""
        self.base_dir = Path(base_dir)
        self.catalog = GutenbergCatalog()
        self.opds = OPDSClient()
        self.downloader = ContentDownloader()
        self.indexer = ContentIndexer()
        self.categorizer = TextCategorizer()
        
        # Create category directories
        for category in ['computer-science', 'mathematics', 'philosophy']:
            (self.base_dir / 'content' / category).mkdir(parents=True, exist_ok=True)
    
    def process_texts(self):
        """Process and organize texts by category."""
        logger.info("Starting text processing")
        
        # Get catalog data
        metadata_list = self.catalog.sync()
        new_books = self.opds.discover_new_books()
        
        # Combine metadata
        all_metadata = metadata_list + new_books
        
        # Process computer science texts first
        cs_texts = self.categorizer.filter_cs_texts(all_metadata)
        logger.info(f"Found {len(cs_texts)} computer science texts")
        
        # Download and index CS texts
        for text in cs_texts:
            result = self.downloader.download_book(text['id'], 'computer-science')
            if result:
                self.indexer.update_index(result)
        
        # Process related fields
        related_texts = self.categorizer.get_related_texts(all_metadata)
        
        for category, texts in related_texts.items():
            logger.info(f"Found {len(texts)} {category} texts")
            for text in texts[:50]:  # Limit to top 50 most relevant texts per category
                result = self.downloader.download_book(text['id'], category)
                if result:
                    self.indexer.update_index(result)
        
        # Save category statistics
        stats = {
            'computer_science': len(cs_texts),
            'mathematics': len(related_texts['mathematics']),
            'philosophy': len(related_texts['philosophy'])
        }
        
        with open(self.base_dir / 'content' / 'stats.json', 'w') as f:
            json.dump(stats, f, indent=2)
        
        logger.info("Text processing completed")
        return stats
