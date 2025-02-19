import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentIndexer:
    """Manages content indexing and organization."""
    
    def __init__(self, content_dir: str = 'library/content'):
        """Initialize the content indexer."""
        self.content_dir = Path(content_dir)
        self.index_file = self.content_dir / 'index.json'
    
    def load_index(self) -> Dict:
        """Load existing index or create new one."""
        if self.index_file.exists():
            with open(self.index_file) as f:
                return json.load(f)
        return {'books': {}, 'categories': {}}
    
    def save_index(self, index: Dict):
        """Save index to file."""
        with open(self.index_file, 'w') as f:
            json.dump(index, f, indent=2)
    
    def update_index(self, book_info: Dict):
        """Update index with new book information."""
        index = self.load_index()
        
        book_id = book_info['id']
        category = Path(book_info['path']).parent.name
        
        # Update books section
        index['books'][book_id] = {
            'path': book_info['path'],
            'format': book_info['format'],
            'checksum': book_info['checksum'],
            'category': category
        }
        
        # Update categories section
        if category not in index['categories']:
            index['categories'][category] = []
        if book_id not in index['categories'][category]:
            index['categories'][category].append(book_id)
        
        self.save_index(index)
    
    def get_book_info(self, book_id: str) -> Optional[Dict]:
        """Get information about a specific book."""
        index = self.load_index()
        return index['books'].get(book_id)
    
    def get_category_books(self, category: str) -> List[str]:
        """Get list of books in a category."""
        index = self.load_index()
        return index['categories'].get(category, [])
    
    def verify_integrity(self) -> bool:
        """Verify integrity of indexed files."""
        index = self.load_index()
        all_valid = True
        
        for book_id, info in index['books'].items():
            path = Path(info['path'])
            if not path.exists():
                logger.error(f"Missing file: {path}")
                all_valid = False
                continue
            
            from ..tools.downloader import ContentDownloader
            downloader = ContentDownloader()
            if not downloader.verify_checksum(path, info['checksum']):
                logger.error(f"Checksum mismatch: {path}")
                all_valid = False
        
        return all_valid
