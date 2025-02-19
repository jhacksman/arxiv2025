import requests
import os
import time
import hashlib
import logging
from typing import Optional, List, Dict
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContentDownloader:
    """Handles rate-limited content downloads from Project Gutenberg."""
    
    FORMATS = ['txt', 'html', 'epub']  # Priority order
    
    def __init__(self, base_dir: str = 'library/content'):
        """Initialize the content downloader."""
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def compute_checksum(self, file_path: Path) -> str:
        """Compute SHA-256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def verify_checksum(self, file_path: Path, expected_checksum: str) -> bool:
        """Verify file integrity using checksum."""
        actual_checksum = self.compute_checksum(file_path)
        return actual_checksum == expected_checksum
    
    def download_book(self, book_id: str, category: str) -> Optional[Dict]:
        """Download a book with rate limiting and checksum verification."""
        category_dir = self.base_dir / category
        category_dir.mkdir(exist_ok=True)
        
        book_info = {
            'id': book_id,
            'path': None,
            'format': None,
            'checksum': None
        }
        
        time.sleep(2)  # Rate limiting between books
        
        for format_type in self.FORMATS:
            url = f'https://www.gutenberg.org/files/{book_id}/{book_id}.{format_type}'
            local_path = category_dir / f'{book_id}.{format_type}'
            
            try:
                logger.info(f"Downloading {url}")
                response = requests.get(url, stream=True)
                response.raise_for_status()
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                # Get and verify checksum
                checksum_url = f'{url}.sha256'
                checksum_response = requests.get(checksum_url)
                if checksum_response.ok:
                    expected_checksum = checksum_response.text.split()[0]
                    if self.verify_checksum(local_path, expected_checksum):
                        book_info.update({
                            'path': str(local_path),
                            'format': format_type,
                            'checksum': expected_checksum
                        })
                        return book_info
                    else:
                        logger.error(f"Checksum verification failed for {local_path}")
                        local_path.unlink()
                else:
                    # If no checksum available, just compute and store it
                    book_info.update({
                        'path': str(local_path),
                        'format': format_type,
                        'checksum': self.compute_checksum(local_path)
                    })
                    return book_info
                    
            except requests.RequestException as e:
                logger.warning(f"Failed to download {format_type} format: {str(e)}")
                if local_path.exists():
                    local_path.unlink()
                continue
        
        return None
    
    def download_books(self, book_list: List[Dict]) -> List[Dict]:
        """Download multiple books with rate limiting."""
        results = []
        
        for book in book_list:
            book_id = book['id']
            # Determine category based on subjects
            category = self.determine_category(book.get('subjects', []))
            
            result = self.download_book(book_id, category)
            if result:
                results.append(result)
            
            time.sleep(2)  # Rate limiting between books
        
        return results
    
    def determine_category(self, subjects: List[str]) -> str:
        """Determine the appropriate category based on subjects."""
        subjects_lower = [s.lower() for s in subjects]
        
        if any(kw in ' '.join(subjects_lower) for kw in ['computer', 'programming', 'algorithm']):
            return 'computer-science'
        elif any(kw in ' '.join(subjects_lower) for kw in ['mathematics', 'geometry', 'algebra']):
            return 'mathematics'
        elif any(kw in ' '.join(subjects_lower) for kw in ['philosophy', 'logic', 'ethics']):
            return 'philosophy'
        else:
            return 'other'  # Default category
