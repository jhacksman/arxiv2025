# Project Gutenberg Library Integration

This directory contains the integration with Project Gutenberg's catalog, focusing on computer science and related texts.

## Focus Areas

### Computer Science (Priority)
- Algorithm Analysis
- Programming Theory
- Mathematical Foundations
- Computer Architecture
- Information Theory

### Mathematics
- Number Theory
- Geometry
- Algebra
- Analysis
- Logic

### Philosophy
- Logic and Computation
- Philosophy of Mathematics
- Ethics in Computing
- Information Ethics

## Implementation Details

### Rate Limiting
- 2-second delay between requests
- Chunked downloads with 0.1s delays
- Respects robot access policy

### Content Organization
- Categorized by subject area
- Format priority: TXT > HTML > EPUB
- Checksum verification
- Delta updates

### Tools
- `catalog/`: Catalog sync and OPDS integration
- `tools/`: Content retrieval and indexing
- `content/`: Organized text content

## Usage
```python
from library.catalog.sync import GutenbergCatalog
from library.catalog.opds import OPDSClient
from library.tools.downloader import ContentDownloader
from library.tools.indexer import ContentIndexer

# Initialize components
catalog = GutenbergCatalog()
opds = OPDSClient()
downloader = ContentDownloader()
indexer = ContentIndexer()

# Sync catalog and discover books
metadata = catalog.sync()
new_books = opds.discover_new_books()

# Download and index content
downloaded = downloader.download_books(metadata)
for book in downloaded:
    indexer.update_index(book)
```
