import logging
from typing import Dict, List, Set
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TextCategorizer:
    """Categorizes texts based on subjects and content analysis."""
    
    CS_KEYWORDS = {
        'algorithm': 5,
        'computation': 5,
        'programming': 5,
        'computer': 4,
        'information theory': 4,
        'cryptography': 4,
        'binary': 3,
        'calculation': 3,
        'arithmetic': 3
    }
    
    MATH_KEYWORDS = {
        'mathematics': 5,
        'geometry': 4,
        'algebra': 4,
        'calculus': 4,
        'number theory': 4,
        'mathematical': 3,
        'theorem': 3,
        'proof': 3
    }
    
    PHIL_KEYWORDS = {
        'logic': 5,
        'philosophy': 4,
        'ethics': 4,
        'reasoning': 4,
        'philosophical': 3,
        'epistemology': 3,
        'metaphysics': 3
    }
    
    def __init__(self):
        """Initialize the categorizer."""
        self.categories = {
            'computer-science': self.CS_KEYWORDS,
            'mathematics': self.MATH_KEYWORDS,
            'philosophy': self.PHIL_KEYWORDS
        }
    
    def score_text(self, text: str, keywords: Dict[str, int]) -> int:
        """Score text based on keyword occurrences."""
        score = 0
        text_lower = text.lower()
        
        for keyword, weight in keywords.items():
            count = len(re.findall(r'\b' + re.escape(keyword) + r'\b', text_lower))
            score += count * weight
        
        return score
    
    def categorize(self, metadata: Dict) -> List[str]:
        """Categorize a text based on its metadata."""
        categories = []
        text_content = ' '.join([
            metadata.get('title', ''),
            ' '.join(metadata.get('authors', [])),
            ' '.join(metadata.get('subjects', []))
        ])
        
        # Score for each category
        scores = {}
        for category, keywords in self.categories.items():
            score = self.score_text(text_content, keywords)
            if score > 0:
                scores[category] = score
        
        # Sort by score and return categories
        return [cat for cat, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)]
    
    def filter_cs_texts(self, metadata_list: List[Dict]) -> List[Dict]:
        """Filter and prioritize computer science texts."""
        cs_texts = []
        
        for metadata in metadata_list:
            categories = self.categorize(metadata)
            if categories and categories[0] == 'computer-science':
                metadata['categories'] = categories
                cs_texts.append(metadata)
        
        return sorted(cs_texts, key=lambda x: len(x.get('categories', [])), reverse=True)
    
    def get_related_texts(self, metadata_list: List[Dict]) -> Dict[str, List[Dict]]:
        """Get texts from related fields (mathematics, philosophy)."""
        related_texts = {
            'mathematics': [],
            'philosophy': []
        }
        
        for metadata in metadata_list:
            categories = self.categorize(metadata)
            if categories:
                primary_category = categories[0]
                if primary_category in related_texts:
                    metadata['categories'] = categories
                    related_texts[primary_category].append(metadata)
        
        # Sort each category by relevance
        for category in related_texts:
            related_texts[category] = sorted(
                related_texts[category],
                key=lambda x: len(x.get('categories', [])),
                reverse=True
            )
        
        return related_texts
