# OPTIMIZED Database Manager with Enhanced Search Strategies
# Replace your existing app/core/database.py with this OPTIMIZED version

import logging
import json
import re
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from dateutil.relativedelta import relativedelta
import numpy as np
from supabase import create_client, Client
from dataclasses import dataclass
from enum import Enum

# Optional dependencies with fallbacks
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False

try:
    from sentence_transformers import SentenceTransformer
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False
import os
from dotenv import load_dotenv

load_dotenv()

# Create a simple settings class
class Settings:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

settings = Settings()

# Replace logger setup with simple logging
import logging
logger = logging.getLogger(__name__)

@dataclass
class TemporalEntity:
    """Represents a temporal reference found in text"""
    text: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    period_type: str = "unknown"  # day, month, quarter, year
    confidence: float = 0.0

class SearchStrategy(Enum):
    VECTOR_SIMILARITY = "vector_similarity"
    TEMPORAL_BOOST = "temporal_boost"
    HYBRID = "hybrid"
    FALLBACK = "fallback"

class ProductionRAGSearch:
    """
    OPTIMIZED Production-ready RAG search system with intelligent temporal understanding
    """
    
    def __init__(self):
        self._client: Optional[Client] = None
        self.nlp = None
        self.sentence_transformer = None
        
        # Initialize NLP components
        self._initialize_nlp()
        
        # Enhanced temporal patterns for robust date recognition
        self.temporal_patterns = {
            # ISO formats
            'iso_date': r'\b\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\b',
            'iso_reverse': r'\b\d{1,2}[-/.]\d{1,2}[-/.]\d{4}\b',
            
            # Month-year patterns
            'month_year': r'\b(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\s+\d{4}\b',
            
            # Quarter patterns
            'quarter': r'\b(q[1-4]|quarter\s*[1-4]|[1-4](st|nd|rd|th)?\s*quarter)\s*\d{4}\b',
            
            # Relative dates
            'relative': r'\b(last|this|next|current|previous)\s+(week|month|quarter|year)\b',
            
            # Month names with optional year
            'month_name': r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b',
            'month_abbr': r'\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\b',
            
            # Specific date mentions
            'ordinal_date': r'\b\d{1,2}(st|nd|rd|th)\s+(of\s+)?(january|february|march|april|may|june|july|august|september|october|november|december)\b',
        }
        
        # Month normalization
        self.month_mapping = {
            'january': 1, 'jan': 1, 'february': 2, 'feb': 2, 'march': 3, 'mar': 3,
            'april': 4, 'apr': 4, 'may': 5, 'june': 6, 'jun': 6,
            'july': 7, 'jul': 7, 'august': 8, 'aug': 8, 'september': 9, 'sep': 9,
            'october': 10, 'oct': 10, 'november': 11, 'nov': 11, 'december': 12, 'dec': 12
        }
    
    def _initialize_nlp(self):
        """Initialize NLP components for better text understanding"""
        if SPACY_AVAILABLE:
            try:
                self.nlp = spacy.load("en_core_web_sm")
                logger.info("Loaded spaCy model for temporal NER")
            except:
                logger.warning("spaCy model not available - using pattern matching only")
                self.nlp = None
        else:
            logger.warning("spaCy not available - using pattern matching only")
            self.nlp = None
        
        if TRANSFORMERS_AVAILABLE:
            try:
                self.sentence_transformer = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("Loaded sentence transformer for semantic search")
            except:
                logger.warning("Sentence transformers not available - using OpenAI embeddings only")
                self.sentence_transformer = None
        else:
            logger.warning("Sentence transformers not available - using OpenAI embeddings only")
            self.sentence_transformer = None
    
    @property
    def client(self) -> Client:
        """Lazy initialization of Supabase client"""
        if self._client is None:
            try:
                self._client = create_client(
                    settings.SUPABASE_URL,
                    settings.SUPABASE_ANON_KEY
                )
                logger.info("Connected to Supabase database")
            except Exception as e:
                logger.error(f"Failed to connect to Supabase: {e}")
                raise
        return self._client
    
    def search_documents(
        self, 
        query_embedding: List[float], 
        query_text: str = "",
        limit: int = 5,
        similarity_threshold: float = 0.3
    ) -> List[Dict]:
        """
        OPTIMIZED Universal search that handles all temporal references intelligently
        """
        try:
            logger.info(f"Processing query: '{query_text}'")
            
            # Step 1: Extract temporal entities from query
            temporal_entities = self.extract_temporal_entities(query_text)
            
            # Step 2: Get all relevant documents (with optimized filtering)
            documents = self._get_documents_from_database_optimized(query_text)
            
            if not documents:
                logger.warning("No documents found in database")
                return []
            
            # Step 3: Determine search strategy based on query
            strategy = self._determine_search_strategy(query_text, temporal_entities)
            logger.info(f"Using search strategy: {strategy.value}")
            
            # Step 4: Execute search based on strategy
            if strategy == SearchStrategy.TEMPORAL_BOOST:
                results = self._temporal_boosted_search(
                    documents, query_embedding, query_text, temporal_entities, limit
                )
            elif strategy == SearchStrategy.VECTOR_SIMILARITY:
                results = self._vector_similarity_search(
                    documents, query_embedding, similarity_threshold, limit
                )
            else:  # HYBRID (default)
                results = self._hybrid_search(
                    documents, query_embedding, query_text, temporal_entities, 
                    similarity_threshold, limit
                )
            
            logger.info(f"Search completed: {len(results)} results")
            return self._format_results(results)
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return self._fallback_search(query_text, limit)
    
    def _get_documents_from_database_optimized(self, query_text: str = "") -> List[Dict]:
        """
        OPTIMIZED: Retrieve documents with smart pre-filtering
        """
        try:
            # Basic query for all documents
            query = self.client.table("documents").select(
                "id, content, file_name, file_id, chunk_index, metadata, processed_at, embedding"
            )
            
            # OPTIMIZATION: Pre-filter based on query keywords for better performance
            if query_text:
                query_words = query_text.lower().split()
                priority_keywords = ['roas', 'ctr', 'cpc', 'revenue', 'conversion', 'performance', 
                                   'july', 'june', 'may', 'august', 'responsibilities', 'manager']
                
                # Check if query contains high-priority keywords
                has_priority_keywords = any(keyword in query_words for keyword in priority_keywords)
                
                if has_priority_keywords:
                    # For priority queries, get more documents to ensure we don't miss relevant content
                    result = query.limit(1000).execute()  # Increased limit for important queries
                else:
                    result = query.limit(500).execute()   # Standard limit
            else:
                result = query.limit(300).execute()       # Conservative limit for general queries
            
            return result.data if result.data else []
            
        except Exception as e:
            logger.error(f"Failed to retrieve documents: {e}")
            return []
    
    def extract_temporal_entities(self, text: str) -> List[TemporalEntity]:
        """ENHANCED: Extract temporal entities using multiple methods for robustness"""
        entities = []
        text_lower = text.lower()
        
        # Method 1: Use spaCy if available
        if self.nlp:
            entities.extend(self._extract_with_spacy(text))
        
        # Method 2: Pattern-based extraction (ENHANCED)
        entities.extend(self._extract_with_patterns_enhanced(text_lower))
        
        # Method 3: dateutil parsing for various formats
        entities.extend(self._extract_with_dateutil(text))
        
        # Remove duplicates and sort by confidence
        unique_entities = self._deduplicate_entities(entities)
        
        logger.debug(f"Extracted temporal entities: {[e.text for e in unique_entities]}")
        return unique_entities
    
    def _extract_with_patterns_enhanced(self, text: str) -> List[TemporalEntity]:
        """ENHANCED: Pattern-based extraction with better marketing context"""
        entities = []
        
        # Enhanced patterns with marketing context
        enhanced_patterns = {
            **self.temporal_patterns,
            # Marketing-specific temporal patterns
            'scorecard_date': r'scorecard\s*[-\s]*(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})',
            'report_period': r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(report|performance|monthly)',
            'performance_month': r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(performance|metrics|roas|ctr)',
        }
        
        for pattern_name, pattern in enhanced_patterns.items():
            matches = re.finditer(pattern, text, re.IGNORECASE)
            
            for match in matches:
                matched_text = match.group(0)
                parsed_date = self._parse_date_string(matched_text)
                
                if parsed_date:
                    # Higher confidence for marketing-specific patterns
                    confidence = 0.9 if 'scorecard' in pattern_name or 'report' in pattern_name else 0.8
                    
                    entities.append(TemporalEntity(
                        text=matched_text,
                        start_date=parsed_date,
                        period_type=self._infer_period_type(pattern_name),
                        confidence=confidence
                    ))
        
        return entities
    
    def _extract_with_spacy(self, text: str) -> List[TemporalEntity]:
        """Extract temporal entities using spaCy NER"""
        entities = []
        
        try:
            doc = self.nlp(text)
            
            for ent in doc.ents:
                if ent.label_ in ["DATE", "TIME"]:
                    # Try to parse the date
                    parsed_date = self._parse_date_string(ent.text)
                    if parsed_date:
                        entities.append(TemporalEntity(
                            text=ent.text,
                            start_date=parsed_date,
                            confidence=0.9
                        ))
        except Exception as e:
            logger.debug(f"spaCy extraction failed: {e}")
        
        return entities
    
    def _extract_with_dateutil(self, text: str) -> List[TemporalEntity]:
        """Extract dates using dateutil's fuzzy parsing"""
        entities = []
        
        # Look for potential date strings
        words = text.split()
        
        for i in range(len(words)):
            for length in [1, 2, 3, 4]:  # Try different phrase lengths
                if i + length > len(words):
                    break
                
                phrase = " ".join(words[i:i + length])
                
                try:
                    parsed_date = date_parser.parse(phrase, fuzzy=True)
                    
                    # Only accept if the phrase looks date-like
                    if self._looks_like_date(phrase):
                        entities.append(TemporalEntity(
                            text=phrase,
                            start_date=parsed_date,
                            confidence=0.7
                        ))
                except:
                    continue
        
        return entities
    
    def _parse_date_string(self, date_str: str) -> Optional[datetime]:
        """ENHANCED: Parse various date string formats with marketing context"""
        
        # Handle special formats
        date_str_clean = date_str.strip().lower()
        
        # Handle formats like "2025.06.18" or "2025/06/18"
        if re.match(r'\d{4}[./-]\d{1,2}[./-]\d{1,2}', date_str_clean):
            try:
                date_str_clean = re.sub(r'[./-]', '-', date_str_clean)
                return datetime.strptime(date_str_clean, '%Y-%m-%d')
            except:
                pass
        
        # Handle month names with marketing context
        for month_name, month_num in self.month_mapping.items():
            if month_name in date_str_clean:
                # Extract year if present
                year_match = re.search(r'\d{4}', date_str_clean)
                year = int(year_match.group()) if year_match else datetime.now().year
                
                # Handle scorecard dates with month context
                if 'scorecard' in date_str_clean or 'report' in date_str_clean:
                    return datetime(year, month_num, 1)
                
                return datetime(year, month_num, 1)
        
        # Handle quarters
        quarter_match = re.search(r'q([1-4])', date_str_clean)
        if quarter_match:
            quarter = int(quarter_match.group(1))
            year_match = re.search(r'\d{4}', date_str_clean)
            year = int(year_match.group()) if year_match else datetime.now().year
            
            month = (quarter - 1) * 3 + 1
            return datetime(year, month, 1)
        
        # Use dateutil as fallback
        try:
            return date_parser.parse(date_str, fuzzy=True)
        except:
            return None
    
    def _determine_search_strategy(
        self, 
        query_text: str, 
        temporal_entities: List[TemporalEntity]
    ) -> SearchStrategy:
        """ENHANCED: Strategy determination with better document-specific detection"""
        
        query_lower = query_text.lower()
        
        # ENHANCED: Document-specific queries with broader matching
        document_indicators = {
            'roles_responsibilities': ['roles', 'responsibilities', 'job description', 'duties',
                                     'performance marketing manager', 'core responsibilities',
                                     'strategic planning', 'leadership'],
            'budget_mailboxes': ['budget mailboxes', 'budget mailbox', 'bmb'],
            'mailbox_works': ['mailbox works', 'mw'],
            'scorecards': ['scorecard', 'performance scorecard', 'weekly scorecard'],
            'monthly_reports': ['monthly report', 'monthly performance', 'monthly analysis']
        }
        
        for doc_type, indicators in document_indicators.items():
            if any(indicator in query_lower for indicator in indicators):
                return SearchStrategy.HYBRID  # Use hybrid for better document matching
        
        # Strong temporal signals
        if temporal_entities and any(e.confidence > 0.8 for e in temporal_entities):
            return SearchStrategy.TEMPORAL_BOOST
        
        # Performance metrics queries
        performance_keywords = ['performance', 'metrics', 'roas', 'ctr', 'conversion', 'revenue', 
                               'cpc', 'spend', 'impressions', 'clicks', 'budget']
        if any(keyword in query_lower for keyword in performance_keywords):
            return SearchStrategy.HYBRID
        
        # Default to vector similarity
        return SearchStrategy.VECTOR_SIMILARITY
    
    def _calculate_document_relevance(self, doc: Dict, query_text: str) -> float:
        """ENHANCED: Calculate document-specific relevance for targeted queries"""
        if not query_text:
            return 1.0
        
        query_lower = query_text.lower()
        file_name = doc.get('file_name', '').lower()
        content = doc.get('content', '').lower()
        
        # ENHANCED: Document-specific boosting patterns with better matching
        document_patterns = {
            # Roles & Responsibilities queries (ENHANCED)
            'roles_responsibilities': {
                'triggers': ['roles', 'responsibilities', 'job description', 'duties', 
                           'performance marketing manager', 'strategic planning', 'leadership',
                           'core responsibilities', 'required skills'],
                'target_files': ['roles', 'responsibilities', 'performance marketing manager'],
                'content_indicators': ['strategic planning', 'team management', 'core responsibilities', 
                                     'required skills', 'leadership', 'budget management'],
                'boost': 3.0  # Increased boost for roles queries
            },
            
            # Budget Mailboxes specific
            'budget_mailboxes': {
                'triggers': ['budget mailboxes', 'budget mailbox', 'bmb'],
                'target_files': ['budget mailboxes', 'budget mailbox'],
                'content_indicators': ['budget mailboxes', 'bmb', 'budget allocation'],
                'boost': 2.0
            },
            
            # Mailbox Works specific  
            'mailbox_works': {
                'triggers': ['mailbox works', 'mw'],
                'target_files': ['mailbox works'],
                'content_indicators': ['mailbox works', 'mw'],
                'boost': 2.0
            },
            
            # Monthly reports (ENHANCED)
            'monthly_reports': {
                'triggers': ['monthly report', 'monthly performance', 'monthly analysis'],
                'target_files': ['monthly report'],
                'content_indicators': ['monthly report', 'performance analysis', 'recommendations',
                                     'executive summary', 'key metrics'],
                'boost': 1.8
            },
            
            # Scorecards (NEW)
            'scorecards': {
                'triggers': ['scorecard', 'performance scorecard', 'weekly scorecard'],
                'target_files': ['scorecard'],
                'content_indicators': ['roas', 'ctr', 'cpc', 'performance metrics', 'kpi'],
                'boost': 1.8
            }
        }
        
        max_boost = 1.0
        
        for pattern_name, pattern_config in document_patterns.items():
            # Check if query contains triggers
            trigger_matches = sum(1 for trigger in pattern_config['triggers'] 
                                if trigger in query_lower)
            
            if trigger_matches > 0:
                # Check if document filename matches
                filename_matches = sum(1 for target in pattern_config['target_files']
                                     if target in file_name)
                
                # Check if document content matches
                content_matches = sum(1 for indicator in pattern_config['content_indicators']
                                    if indicator in content)
                
                # ENHANCED: Better scoring algorithm
                if filename_matches > 0 or content_matches > 0:
                    # More sophisticated boost calculation
                    filename_score = filename_matches * 2.0
                    content_score = min(content_matches / len(pattern_config['content_indicators']), 1.0)
                    combined_score = (filename_score + content_score) / 3.0
                    
                    boost = pattern_config['boost'] * max(combined_score, 0.5)  # Minimum 50% of full boost
                    max_boost = max(max_boost, boost)
        
        return min(max_boost, 4.0)  # Increased cap to 4x boost
    
    def _hybrid_search(
        self,
        documents: List[Dict],
        query_embedding: List[float],
        query_text: str,
        temporal_entities: List[TemporalEntity],
        similarity_threshold: float,
        limit: int
    ) -> List[Dict]:
        """ENHANCED: Hybrid search with better scoring and document-specific relevance"""
        
        scored_docs = []
        
        for doc in documents:
            # Calculate semantic similarity
            semantic_score = self._calculate_semantic_similarity(doc, query_embedding, query_text)
            
            # ENHANCED: Dynamic threshold adjustment based on query type
            document_relevance = self._calculate_document_relevance(doc, query_text)
            adjusted_threshold = similarity_threshold * (0.3 if document_relevance > 2.0 else 0.5)
            
            if semantic_score < adjusted_threshold:
                continue
            
            # Calculate temporal relevance
            temporal_score = self._calculate_temporal_relevance(doc, temporal_entities)
            
            # Calculate keyword relevance
            keyword_score = self._calculate_keyword_relevance_enhanced(doc, query_text)
            
            # ENHANCED: Better scoring logic with document type awareness
            if document_relevance > 2.0:  # Strong document match (roles, responsibilities)
                # Prioritize document-specific content heavily
                final_score = (semantic_score * 0.15 + 
                             keyword_score * 0.35 + 
                             document_relevance * 0.5)
                
            elif temporal_entities:
                # Temporal query - balance all factors
                final_score = (semantic_score * 0.3 + 
                             temporal_score * 0.4 + 
                             keyword_score * 0.2 +
                             document_relevance * 0.1)
            else:
                # General query - focus on semantic + keyword + document relevance
                final_score = (semantic_score * 0.45 + 
                             keyword_score * 0.35 +
                             document_relevance * 0.2)
            
            # Apply document relevance multiplier (reduced impact to avoid over-boosting)
            final_score *= min(document_relevance, 2.5)
            
            doc['similarity_score'] = final_score
            scored_docs.append(doc)
        
        # Sort and return top results
        scored_docs.sort(key=lambda x: x['similarity_score'], reverse=True)
        
        # ENHANCED: Debugging with more detail
        logger.debug("Top search results:")
        for i, doc in enumerate(scored_docs[:5]):
            file_name = doc.get('file_name', 'Unknown')
            score = doc['similarity_score']
            chunk_method = doc.get('metadata', {}).get('chunking_method', 'unknown')
            logger.debug(f"  {i+1}. {file_name} (Score: {score:.3f}, Method: {chunk_method})")
        
        return scored_docs[:limit]
    
    def _calculate_keyword_relevance_enhanced(self, doc: Dict, query_text: str) -> float:
        """ENHANCED: Better keyword relevance calculation"""
        
        if not query_text:
            return 0.3
        
        query_words = set(word.lower() for word in query_text.split() if len(word) > 2)
        doc_content = (doc.get('content', '') + ' ' + doc.get('file_name', '')).lower()
        
        if not query_words:
            return 0.3
        
        # ENHANCED: Weight important keywords more heavily
        important_keywords = ['roas', 'ctr', 'cpc', 'performance', 'responsibilities', 
                            'manager', 'revenue', 'conversion', 'july', 'june', 'may']
        
        total_score = 0
        word_count = 0
        
        for word in query_words:
            word_count += 1
            if word in doc_content:
                # Higher weight for important keywords
                weight = 2.0 if word in important_keywords else 1.0
                total_score += weight
        
        return min(total_score / (word_count * 1.5), 1.0)  # Normalize with importance weighting
    
    def _temporal_boosted_search(
        self,
        documents: List[Dict],
        query_embedding: List[float],
        query_text: str,
        temporal_entities: List[TemporalEntity],
        limit: int
    ) -> List[Dict]:
        """Search with strong temporal boosting"""
        
        scored_docs = []
        
        for doc in documents:
            # Calculate base semantic similarity
            base_score = self._calculate_semantic_similarity(doc, query_embedding, query_text)
            
            # Calculate temporal relevance
            temporal_score = self._calculate_temporal_relevance(doc, temporal_entities)
            
            # Combine scores with strong temporal weighting
            final_score = base_score * 0.3 + temporal_score * 0.7
            
            doc['similarity_score'] = final_score
            scored_docs.append(doc)
        
        # Sort and return top results
        scored_docs.sort(key=lambda x: x['similarity_score'], reverse=True)
        return scored_docs[:limit]
    
    def _calculate_semantic_similarity(
        self, 
        doc: Dict, 
        query_embedding: List[float], 
        query_text: str
    ) -> float:
        """Calculate semantic similarity using embeddings"""
        
        try:
            # Use provided query embedding if available
            if query_embedding and len(query_embedding) > 0:
                doc_embedding = doc.get('embedding')
                if doc_embedding:
                    if isinstance(doc_embedding, str):
                        doc_embedding = json.loads(doc_embedding)
                    
                    return self._cosine_similarity(query_embedding, doc_embedding)
            
            # Fallback to sentence transformer if available
            if self.sentence_transformer and query_text:
                doc_content = doc.get('content', '')
                if doc_content:
                    query_emb = self.sentence_transformer.encode([query_text])
                    doc_emb = self.sentence_transformer.encode([doc_content])
                    return float(np.dot(query_emb[0], doc_emb[0]) / 
                               (np.linalg.norm(query_emb[0]) * np.linalg.norm(doc_emb[0])))
            
            # Final fallback to keyword matching
            return self._calculate_keyword_relevance_enhanced(doc, query_text)
            
        except Exception as e:
            logger.debug(f"Semantic similarity calculation failed: {e}")
            return 0.3  # Default score
    
    def _calculate_temporal_relevance(
        self, 
        doc: Dict, 
        temporal_entities: List[TemporalEntity]
    ) -> float:
        """ENHANCED: Calculate how well document matches temporal context"""
        
        if not temporal_entities:
            return 0.5  # Neutral score
        
        doc_content = (doc.get('content', '') + ' ' + doc.get('file_name', '')).lower()
        max_relevance = 0.0
        
        for entity in temporal_entities:
            relevance = 0.0
            
            # Direct text match
            if entity.text.lower() in doc_content:
                relevance = 1.0
            
            # Month/year matching with enhanced patterns
            elif entity.start_date:
                date_obj = entity.start_date
                month_name = date_obj.strftime('%B').lower()
                month_abbr = date_obj.strftime('%b').lower()
                year_str = str(date_obj.year)
                
                # ENHANCED: Better temporal matching
                if month_name in doc_content or month_abbr in doc_content:
                    # Higher relevance if it's a scorecard or report for that month
                    if 'scorecard' in doc_content or 'report' in doc_content:
                        relevance = 0.95
                    else:
                        relevance = 0.9
                elif year_str in doc_content:
                    relevance = 0.6
                
                # Check for date patterns in filename
                filename = doc.get('file_name', '').lower()
                if month_name in filename or month_abbr in filename or year_str in filename:
                    relevance = max(relevance, 0.85)
            
            # Weight by entity confidence
            weighted_relevance = relevance * entity.confidence
            max_relevance = max(max_relevance, weighted_relevance)
        
        return max_relevance
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Calculate cosine similarity between two vectors"""
        try:
            v1 = np.array(vec1)
            v2 = np.array(vec2)
            
            dot_product = np.dot(v1, v2)
            norms = np.linalg.norm(v1) * np.linalg.norm(v2)
            
            if norms == 0:
                return 0.0
            
            return float(dot_product / norms)
        except:
            return 0.0
    
    def _vector_similarity_search(
        self,
        documents: List[Dict],
        query_embedding: List[float],
        similarity_threshold: float,
        limit: int
    ) -> List[Dict]:
        """Pure vector similarity search"""
        scored_docs = []
        
        for doc in documents:
            similarity = self._calculate_semantic_similarity(doc, query_embedding, "")
            
            if similarity > similarity_threshold:
                doc['similarity_score'] = similarity
                scored_docs.append(doc)
        
        scored_docs.sort(key=lambda x: x['similarity_score'], reverse=True)
        return scored_docs[:limit]
    
    def _fallback_search(self, query_text: str, limit: int) -> List[Dict]:
        """Fallback search when main search fails"""
        try:
            documents = self._get_documents_from_database_optimized()
            
            # Simple keyword-based scoring
            scored_docs = []
            for doc in documents:
                score = self._calculate_keyword_relevance_enhanced(doc, query_text)
                doc['similarity_score'] = score
                scored_docs.append(doc)
            
            scored_docs.sort(key=lambda x: x['similarity_score'], reverse=True)
            return self._format_results(scored_docs[:limit])
            
        except Exception as e:
            logger.error(f"Fallback search failed: {e}")
            return []
    
    def _format_results(self, results: List[Dict]) -> List[Dict]:
        """Format results for QA engine compatibility"""
        formatted_results = []
        
        for doc in results:
            metadata = doc.get('metadata', {})
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            formatted_doc = {
                'id': doc.get('id'),
                'content': doc.get('content', ''),
                'similarity': doc.get('similarity_score', 0.7),
                'metadata': metadata,
                'file_name': doc.get('file_name') or metadata.get('file_name', ''),
                'file_id': doc.get('file_id') or metadata.get('file_id', ''),
                'chunk_index': doc.get('chunk_index', 0),
                'processed_at': doc.get('processed_at', '')
            }
            formatted_results.append(formatted_doc)
        
        return formatted_results
    
    # Helper methods for entity processing
    def _deduplicate_entities(self, entities: List[TemporalEntity]) -> List[TemporalEntity]:
        """Remove duplicate temporal entities"""
        seen = set()
        unique = []
        
        for entity in entities:
            key = (entity.text.lower(), str(entity.start_date))
            if key not in seen:
                seen.add(key)
                unique.append(entity)
        
        return sorted(unique, key=lambda x: x.confidence, reverse=True)
    
    def _infer_period_type(self, pattern_name: str) -> str:
        """Infer the period type from pattern name"""
        if 'quarter' in pattern_name:
            return 'quarter'
        elif 'month' in pattern_name:
            return 'month'
        elif 'year' in pattern_name:
            return 'year'
        else:
            return 'day'
    
    def _looks_like_date(self, phrase: str) -> bool:
        """Check if phrase looks like a date"""
        # Simple heuristics
        date_indicators = ['january', 'february', 'march', 'april', 'may', 'june',
                          'july', 'august', 'september', 'october', 'november', 'december',
                          'jan', 'feb', 'mar', 'apr', 'may', 'jun',
                          'jul', 'aug', 'sep', 'oct', 'nov', 'dec',
                          'q1', 'q2', 'q3', 'q4', 'quarter']
        
        phrase_lower = phrase.lower()
        return (any(indicator in phrase_lower for indicator in date_indicators) or
                re.search(r'\d{4}', phrase) or
                re.search(r'\d{1,2}[./-]\d{1,2}', phrase))
    
    # Keep existing methods for compatibility
    def health_check(self) -> Dict[str, any]:
        """Check database connectivity"""
        try:
            result = self.client.table("documents").select("count").limit(1).execute()
            return {
                "status": "healthy",
                "connection": True,
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "status": "unhealthy", 
                "connection": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    def document_exists(self, file_id: str, modified_time: str) -> bool:
        """Check if document exists and is up to date"""
        try:
            result = self.client.table("documents").select("metadata").eq(
                "metadata->>file_id", file_id
            ).execute()
            
            if not result.data:
                return False
            
            stored_modified = result.data[0]["metadata"].get("modified_time")
            return stored_modified == modified_time
        except Exception as e:
            logger.error(f"Error checking document existence: {e}")
            return False
    
    def store_documents(self, documents: List[Dict]) -> bool:
        """Store processed documents in batches"""
        if not documents:
            return True
        
        try:
            batch_size = settings.BATCH_SIZE
            total_stored = 0
            
            for i in range(0, len(documents), batch_size):
                batch = documents[i:i + batch_size]
                result = self.client.table("documents").insert(batch).execute()
                
                if not result.data:
                    raise Exception(f"Failed to insert batch starting at index {i}")
                
                total_stored += len(batch)
            
            logger.info(f"Successfully stored {total_stored} documents")
            return True
        except Exception as e:
            logger.error(f"Error storing documents: {e}")
            return False
    
    def delete_document(self, file_id: str) -> bool:
        """Delete all chunks for a specific file"""
        try:
            result = self.client.table("documents").delete().eq(
                "metadata->>file_id", file_id
            ).execute()
            
            deleted_count = len(result.data) if result.data else 0
            logger.info(f"Deleted {deleted_count} chunks for file {file_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {file_id}: {e}")
            return False


# Create the production-ready instance
class DatabaseManager(ProductionRAGSearch):
    """
    Backwards compatible DatabaseManager using production RAG search
    """
    pass


# Global database manager instance
db = DatabaseManager()