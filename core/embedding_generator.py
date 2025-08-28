# FIXED Embedding Generator  
# Replace your existing app/core/embedding_generator.py with this version

import logging
import openai
from typing import List, Dict, Any, Optional
import asyncio
import json
from datetime import datetime
import time

import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# Create a simple settings class
class Settings:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

settings = Settings()

class EmbeddingGenerator:
    """Enhanced embedding generator with proper error handling"""
    
    def __init__(self):
        self.client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model = "text-embedding-3-small"  # Cost-effective and good performance
        self.batch_size = 20  # Conservative batch size for rate limiting
    
    def generate_embeddings(self, chunks: List[Dict]) -> List[Dict]:
        """
        Generate embeddings for document chunks
        FIXED: Handles the new chunk format properly
        """
        if not chunks:
            logger.warning("No chunks provided for embedding generation")
            return []
        
        logger.info(f"Generating embeddings for {len(chunks)} chunks")
        
        try:
            # Process in batches to avoid rate limits
            embedded_chunks = []
            
            for i in range(0, len(chunks), self.batch_size):
                batch = chunks[i:i + self.batch_size]
                batch_results = self._process_batch(batch, i)
                
                if batch_results:
                    embedded_chunks.extend(batch_results)
                else:
                    logger.error(f"Failed to process batch starting at index {i}")
                    return []  # Fail fast if any batch fails
                
                # Small delay to respect rate limits
                if i + self.batch_size < len(chunks):
                   time.sleep(0.1) 

            
            logger.info(f"Successfully generated embeddings for {len(embedded_chunks)} chunks")
            return embedded_chunks
            
        except Exception as e:
            logger.error(f"Unexpected error generating embeddings: {e}")
            return []
    
    def _process_batch(self, batch: List[Dict], batch_start_index: int) -> List[Dict]:
        """Process a batch of chunks"""
        
        try:
            # Extract text content from chunks (FIX: Handle new format)
            texts = []
            for chunk in batch:
                # Handle both old and new chunk formats
                if isinstance(chunk, dict):
                    content = chunk.get('content', '')
                    if isinstance(content, bytes):
                        content = content.decode('utf-8', errors='ignore')
                    elif not isinstance(content, str):
                        content = str(content)
                    texts.append(content.strip())
                else:
                    # Fallback for unexpected formats
                    text = str(chunk).strip() if chunk else ""
                    texts.append(text)
            
            # Filter out empty texts
            valid_texts = []
            valid_indices = []
            for i, text in enumerate(texts):
                if text and len(text) > 10:  # Minimum content length
                    valid_texts.append(text)
                    valid_indices.append(i)
            
            if not valid_texts:
                logger.warning(f"No valid texts in batch starting at {batch_start_index}")
                return []
            
            # Generate embeddings via OpenAI
            logger.debug(f"Generating embeddings for batch of {len(valid_texts)} texts")
            
            response = self.client.embeddings.create(
                model=self.model,
                input=valid_texts
            )
            
            # Create result chunks with embeddings (FIX: Ensure proper format)
            embedded_chunks = []
            
            for i, valid_index in enumerate(valid_indices):
                original_chunk = batch[valid_index]
                embedding = response.data[i].embedding
                
                # Create properly formatted embedded chunk
                embedded_chunk = {
                    'content': texts[valid_index],  # Ensure it's clean text
                    'embedding': embedding,
                    'metadata': self._ensure_safe_metadata(original_chunk.get('metadata', {}))
                }
                
                embedded_chunks.append(embedded_chunk)
            
            return embedded_chunks
            
        except Exception as e:
            logger.error(f"Failed to generate embeddings for batch starting at index {batch_start_index}: {e}")
            logger.error(f"Batch content types: {[type(chunk.get('content') if isinstance(chunk, dict) else chunk) for chunk in batch[:3]]}")
            return []
    
    def _ensure_safe_metadata(self, metadata: Dict) -> Dict:
        """
        Ensure metadata is JSON serializable (FIX: Remove bytes objects)
        """
        safe_metadata = {}
        
        for key, value in metadata.items():
            try:
                if value is None:
                    safe_metadata[key] = None
                elif isinstance(value, (str, int, float, bool)):
                    safe_metadata[key] = value
                elif isinstance(value, bytes):
                    # Convert bytes to string
                    try:
                        safe_metadata[key] = value.decode('utf-8', errors='ignore')
                    except:
                        safe_metadata[key] = str(value)
                elif isinstance(value, (list, dict)):
                    # Check if it's JSON serializable
                    try:
                        json.dumps(value)
                        safe_metadata[key] = value
                    except:
                        safe_metadata[key] = str(value)
                else:
                    # Convert everything else to string
                    safe_metadata[key] = str(value)
                    
            except Exception as e:
                logger.warning(f"Could not process metadata key {key}: {e}")
                safe_metadata[key] = str(value) if value is not None else None
        
        return safe_metadata
    
    async def generate_embedding(self, text: str) -> List[float]:
        """
        Generate single embedding (for compatibility)
        """
        try:
            if not text or not text.strip():
                logger.warning("Empty text provided for embedding")
                return []
            
            response = self.client.embeddings.create(
                model=self.model,
                input=[text.strip()]
            )
            
            return response.data[0].embedding
            
        except Exception as e:
            logger.error(f"Failed to generate single embedding: {e}")
            return []


# Global instance
embedding_generator = EmbeddingGenerator()