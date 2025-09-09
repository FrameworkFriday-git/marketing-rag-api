"""
Complete Q&A engine with retrieval and generation - FIXED VERSION
"""
import logging
from typing import List, Dict, Any, Optional  # FIXED: Added missing imports
from datetime import datetime
import asyncio
import openai

import os
import logging
from dotenv import load_dotenv
from .database import db
from .embedding_generator import embedding_generator

load_dotenv()
logger = logging.getLogger(__name__)

# Create a simple settings class
class Settings:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

settings = Settings()

class QAEngine:
    """
    Complete Q&A system with retrieval-augmented generation
    """
    
    def __init__(self):
        self.client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        self.chat_model = "gpt-4o-mini"  
        self._query_cache = {}  
        self.max_cache_size = 100
    
    def answer_question(  
        self, 
        question: str, 
        num_sources: int = 8,
        similarity_threshold: float = 0.3  # Lowered threshold for better results
    ) -> Dict[str, Any]:
        """
        Complete Q&A pipeline: retrieve relevant documents and generate answer
        
        Args:
            question: User question
            num_sources: Number of source documents to retrieve
            similarity_threshold: Minimum similarity score for relevance
            
        Returns:
            Dictionary containing answer, sources, and metadata
        """
        logger.info(f"Processing question: {question[:100]}...")
        
        # Check cache first
        cache_key = f"{question}:{num_sources}:{similarity_threshold}"
        if cache_key in self._query_cache:
            logger.debug("Returning cached result")
            cached_result = self._query_cache[cache_key].copy()
            cached_result['cached'] = True
            return cached_result
        
        try:
            # Step 1: Generate query embedding
            start_time = datetime.now()
            query_embedding = self.generate_query_embedding(question)  # FIXED: Use self method
            
            if query_embedding is None or len(query_embedding) == 0:
                return self._create_error_response(
                    question, "Failed to generate query embedding"
                )
            
            # Step 2: Retrieve relevant documents (PASS question as query_text for date-aware boosting)
            relevant_docs = db.search_documents(
                query_embedding=query_embedding,
                query_text=question,
                limit=num_sources,
                similarity_threshold=similarity_threshold
            )
            
            retrieval_time = (datetime.now() - start_time).total_seconds()
            
            # Step 3: Generate answer
            generation_start = datetime.now()
            answer = self._generate_answer(question, relevant_docs)
            generation_time = (datetime.now() - generation_start).total_seconds()
            
            # Step 4: Prepare result
            result = {
                'question': question,
                'answer': answer,
                'sources': relevant_docs,
                'num_sources': len(relevant_docs),
                'retrieval_time': retrieval_time,
                'generation_time': generation_time,
                'total_time': (datetime.now() - start_time).total_seconds(),
                'similarity_threshold': similarity_threshold,
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'cached': False
            }
            
            # Cache result (with size limit)
            self._cache_result(cache_key, result)
            
            logger.info(f"Question processed successfully in {result['total_time']:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"QA pipeline failed: {e}")
            return self._create_error_response(question, str(e))
    
    def generate_query_embedding(self, question: str) -> List[float]:
        """
        FIXED: Added missing method to generate query embedding
        """
        try:
            # Use the existing embedding generator for single text
            response = self.client.embeddings.create(
                model="text-embedding-3-small",
                input=[question.strip()]
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to generate query embedding: {e}")
            return []
    
    def _generate_answer(self, question: str, context_docs: List[Dict[str, Any]]) -> str:
        """
        Generate answer using OpenAI with retrieved context
        
        Args:
            question: User question
            context_docs: Retrieved relevant documents
            
        Returns:
            Generated answer
        """
        if not context_docs:
            return (
                "I couldn't find relevant information in the knowledge base to answer your question. "
                "This might be because:\n"
                "• The information isn't available in the current documents\n"
                "• The question requires information from sources not yet processed\n"
                "• The similarity threshold was too high\n\n"
                "Try rephrasing your question or asking about topics covered in the marketing documents."
            )
        
        # Prepare context from retrieved documents
        context_parts = []
        for i, doc in enumerate(context_docs, 1):
            metadata = doc.get('metadata', {})
            file_name = metadata.get('file_name', f'Document {i}')
            similarity = doc.get('similarity', 0)
            
            # Add source information
            source_info = f"[Source {i}: {file_name} (Relevance: {similarity:.1%})]"
            content = doc['content']
            
            context_parts.append(f"{source_info}\n{content}")
        
        context = "\n\n---\n\n".join(context_parts)
        
        # System prompt optimized for paid media marketing
        system_prompt = """You are a specialized AI assistant for paid media marketing analysis.

Your expertise includes:
- Campaign performance analysis (CTR, CPC, ROAS, CPM, CPV)
- Budget allocation and optimization strategies
- Channel performance comparison (Google Ads, Facebook, LinkedIn, etc.)
- Keyword research and audience insights
- Creative performance and A/B testing analysis
- Attribution modeling and conversion tracking
- Competitive analysis and market insights

Instructions:
1. Provide specific, data-driven answers using ONLY the context provided
2. Reference actual numbers, metrics, and KPIs when available in the context
3. Cite source documents when mentioning specific data
4. Structure your response with clear headings and bullet points when appropriate
5. Suggest actionable insights and recommendations based on the data
6. If the context lacks sufficient information, clearly state what's missing
7. Focus on paid media metrics and performance indicators
8. Always prioritize accuracy over completeness

Format Guidelines:
- Use bullet points for lists and key findings
- Include specific metrics and percentages when available
- Organize information logically (overview, key findings, recommendations)
- Keep responses concise but comprehensive"""

        try:
            response = self.client.chat.completions.create(
                model=self.chat_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {
                        "role": "user", 
                        "content": f"Context from marketing documents:\n\n{context}\n\n"
                                 f"Question: {question}\n\n"
                                 f"Please provide a detailed answer based on the context above."
                    }
                ],
               max_tokens=1000
            )
            
            answer = response.choices[0].message.content
            logger.debug("Answer generated successfully")
            return answer
            
        except Exception as e:
            logger.error(f"Answer generation failed: {e}")
            return (
                f"I found relevant information but encountered an error while generating the answer: {str(e)}. "
                "Please try asking your question again."
            )
    
    def _create_error_response(self, question: str, error_message: str) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            'question': question,
            'answer': f"Sorry, I encountered an error: {error_message}",
            'sources': [],
            'num_sources': 0,
            'status': 'error',
            'error': error_message,
            'timestamp': datetime.now().isoformat(),
            'cached': False
        }
    
    def _cache_result(self, cache_key: str, result: Dict[str, Any]) -> None:
        """Cache result with size management"""
        if len(self._query_cache) >= self.max_cache_size:
            # Remove oldest entry
            oldest_key = next(iter(self._query_cache))
            del self._query_cache[oldest_key]
        
        self._query_cache[cache_key] = result.copy()
        logger.debug(f"Cached result for query: {cache_key[:50]}...")
    
    def clear_cache(self) -> None:
        """Clear the query cache"""
        self._query_cache.clear()
        logger.info("Query cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            'cache_size': len(self._query_cache),
            'max_cache_size': self.max_cache_size,
            'cache_keys': list(self._query_cache.keys())[:5]  # First 5 keys for debugging
        }

# Global QA engine instance
qa_engine = QAEngine()