# main.py - FIXED SOLID AI SQL Generation v3.2.0 - Enhanced with Fixed Intelligent Query Integration
# Version Control: 
# v3.0.0 - Enhanced Table Selection: Intelligent platform detection, account name matching, plural forms support
# v3.1.0 - Integrated Intelligent Query Generation with Enhanced MCP
# v3.2.0 - FIXED: Platform matching, schema analysis, intelligent SQL generation, join detection

import os
import logging
import time
import json
import re
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Literal, List, Any, Tuple
from dotenv import load_dotenv
import asyncio

# Version tracking
VERSION = "3.2.0"
PREVIOUS_STABLE_VERSION = "3.1.0"

# Enhanced BigQuery MCP integration - FIXED VERSION
from api.enhanced_bigquery_mcp import intelligent_bigquery_mcp

# Request models
class UnifiedQueryRequest(BaseModel):
    question: str
    data_source: Literal["rag", "bigquery"] = "rag"
    preferred_style: str = "standard"

class QueryRequest(BaseModel):
    question: str
    preferred_style: Optional[str] = "standard"
    context: Optional[str] = None

class IntelligentQueryRequest(BaseModel):
    query: str
    context: Optional[Dict[str, Any]] = None
    force_discovery: Optional[bool] = False

class SQLGenerationRequest(BaseModel):
    query: str
    tables_context: Optional[Dict[str, Any]] = None
    generate_sql: bool = True

# Response models
class QueryResponse(BaseModel):
    answer: str
    query_type: str
    processing_method: str
    sources_used: int
    processing_time: float
    response_style: str

class HealthResponse(BaseModel):
    status: str
    systems: Dict[str, str]
    timestamp: str
    environment: str

class DashboardSummary(BaseModel):
    total_stores: int
    total_days: int
    total_users: int
    total_sessions: int
    total_transactions: int
    total_revenue: float
    avg_conversion_rate: float
    avg_bounce_rate: float
    total_new_users: int
    total_add_to_carts: int

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import our core modules
try:
    from api.core.qa_engine import qa_engine
    from api.core.database import db
    CORE_MODULES_AVAILABLE = True
    logger.info("Core RAG modules loaded successfully")
except ImportError as e:
    CORE_MODULES_AVAILABLE = False
    logger.error(f"Failed to load core modules: {e}")

try:
    from supabase import create_client
    import openai
    EXTERNAL_CLIENTS_AVAILABLE = True
except ImportError as e:
    EXTERNAL_CLIENTS_AVAILABLE = False
    logger.error(f"External clients not available: {e}")

# Import dashboard routes
try:
    from api.routes.dashboard import router as dashboard_router
    DASHBOARD_AVAILABLE = True
    logger.info("Dashboard routes loaded successfully")
except ImportError as e:
    DASHBOARD_AVAILABLE = False
    logger.error(f"Dashboard routes not available: {e}")

# Initialize FastAPI
app = FastAPI(
    title="Marketing Intelligence API - FIXED Enhanced Dynamic System v3.2",
    description=f"FIXED Enhanced Dynamic AI SQL Generation with Intelligent Query Generation v{VERSION}",
    version=VERSION,
    docs_url="/docs" if os.getenv("ENVIRONMENT") == "development" else None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# Include dashboard routes if available
if DASHBOARD_AVAILABLE:
    app.include_router(dashboard_router, prefix="/api", tags=["dashboard"])
    logger.info("Dashboard routes included")

# Global variables
start_time = time.time()

# Initialize external clients
openai_client = None
supabase_client = None

if EXTERNAL_CLIENTS_AVAILABLE:
    try:
        if os.getenv("OPENAI_API_KEY"):
            openai_client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            logger.info("OpenAI client initialized")
    except Exception as e:
        logger.error(f"OpenAI initialization failed: {e}")

    try:
        if os.getenv("SUPABASE_URL"):
            supabase_client = create_client(
                os.getenv("SUPABASE_URL"),
                os.getenv("SUPABASE_ANON_KEY")
            )
            logger.info("Supabase client initialized")
    except Exception as e:
        logger.error(f"Supabase initialization failed: {e}")

# ENHANCED INTELLIGENT QUERY GENERATION ENDPOINTS

@app.post("/api/intelligent/discover-tables")
async def discover_relevant_tables(request: IntelligentQueryRequest):
    """
    FIXED: Discover tables with enhanced platform matching and relevance scoring.
    
    Example queries:
    - "Microsoft Ads clicks for last 30 days" -> Should find MSAds tables
    - "Google Ads spend by campaign" -> Should find GAds tables  
    - "Facebook ad performance" -> Should find Facebook tables
    """
    try:
        logger.info(f"FIXED Discovery for query: {request.query}")
        
        # Force discovery if requested
        if request.force_discovery:
            await intelligent_bigquery_mcp.discover_and_cache_metadata(force_refresh=True)
        
        # Enhanced table discovery with fixed scoring
        discovery_result = await intelligent_bigquery_mcp.intelligent_table_discovery(request.query)
        
        return {
            "success": True,
            "query": request.query,
            "discovery": discovery_result,
            "platform_detected": discovery_result.get('query_intent', {}).get('primary_platform'),
            "confidence_scores": [(t['table_name'], t['relevance_score']) for t in discovery_result.get('relevant_tables', [])[:5]],
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"FIXED table discovery failed: {e}")
        raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)}")

@app.post("/api/intelligent/generate-context")
async def generate_query_context(request: IntelligentQueryRequest):
    """
    FIXED: Generate intelligent context with proper platform detection and SQL generation.
    """
    try:
        logger.info(f"FIXED context generation for query: {request.query}")
        
        # Generate enhanced intelligent context
        context_result = await intelligent_bigquery_mcp.generate_intelligent_sql(
            request.query, 
            request.context
        )
        
        return {
            "success": True,
            "query": request.query,
            "context": context_result,
            "selected_table": context_result.get('table_context', {}).get('available_tables', [{}])[0].get('table_name', 'none'),
            "generated_sql": context_result.get('table_context', {}).get('generated_sql', 'none'),
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"FIXED context generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Context generation failed: {str(e)}")

@app.post("/api/intelligent/smart-sql-generation")
async def smart_sql_generation(request: SQLGenerationRequest):
    """
    FIXED: Generate SQL with enhanced intelligence and proper table selection.
    """
    try:
        logger.info(f"FIXED Smart SQL generation for: {request.query}")
        
        # Step 1: Enhanced context generation
        context_result = await intelligent_bigquery_mcp.generate_intelligent_sql(request.query)
        
        if context_result["status"] == "no_relevant_tables":
            return {
                "success": False,
                "error": "No relevant tables found",
                "suggestion": context_result.get("suggestion"),
                "query": request.query,
                "platform_detected": context_result.get("query_intent", {}).get("primary_platform", "unknown")
            }
        
        # Step 2: Extract generated SQL and context
        table_context = context_result["table_context"]
        generated_sql = table_context.get("generated_sql")
        
        return {
            "success": True,
            "query": request.query,
            "enhanced_context": context_result,
            "generated_sql": generated_sql,
            "selected_table": table_context["available_tables"][0]["table_name"],
            "platform_detected": context_result.get("discovery_result", {}).get("query_intent", {}).get("primary_platform"),
            "relevance_score": table_context["available_tables"][0]["relevance_score"],
            "recommended_approach": context_result.get("recommended_approach"),
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"FIXED Smart SQL generation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Smart SQL generation failed: {str(e)}")

@app.post("/api/intelligent/test-platform-detection")
async def test_platform_detection(request: IntelligentQueryRequest):
    """
    NEW: Test platform detection accuracy for debugging.
    """
    try:
        logger.info(f"Testing platform detection for: {request.query}")
        
        # Run discovery
        discovery_result = await intelligent_bigquery_mcp.intelligent_table_discovery(request.query)
        query_intent = discovery_result.get('query_intent', {})
        relevant_tables = discovery_result.get('relevant_tables', [])
        
        # Analyze results
        analysis = {
            "query": request.query,
            "detected_platform": query_intent.get('primary_platform'),
            "platforms_found": query_intent.get('platforms_detected', {}),
            "terms_extracted": query_intent.get('terms', []),
            "metrics_requested": query_intent.get('metrics_requested', []),
            "top_3_tables": [
                {
                    "table": t['table_name'],
                    "score": t['relevance_score'],
                    "tags": t['semantic_tags']
                } for t in relevant_tables[:3]
            ],
            "correct_match": None
        }
        
        # Check if top table matches detected platform
        if relevant_tables:
            top_table_tags = relevant_tables[0]['semantic_tags']
            detected_platform = query_intent.get('primary_platform')
            analysis["correct_match"] = detected_platform in top_table_tags if detected_platform else False
        
        return {
            "success": True,
            "analysis": analysis,
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"Platform detection test failed: {e}")
        raise HTTPException(status_code=500, detail=f"Test failed: {str(e)}")

@app.get("/api/intelligent/metadata/refresh")
async def refresh_metadata():
    """Force refresh of metadata cache."""
    try:
        logger.info("FIXED: Forcing metadata refresh...")
        result = await intelligent_bigquery_mcp.discover_and_cache_metadata(force_refresh=True)
        
        # Get sample of cached tables for verification
        sample_tables = {}
        for table_name, table_info in list(intelligent_bigquery_mcp._table_cache.items())[:5]:
            sample_tables[table_name] = {
                "semantic_tags": table_info.get('semantic_tags', []),
                "has_schema": bool(table_info.get('schema'))
            }
        
        return {
            "success": True,
            "refresh_result": result,
            "sample_tables": sample_tables,
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"FIXED metadata refresh failed: {e}")
        raise HTTPException(status_code=500, detail=f"Metadata refresh failed: {str(e)}")

@app.get("/api/intelligent/metadata/status")
async def get_metadata_status():
    """Get current metadata cache status with diagnostics."""
    try:
        # Check cache status without forcing refresh
        result = await intelligent_bigquery_mcp.discover_and_cache_metadata(force_refresh=False)
        
        # Platform distribution analysis
        platform_distribution = {}
        for table_name, table_info in intelligent_bigquery_mcp._table_cache.items():
            for tag in table_info.get('semantic_tags', []):
                if tag in intelligent_bigquery_mcp._semantic_mapping:
                    platform_distribution[tag] = platform_distribution.get(tag, 0) + 1
        
        return {
            "success": True,
            "cache_status": result,
            "tables_cached": len(intelligent_bigquery_mcp._table_cache),
            "relationships_found": len(intelligent_bigquery_mcp._relationship_cache),
            "platform_distribution": platform_distribution,
            "semantic_mappings_available": len(intelligent_bigquery_mcp._semantic_mapping),
            "timestamp": datetime.now().isoformat(),
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"FIXED status check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")

# Enhanced unified query function with FIXES v3.3.0 - CORRECTED
async def unified_query_with_enhanced_intelligence(question: str, force_discovery: bool = False) -> dict:
    """
    FIXED v3.3.0: Enhanced unified query processing with proper platform detection and validation
    """
    try:
        logger.info(f"FIXED v3.3.0: Unified intelligent query for: {question}")
        
        # Step 1: Force discovery if requested
        if force_discovery:
            await intelligent_bigquery_mcp.discover_and_cache_metadata(force_refresh=True)
        
        # Step 2: Enhanced intelligent table discovery
        discovery_result = await intelligent_bigquery_mcp.intelligent_table_discovery(question)
        
        if not discovery_result.get('relevant_tables'):
            query_intent = discovery_result.get('query_intent', {})
            return {
                "answer": f"I couldn't find relevant tables for your query about {query_intent.get('primary_platform', 'data')}. The system detected intent for: {', '.join(query_intent.get('terms', []))[:100]}...",
                "error": "No relevant tables found", 
                "query_intent": query_intent,
                "processing_method": "enhanced_discovery_no_tables",
                "version": "3.3.0"
            }
        
        # Step 3: Generate enhanced SQL context
        context_result = await intelligent_bigquery_mcp.generate_intelligent_sql(question)
        
        if context_result["status"] == "no_relevant_tables":
            return {
                "answer": context_result.get("message", "No relevant tables found"),
                "suggestion": context_result.get("suggestion"),
                "processing_method": "enhanced_context_no_tables",
                "version": "3.3.0"
            }
        
        # Step 4: Extract the intelligent SQL that was generated
        table_context = context_result["table_context"]
        sql_query = table_context.get("generated_sql")
        selected_table = table_context["available_tables"][0]
        
        # Step 5: CRITICAL VALIDATION - Platform Routing Check
        table_name = selected_table['table_name'].lower()
        detected_platform = discovery_result.get('query_intent', {}).get('primary_platform')
        
        logger.info(f"VALIDATION CHECK: Detected platform: {detected_platform}, Selected table: {table_name}")
        
        # Check for Google Ads -> Microsoft Ads routing error
        if detected_platform == 'google_ads' and 'msads' in table_name:
            logger.error(f"PLATFORM ROUTING ERROR: Google Ads query routed to Microsoft Ads table: {table_name}")
            return {
                "answer": f"Platform routing error detected. Google Ads query was incorrectly routed to Microsoft Ads table ({table_name}). This indicates a semantic tagging issue in the table discovery system.",
                "error": "platform_routing_mismatch",
                "detected_platform": detected_platform,
                "selected_table": table_name,
                "processing_method": "enhanced_validation_error_google_to_ms",
                "debug_info": {
                    "query_intent": discovery_result.get('query_intent'),
                    "table_semantic_tags": selected_table.get('semantic_tags', []),
                    "relevance_score": selected_table.get('relevance_score', 0)
                },
                "version": "3.3.0"
            }

        # Check for Microsoft Ads -> Google Ads routing error  
        if detected_platform == 'microsoft_ads' and 'gads' in table_name:
            logger.error(f"PLATFORM ROUTING ERROR: Microsoft Ads query routed to Google Ads table: {table_name}")
            return {
                "answer": f"Platform routing error detected. Microsoft Ads query was incorrectly routed to Google Ads table ({table_name}). This indicates a semantic tagging issue in the table discovery system.",
                "error": "platform_routing_mismatch", 
                "detected_platform": detected_platform,
                "selected_table": table_name,
                "processing_method": "enhanced_validation_error_ms_to_google",
                "debug_info": {
                    "query_intent": discovery_result.get('query_intent'),
                    "table_semantic_tags": selected_table.get('semantic_tags', []),
                    "relevance_score": selected_table.get('relevance_score', 0)
                },
                "version": "3.3.0"
            }
        
        # Check for Facebook Ads routing errors
        if detected_platform == 'facebook_ads' and ('msads' in table_name or 'gads' in table_name):
            logger.error(f"PLATFORM ROUTING ERROR: Facebook Ads query routed to {table_name}")
            return {
                "answer": f"Platform routing error detected. Facebook Ads query was incorrectly routed to {table_name}. This indicates a semantic tagging issue.",
                "error": "platform_routing_mismatch",
                "detected_platform": detected_platform,
                "selected_table": table_name,
                "processing_method": "enhanced_validation_error_facebook",
                "debug_info": {
                    "query_intent": discovery_result.get('query_intent'),
                    "table_semantic_tags": selected_table.get('semantic_tags', []),
                    "relevance_score": selected_table.get('relevance_score', 0)
                },
                "version": "3.3.0"
            }
        
        # Step 6: Log successful platform matching
        logger.info(f"VALIDATION SUCCESS: Platform {detected_platform} correctly routed to table {table_name}")
        logger.info(f"Selected table {selected_table['table_name']} with score {selected_table['relevance_score']}")
        logger.info(f"Generated SQL: {sql_query[:200]}...")
        
        if not sql_query:
            # This shouldn't happen with the fixed version, but fallback just in case
            sql_query = f"SELECT * FROM `{selected_table['table_name']}` LIMIT 10"
        
        # Step 7: Execute SQL
        result = await intelligent_bigquery_mcp.execute_sql(sql_query)
        
        if result.get("isError") or not result.get("content"):
            return {
                "answer": f"Query execution failed: {result.get('error', 'Unknown error')}",
                "sql_query": sql_query,
                "selected_table": selected_table['table_name'],
                "processing_method": "enhanced_sql_execution_failed",
                "version": "3.3.0"
            }
        
        # Step 8: Format results with enhanced context
        formatted_answer = await format_bigquery_results_enhanced(
            result, question, sql_query, selected_table, discovery_result.get('query_intent', {})
        )
        
        return {
            "answer": formatted_answer,
            "data": result,
            "sql_query": sql_query,
            "discovery_result": discovery_result,
            "context_result": context_result,
            "table_used": selected_table["table_name"],
            "platform_detected": discovery_result.get('query_intent', {}).get('primary_platform'),
            "relevance_score": selected_table['relevance_score'],
            "processing_method": "enhanced_intelligent_end_to_end_success",
            "validation_status": "platform_routing_validated",
            "version": "3.3.0"
        }
        
    except Exception as e:
        logger.error(f"FIXED unified intelligent query failed: {e}")
        return {
            "answer": f"Enhanced Intelligent Query Generation v3.3.0 error: {str(e)}",
            "error": str(e),
            "processing_method": "enhanced_unified_error",
            "version": "3.3.0"
        }


# Enhanced format function with correct platform detection
async def format_bigquery_results_enhanced(result: dict, question: str, sql_query: str, 
                                         selected_table: dict, query_intent: dict) -> str:
    """Enhanced result formatting with ACTUAL platform detection from table name"""
    
    if not openai_client:
        return f"Here are the BigQuery results for: {question}"
    
    try:
        if not result.get('content'):
            return f"No data found for: {question}"
        
        # Extract and structure the raw data
        raw_data = []
        for item in result['content']:
            if isinstance(item, dict) and 'text' in item:
                raw_data.append(item['text'])
        
        # Combine all raw data for analysis
        data_text = '\n'.join(raw_data)
        
        # FIXED: Determine actual platform from table name, not just detected intent
        table_name = selected_table.get('table_name', 'unknown')
        relevance_score = selected_table.get('relevance_score', 0)
        
        # Determine ACTUAL platform from table name
        actual_platform = "unknown"
        if 'msads' in table_name.lower():
            actual_platform = "microsoft_ads"
        elif 'gads' in table_name.lower():
            actual_platform = "google_ads"  
        elif 'facebook' in table_name.lower():
            actual_platform = "facebook_ads"
        
        detected_platform = query_intent.get('primary_platform', 'unknown')
        
        analysis_prompt = f"""You are an expert marketing data analyst. Analyze this BigQuery data and provide insights in a clean, readable format for a chat interface.

ORIGINAL QUESTION: {question}
PLATFORM DETECTED IN QUERY: {detected_platform}
ACTUAL PLATFORM FROM TABLE: {actual_platform}
TABLE USED: {table_name} (relevance score: {relevance_score})
SQL QUERY USED: {sql_query}

RAW DATA FROM BIGQUERY:
{data_text}

FORMATTING REQUIREMENTS - VERY IMPORTANT:
1. Use ONLY plain text formatting that works in chat
2. Use line breaks for spacing between sections
3. Use simple text headers like "EXECUTIVE SUMMARY:" 
4. Use bullet points with "•" character
5. Use numbered lists with "1.", "2.", "3."
6. Put each major section on a new line with blank line before it
7. NO markdown (###, **, etc.) - just plain readable text
8. Use capital letters for emphasis instead of bold

RESPONSE STRUCTURE:
Start with executive summary mentioning the ACTUAL platform ({actual_platform}) and key findings, then break down metrics, then insights, then recommendations.

Make this professional and easy to read in a chat interface. Base your analysis on the ACTUAL platform data source."""

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": analysis_prompt}],
            max_tokens=1000,
            temperature=0.3
        )
        
        formatted_response = response.choices[0].message.content.strip()
        
        # Clean up any remaining markdown
        formatted_response = formatted_response.replace('###', '')
        formatted_response = formatted_response.replace('**', '')
        formatted_response = formatted_response.replace('##', '')
        
        # Add enhanced data source note with ACTUAL platform
        formatted_response += f"\n\n---\nAnalysis based on {len(result['content'])} records from {table_name}"
        formatted_response += f"\nActual Platform: {actual_platform.upper()} | Query Intent: {detected_platform.upper()} | Relevance Score: {relevance_score} | Enhanced Intelligence v3.3.0"
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"Enhanced response formatting error: {e}")
        return f"Retrieved {len(result.get('content', []))} records from {selected_table.get('table_name', 'BigQuery')} for: {question}"
# Legacy functions (keeping existing functionality)
def classify_query(question: str) -> Dict[str, str]:
    """Enhanced query classification"""
    question_lower = question.lower()
    
    temporal_keywords = [
        "july", "june", "may", "august", "september", "october", "november", "december",
        "q1", "q2", "q3", "q4", "quarter", "2024", "2023", "2025",
        "last month", "this month", "previous", "current"
    ]
    
    marketing_keywords = [
        "roas", "ctr", "cpc", "performance", "campaign", "ads", "spend", 
        "conversions", "revenue", "budget", "linkedin", "facebook", "google",
        "meta", "tiktok", "impressions", "clicks", "microsoft", "bing"
    ]
    
    document_keywords = [
        "responsibilities", "roles", "duties", "scorecard", "report",
        "budget mailboxes", "mailbox works", "manager", "bmb", "mw"
    ]
    
    has_temporal = any(keyword in question_lower for keyword in temporal_keywords)
    has_marketing = any(keyword in question_lower for keyword in marketing_keywords)
    has_document = any(keyword in question_lower for keyword in document_keywords)
    
    if has_temporal and (has_marketing or has_document):
        return {"type": "temporal_complex", "confidence": "0.9"}
    elif has_marketing and has_document:
        return {"type": "analytical", "confidence": "0.8"}
    elif has_document:
        return {"type": "document_specific", "confidence": "0.8"}
    elif has_marketing:
        return {"type": "marketing_general", "confidence": "0.7"}
    else:
        return {"type": "general", "confidence": "0.6"}

async def simple_supabase_search(question: str) -> Dict:
    """Simple Supabase vector search for basic queries using GPT-4o"""
    try:
        if not supabase_client or not openai_client:
            raise Exception("Required clients not initialized")
        
        embedding_response = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=question
        )
        query_embedding = embedding_response.data[0].embedding
        
        result = supabase_client.rpc(
            'match_documents',
            {
                'query_embedding': query_embedding,
                'match_threshold': 0.3,
                'match_count': 5
            }
        ).execute()
        
        documents = result.data if result.data else []
        
        if documents:
            context = "\n".join([doc['content'] for doc in documents[:3]])
            
            response = openai_client.chat.completions.create(
                model="gpt-4o",
                messages=[{
                    "role": "user", 
                    "content": f"""Based on this context from marketing documents:

{context}

Question: {question}

Provide a clear, direct answer using the information from the context. If the context doesn't fully answer the question, say so."""
                }],
                max_tokens=200,
                temperature=0.1
            )
            
            answer = response.choices[0].message.content
            processing_method = "supabase_search"
            sources_count = len(documents)
        else:
            answer = "I couldn't find specific information about that in the documents."
            processing_method = "no_results"
            sources_count = 0
        
        return {
            "answer": answer,
            "sources": sources_count,
            "method": processing_method
        }
        
    except Exception as e:
        logger.error(f"Simple search error: {e}")
        return {
            "answer": f"I encountered an error searching for that information. Please try again.",
            "sources": 0,
            "method": "error"
        }

def advanced_rag_search(question: str) -> Dict:
    """Use sophisticated Python RAG system"""
    try:
        if not CORE_MODULES_AVAILABLE:
            raise Exception("Advanced RAG system not available")
        
        result = qa_engine.answer_question(
            question=question,
            num_sources=6
        )
        
        return {
            "answer": result.get('answer', 'No answer generated'),
            "sources": len(result.get('sources', [])),
            "method": "advanced_rag",
            "processing_time": result.get('total_time', 0)
        }
        
    except Exception as e:
        logger.error(f"Advanced RAG error: {e}")
        return {
            "answer": f"I encountered an error with the advanced analysis. Please try a simpler query.",
            "sources": 0,
            "method": "error"
        }

async def format_response_by_style(answer: str, style: str, question: str) -> str:
    """Format response according to requested style using GPT-4o"""
    if not openai_client or style == "standard":
        return answer
    
    try:
        if style == "brief":
            prompt = f"Summarize this answer in one clear, concise sentence:\n\n{answer}"
            max_tokens = 50
        elif style == "detailed":
            prompt = f"""Expand this answer with more context and analysis for: "{question}"

Original answer: {answer}

Provide:
1. Direct answer
2. Supporting context
3. Relevant insights
4. Important caveats

Detailed response:"""
            max_tokens = 400
        else:
            return answer

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        logger.error(f"Response formatting error: {e}")
        return answer

# Dashboard endpoints (keeping existing functionality)
@app.get("/api/dashboard/consolidated/summary")
async def get_consolidated_summary():
    """Get high-level summary metrics from consolidated master table (Last 4 weeks)"""
    try:
        query = """
        SELECT 
            COUNT(DISTINCT online_store) as total_stores,
            COUNT(DISTINCT Date) as total_days,
            SUM(Users) as total_users,
            SUM(Sessions) as total_sessions,
            SUM(Transactions) as total_transactions,
            SUM(Revenue) as total_revenue,
            AVG(Conversion_rate) as avg_conversion_rate,
            AVG(Bounce_rate) as avg_bounce_rate,
            SUM(New_users) as total_new_users,
            SUM(Add_To_Carts) as total_add_to_carts
        FROM `data-tables-for-zoho.new_data_tables.consolidated_master`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY)
        """
        
        result = await intelligent_bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            logger.error(f"Query execution failed: {result}")
            raise Exception("Query execution failed")
        
        # Parse the result
        if result["content"] and len(result["content"]) > 0:
            try:
                data_item = result["content"][0]
                
                if isinstance(data_item, dict) and "text" in data_item:
                    row_data = json.loads(data_item["text"])
                    logger.info(f"Parsed aggregation data: {row_data}")
                    
                    summary = {
                        "total_stores": int(row_data.get("total_stores", 0)),
                        "total_days": int(row_data.get("total_days", 0)),
                        "total_users": int(row_data.get("total_users", 0)),
                        "total_sessions": int(row_data.get("total_sessions", 0)),
                        "total_transactions": int(row_data.get("total_transactions", 0)),
                        "total_revenue": float(row_data.get("total_revenue", 0) or 0),
                        "avg_conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0),
                        "avg_bounce_rate": float(row_data.get("avg_bounce_rate", 0) or 0),
                        "total_new_users": int(row_data.get("total_new_users", 0)),
                        "total_add_to_carts": int(row_data.get("total_add_to_carts", 0))
                    }
                    logger.info(f"Dashboard summary generated successfully: {summary}")
                    return summary
                    
            except json.JSONDecodeError as parse_error:
                logger.error(f"JSON parse error: {parse_error}")
            except Exception as parse_error:
                logger.error(f"Data processing error: {parse_error}")
        
        return {
            "total_stores": 0,
            "total_days": 0,
            "total_users": 0,
            "total_sessions": 0,
            "total_transactions": 0,
            "total_revenue": 0.0,
            "avg_conversion_rate": 0.0,
            "avg_bounce_rate": 0.0,
            "total_new_users": 0,
            "total_add_to_carts": 0,
            "note": "Using fallback data"
        }
            
    except Exception as e:
        logger.error(f"Dashboard summary error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Dashboard error: {str(e)}")

# Main API endpoints
@app.get("/")
async def root():
    """Root endpoint with Enhanced Intelligence v3.2 information"""
    uptime = time.time() - start_time
    
    return {
        "service": "Marketing Intelligence API - FIXED Enhanced Dynamic System v3.2",
        "version": VERSION,
        "previous_stable": PREVIOUS_STABLE_VERSION,
        "status": "enhanced_intelligent_query_v3_2_active",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "fixed_features_v3_2": {
            "platform_detection_fixed": True,
            "relevance_scoring_enhanced": True,
            "sql_generation_improved": True,
            "schema_analysis_fixed": True,
            "account_matching_enhanced": True,
            "join_detection_improved": True,
            "microsoft_ads_detection": True,
            "google_ads_detection": True,
            "facebook_ads_detection": True,
            "query_intent_analysis": True,
            "enhanced_column_mapping": True
        },
        "endpoints": {
            "main": {
                "chat": "/api/chat",
                "unified_query": "/api/unified-query",
                "health": "/api/health"
            },
            "intelligent_query": {
                "discover_tables": "/api/intelligent/discover-tables",
                "generate_context": "/api/intelligent/generate-context", 
                "smart_sql_generation": "/api/intelligent/smart-sql-generation",
                "test_platform_detection": "/api/intelligent/test-platform-detection",
                "metadata_refresh": "/api/intelligent/metadata/refresh",
                "metadata_status": "/api/intelligent/metadata/status"
            },
            "dashboard": {
                "summary": "/api/dashboard/consolidated/summary"
            }
        },
        "changelog": {
            "v3.2.0_FIXES": [
                "FIXED: Platform detection now properly matches Microsoft Ads vs Google Ads vs Facebook Ads",
                "FIXED: Relevance scoring heavily weights exact platform matches",
                "FIXED: Schema analysis properly categorizes columns by function",
                "FIXED: SQL generation uses discovered columns intelligently",
                "FIXED: Account name extraction with better pattern matching",
                "FIXED: Query intent analysis with platform confidence scoring",
                "ENHANCED: Column mapping with platform-specific metric patterns",
                "ENHANCED: Table selection logic with proper semantic scoring",
                "ENHANCED: Join detection and relationship analysis",
                "ENHANCED: Error handling and debugging capabilities"
            ]
        }
    }

@app.post("/api/chat", response_model=QueryResponse)
async def chat(request: QueryRequest):
    """Main chat endpoint with intelligent routing"""
    process_start = time.time()
    
    try:
        if not CORE_MODULES_AVAILABLE and not EXTERNAL_CLIENTS_AVAILABLE:
            raise HTTPException(
                status_code=503, 
                detail="RAG system not available. Check server configuration."
            )
        
        logger.info(f"Processing query: {request.question[:100]}...")
        
        classification = classify_query(request.question)
        query_type = classification["type"]
        
        if query_type in ["temporal_complex", "analytical"] and CORE_MODULES_AVAILABLE:
            result = advanced_rag_search(request.question)
        else:
            result = await simple_supabase_search(request.question)
        
        formatted_answer = await format_response_by_style(
            result["answer"],
            request.preferred_style,
            request.question
        )
        
        processing_time = time.time() - process_start
        
        return QueryResponse(
            answer=formatted_answer,
            query_type=query_type,
            processing_method=result.get("method", "unknown"),
            sources_used=result.get("sources", 0),
            processing_time=processing_time,
            response_style=request.preferred_style
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat endpoint error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/api/unified-query")
async def unified_query(request: UnifiedQueryRequest):
    """Enhanced unified endpoint with FIXED Intelligent Query Generation v3.2"""
    process_start = time.time()
    
    try:
        if request.data_source == "rag":
            # RAG processing (existing functionality)
            if not CORE_MODULES_AVAILABLE and not EXTERNAL_CLIENTS_AVAILABLE:
                raise HTTPException(status_code=503, detail="RAG system not available")
            
            classification = classify_query(request.question)
            query_type = classification["type"]
            
            if query_type in ["temporal_complex", "analytical"] and CORE_MODULES_AVAILABLE:
                result = advanced_rag_search(request.question)
            else:
                result = await simple_supabase_search(request.question)
            
            formatted_answer = await format_response_by_style(
                result["answer"], request.preferred_style, request.question
            )
            
            processing_time = time.time() - process_start
            
            return {
                "answer": formatted_answer,
                "query_type": query_type,
                "processing_method": result.get("method", "unknown"),
                "sources_used": result.get("sources", 0),
                "processing_time": processing_time,
                "response_style": request.preferred_style,
                "data_source": "rag",
                "version": VERSION
            }
            
        elif request.data_source == "bigquery":
            # FIXED: Enhanced Intelligent Query Generation approach
            logger.info(f"Using FIXED Enhanced Intelligent Query Generation v{VERSION} for: {request.question}")
            
            result = await unified_query_with_enhanced_intelligence(request.question, force_discovery=False)
            
            processing_time = time.time() - process_start
            result["processing_time"] = processing_time
            result["response_style"] = request.preferred_style
            result["data_source"] = "bigquery"
            
            return result
            
    except Exception as e:
        processing_time = time.time() - process_start
        logger.error(f"FIXED Unified query v{VERSION} error: {e}")
        return {
            "answer": f"Enhanced Intelligent Query Generation v{VERSION} error: {str(e)}",
            "query_type": "enhanced_intelligent_v3_2_error",
            "processing_method": "error_handling",
            "sources_used": 0,
            "processing_time": processing_time,
            "response_style": request.preferred_style,
            "data_source": request.data_source,
            "error_details": str(e),
            "version": VERSION
        }

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check including FIXED Enhanced Intelligence v3.2"""
    systems = {}
    
    systems["enhanced_intelligent_query_version"] = VERSION
    systems["platform_detection"] = "FIXED - Microsoft/Google/Facebook Ads"
    systems["relevance_scoring"] = "FIXED - Enhanced with platform weighting"
    systems["schema_analysis"] = "FIXED - Proper column categorization"
    systems["sql_generation"] = "FIXED - Intelligent with discovered columns"
    systems["account_matching"] = "FIXED - Enhanced pattern recognition"
    systems["join_detection"] = "FIXED - Relationship analysis improved"
    
    systems["core_modules"] = "available" if CORE_MODULES_AVAILABLE else "unavailable"
    systems["dashboard_routes"] = "available" if DASHBOARD_AVAILABLE else "unavailable"
    
    systems["openai_key"] = "configured" if os.getenv("OPENAI_API_KEY") else "missing"
    systems["supabase_url"] = "configured" if os.getenv("SUPABASE_URL") else "missing"
    systems["supabase_key"] = "configured" if os.getenv("SUPABASE_ANON_KEY") else "missing"
    
    systems["openai_client"] = "available (gpt-4o)" if openai_client else "unavailable"
    systems["supabase_client"] = "available" if supabase_client else "unavailable"
    
    # Test enhanced intelligent discovery
    try:
        result = await intelligent_bigquery_mcp.discover_and_cache_metadata(force_refresh=False)
        tables_count = result.get('tables_discovered', len(intelligent_bigquery_mcp._table_cache))
        
        # Count platform tables
        platform_counts = {}
        for table_name, table_info in intelligent_bigquery_mcp._table_cache.items():
            for tag in table_info.get('semantic_tags', []):
                if tag in intelligent_bigquery_mcp._semantic_mapping:
                    platform_counts[tag] = platform_counts.get(tag, 0) + 1
        
        systems["enhanced_discovery"] = f"healthy ({tables_count} tables, platforms: {dict(list(platform_counts.items())[:3])})"
    except Exception as e:
        systems["enhanced_discovery"] = f"error: {str(e)[:50]}"
    
    critical_systems = ["openai_key", "supabase_url", "enhanced_discovery"]
    healthy_systems = sum(1 for sys in critical_systems 
                         if "configured" in str(systems.get(sys, "")) or "healthy" in str(systems.get(sys, "")))
    
    if healthy_systems >= 2:
        overall_status = "enhanced_intelligent_v3_2_healthy"
    elif healthy_systems >= 1:
        overall_status = "enhanced_intelligent_v3_2_degraded"
    else:
        overall_status = "enhanced_intelligent_v3_2_offline"
    
    return HealthResponse(
        status=overall_status,
        systems=systems,
        timestamp=datetime.now().isoformat(),
        environment=os.getenv("ENVIRONMENT", "unknown")
    )

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on app shutdown"""
    await intelligent_bigquery_mcp.close()

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting FIXED Enhanced Intelligent Query Generation System v{VERSION}")
    logger.info(f"Server: {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"FIXED Features: Platform Detection + Enhanced Relevance + Intelligent SQL + Schema Analysis + GPT-4o")
    logger.info(f"BigQuery MCP: {intelligent_bigquery_mcp.server_url}")
    logger.info(f"FIXES in v{VERSION}: Microsoft Ads detection, relevance scoring, column mapping, SQL generation")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )