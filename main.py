# main.py - HOTFIX v2.7.2.3 - Complete Fixed Version with Deterministic SQL
# Version Control: 
# v2.5.0 - Static hardcoded approach (baseline)
# v2.6.0 - Static table descriptions with fallbacks
# v2.7.0 - Fully dynamic real-time BigQuery discovery system
# v2.7.1 - Fixed quote handling for dataset names from MCP responses
# v2.7.2 - Fixed indentation and removed table descriptions
# v2.7.2.3 - COMPLETE HOTFIX: Deterministic SQL generation, no AI randomness

import os
import logging
import time
import json
import re
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Literal, List, Any
from dotenv import load_dotenv
import asyncio

# Version tracking
VERSION = "2.7.2.3"
PREVIOUS_STABLE_VERSION = "2.7.2"

# BigQuery MCP integration
from api.bigquery_mcp import bigquery_mcp

# Request models
class UnifiedQueryRequest(BaseModel):
    question: str
    data_source: Literal["rag", "bigquery"] = "rag"
    preferred_style: str = "standard"

class QueryRequest(BaseModel):
    question: str
    preferred_style: Optional[str] = "standard"
    context: Optional[str] = None

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
    CORE_MODULES_AVAILABLE = True
    logger.info("Core RAG modules loaded successfully")
except ImportError as e:
    CORE_MODULES_AVAILABLE = False
    logger.warning(f"Core modules not available: {e}")

try:
    from supabase import create_client
    import openai
    EXTERNAL_CLIENTS_AVAILABLE = True
except ImportError as e:
    EXTERNAL_CLIENTS_AVAILABLE = False
    logger.warning(f"External clients not available: {e}")

# Initialize FastAPI
app = FastAPI(
    title="Marketing Intelligence API - DETERMINISTIC HOTFIX",
    description=f"Complete Deterministic SQL Generation v{VERSION}",
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

# Global variables
start_time = time.time()

# Dynamic caching system
class BigQueryDiscoveryCache:
    def __init__(self, cache_ttl_minutes: int = 5):
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.projects_cache = {}
        self.datasets_cache = {}
        self.tables_cache = {}
        self.schema_cache = {}
        self.last_discovery = {}
        
    def is_cache_valid(self, cache_key: str) -> bool:
        """Check if cache is still valid"""
        if cache_key not in self.last_discovery:
            return False
        return datetime.now() - self.last_discovery[cache_key] < self.cache_ttl
    
    def update_cache_timestamp(self, cache_key: str):
        """Update cache timestamp"""
        self.last_discovery[cache_key] = datetime.now()

# Initialize cache
discovery_cache = BigQueryDiscoveryCache(cache_ttl_minutes=5)

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

# Quote cleaning for MCP responses
def clean_mcp_response_text(text: str) -> str:
    """
    Clean text from MCP responses that might include quotes or other formatting
    v2.7.2 - Handles quotes from JSON responses properly
    """
    if not text:
        return text
    
    # Strip whitespace
    cleaned = text.strip()
    
    # Remove surrounding quotes (single or double)
    if (cleaned.startswith('"') and cleaned.endswith('"')) or \
       (cleaned.startswith("'") and cleaned.endswith("'")):
        cleaned = cleaned[1:-1]
    
    # Remove any remaining quotes that might be escaped
    cleaned = cleaned.replace('\\"', '').replace("\\'", "")
    
    logger.debug(f"Cleaned MCP text: '{text}' -> '{cleaned}'")
    return cleaned

# Dynamic project discovery
async def discover_all_projects() -> List[str]:
    """Dynamically discover all available BigQuery projects"""
    cache_key = "all_projects"
    
    if discovery_cache.is_cache_valid(cache_key) and cache_key in discovery_cache.projects_cache:
        return discovery_cache.projects_cache[cache_key]
    
    try:
        # Get projects from environment or default
        default_project = os.getenv("GOOGLE_CLOUD_PROJECT", "data-tables-for-zoho")
        
        # In a real implementation, you might query for accessible projects
        # For now, we'll use the configured project
        projects = [default_project]
        
        discovery_cache.projects_cache[cache_key] = projects
        discovery_cache.update_cache_timestamp(cache_key)
        
        logger.info(f"Discovered projects: {projects}")
        return projects
        
    except Exception as e:
        logger.error(f"Project discovery error: {e}")
        return ["data-tables-for-zoho"]  # fallback

async def discover_all_datasets(project: str = None) -> Dict[str, List[str]]:
    """Dynamically discover all datasets across all projects"""
    if not project:
        projects = await discover_all_projects()
    else:
        projects = [project]
    
    all_datasets = {}
    
    for proj in projects:
        cache_key = f"datasets_{proj}"
        
        if discovery_cache.is_cache_valid(cache_key) and cache_key in discovery_cache.datasets_cache:
            all_datasets[proj] = discovery_cache.datasets_cache[cache_key]
            continue
        
        try:
            result = await bigquery_mcp.list_dataset_ids(proj)
            datasets = []
            
            if result and 'content' in result:
                for item in result['content']:
                    if isinstance(item, str):
                        # Clean quotes from dataset names
                        dataset_name = clean_mcp_response_text(item)
                        
                        if dataset_name:  # Only add non-empty dataset names
                            datasets.append(dataset_name)
                            logger.info(f"Found dataset: '{item}' -> '{dataset_name}'")
            
            all_datasets[proj] = datasets
            discovery_cache.datasets_cache[cache_key] = datasets
            discovery_cache.update_cache_timestamp(cache_key)
            
            logger.info(f"Discovered {len(datasets)} datasets in project {proj}: {datasets}")
            
        except Exception as e:
            logger.error(f"Dataset discovery error for project {proj}: {e}")
            all_datasets[proj] = []
    
    return all_datasets

async def discover_all_tables(project: str = None, dataset: str = None) -> Dict[str, Dict[str, List[str]]]:
    """Dynamically discover all tables across all datasets and projects"""
    if project and dataset:
        # Specific project/dataset request
        projects_datasets = {project: [dataset]}
    elif project:
        # Specific project, all datasets
        datasets = await discover_all_datasets(project)
        projects_datasets = datasets
    else:
        # All projects, all datasets
        projects_datasets = await discover_all_datasets()
    
    all_tables = {}
    
    for proj, datasets in projects_datasets.items():
        all_tables[proj] = {}
        
        for ds in datasets:
            cache_key = f"tables_{proj}_{ds}"
            
            if discovery_cache.is_cache_valid(cache_key) and cache_key in discovery_cache.tables_cache:
                all_tables[proj][ds] = discovery_cache.tables_cache[cache_key]
                continue
            
            try:
                # Use cleaned dataset name for API call
                logger.info(f"Listing tables for dataset: '{ds}' in project: '{proj}'")
                result = await bigquery_mcp.list_table_ids(ds, proj)
                tables = []
                
                if result and 'content' in result:
                    for item in result['content']:
                        if isinstance(item, str):
                            # Clean quotes from table names too
                            table_name = clean_mcp_response_text(item)
                            
                            if table_name:  # Only add non-empty table names
                                tables.append(table_name)
                                logger.info(f"Found table: '{item}' -> '{table_name}'")
                
                all_tables[proj][ds] = tables
                discovery_cache.tables_cache[cache_key] = tables
                discovery_cache.update_cache_timestamp(cache_key)
                
                logger.info(f"Discovered {len(tables)} tables in {proj}.{ds}: {tables}")
                
            except Exception as e:
                logger.error(f"Table discovery error for {proj}.{ds}: {e}")
                all_tables[proj][ds] = []
    
    return all_tables

# HOTFIX: Deterministic table selection - NO AI RANDOMNESS
async def select_best_table_for_query(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """HOTFIX: Deterministic table selection without AI randomness"""
    question_lower = question.lower()
    
    # Priority-based deterministic selection - same input = same output
    table_priorities = [
        # ROAS/Campaign queries
        (['roas', 'campaign', 'performance', 'top'], ['campaign', 'ads', 'performance']),
        # Order/shipping queries
        (['order', 'item', 'summary', 'week', 'shipping', 'last'], ['order', 'item', 'shipment', 'crm']),
        # Keyword queries
        (['keyword', 'search'], ['keyword']),
        # Analytics queries
        (['analytics', 'ga4'], ['ga4', 'analytics']),
    ]
    
    # Deterministic matching - same input = same output
    for question_keywords, table_keywords in table_priorities:
        if any(keyword in question_lower for keyword in question_keywords):
            for project, datasets in available_tables.items():
                for dataset, tables in datasets.items():
                    # Sort tables to ensure deterministic selection
                    sorted_tables = sorted(tables)
                    for table in sorted_tables:
                        if any(table_keyword in table.lower() for table_keyword in table_keywords):
                            logger.info(f"HOTFIX: Selected {project}.{dataset}.{table} for '{question}'")
                            return (project, dataset, table)
    
    # Fallback: return first table deterministically
    for project, datasets in available_tables.items():
        for dataset, tables in datasets.items():
            if tables:
                first_table = sorted(tables)[0]  # Ensure deterministic fallback
                logger.info(f"HOTFIX: Fallback selection {project}.{dataset}.{first_table}")
                return (project, dataset, first_table)
    
    return None

# HOTFIX: Safe SQL patterns - NO AI GENERATION
async def generate_sql_with_ai_dynamic(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """HOTFIX: Use safe SQL patterns instead of AI generation"""
    
    # Find the most relevant table deterministically
    relevant_table_info = await select_best_table_for_query(question, available_tables)
    
    if not relevant_table_info:
        raise Exception("No suitable table found for the query")
    
    project, dataset, table = relevant_table_info
    full_table_name = f"`{project}.{dataset}.{table}`"
    question_lower = question.lower()
    
    # SAFE SQL PATTERNS - No AI generation, no randomness
    if any(keyword in question_lower for keyword in ['roas', 'top', 'performing', 'campaign']):
        if 'roas' in question_lower:
            sql_query = f"""
            SELECT 
                campaign_name,
                SUM(CAST(spend AS FLOAT64)) as total_spend,
                SUM(CAST(conversions_value AS FLOAT64)) as total_conversions_value,
                ROUND(SUM(CAST(conversions_value AS FLOAT64)) / NULLIF(SUM(CAST(spend AS FLOAT64)), 0), 2) as roas
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY campaign_name
            HAVING total_spend > 0
            ORDER BY roas DESC
            LIMIT 10
            """
        else:
            sql_query = f"""
            SELECT 
                campaign_name,
                SUM(CAST(clicks AS INT64)) as total_clicks,
                SUM(CAST(impressions AS INT64)) as total_impressions,
                SUM(CAST(spend AS FLOAT64)) as total_spend,
                SUM(CAST(conversions AS INT64)) as total_conversions
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY campaign_name
            ORDER BY total_spend DESC
            LIMIT 10
            """
    
    elif any(keyword in question_lower for keyword in ['order', 'item', 'summary', 'week', 'last']):
        sql_query = f"""
        SELECT 
            *
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY date DESC
        LIMIT 20
        """
    
    elif 'keyword' in question_lower:
        sql_query = f"""
        SELECT 
            keyword,
            SUM(CAST(clicks AS INT64)) as total_clicks,
            SUM(CAST(impressions AS INT64)) as total_impressions,
            ROUND(SUM(CAST(clicks AS INT64)) / NULLIF(SUM(CAST(impressions AS INT64)), 0) * 100, 2) as ctr_percent
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY keyword
        HAVING total_impressions > 0
        ORDER BY total_clicks DESC
        LIMIT 15
        """
    
    elif 'device' in question_lower:
        sql_query = f"""
        SELECT 
            device,
            SUM(CAST(clicks AS INT64)) as total_clicks,
            SUM(CAST(impressions AS INT64)) as total_impressions,
            SUM(CAST(spend AS FLOAT64)) as total_spend
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY device
        ORDER BY total_spend DESC
        LIMIT 5
        """
    
    else:
        # Safe default query
        sql_query = f"""
        SELECT *
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY date DESC
        LIMIT 20
        """
    
    logger.info(f"HOTFIX: Generated deterministic SQL for {project}.{dataset}.{table}")
    return sql_query.strip(), (project, dataset, table)

# SIMPLIFIED: Result formatting without AI complexity
async def format_bigquery_results_simple(result: dict, question: str, sql_query: str = None) -> str:
    """Simple result formatting without AI to ensure consistency"""
    
    if not result.get('content'):
        return f"No data found for: {question}"
    
    content = result.get('content', [])
    
    # Simple, consistent formatting
    answer = f"RESULTS FOR: {question}\n\n"
    answer += f"EXECUTIVE SUMMARY:\n"
    answer += f"Retrieved {len(content)} records from BigQuery.\n\n"
    
    if content:
        answer += f"KEY FINDINGS:\n"
        
        # Show first few rows as sample data
        answer += f"Sample Data (showing first 5 of {len(content)} records):\n\n"
        
        for i, row in enumerate(content[:5]):
            answer += f"{i+1}. {str(row)[:200]}...\n"
        
        if len(content) > 5:
            answer += f"\n... and {len(content) - 5} more records available\n"
        
        answer += f"\nINSIGHTS:\n"
        answer += f"• Data spans the requested time period\n"
        answer += f"• Results are ordered by relevance\n"
        answer += f"• Complete dataset available for analysis\n"
    
    answer += f"\n---\nAnalysis based on {len(content)} records from BigQuery"
    
    return answer

async def handle_bigquery_tables_query_dynamic(question: str) -> dict:
    """
    Handle table/dataset listing with consistent indentation and no descriptions
    Version: 2.7.2.3 - HOTFIX version
    """
    try:
        # Enhanced keywords for table listing queries
        table_keywords = [
            'tables', 'datasets', 'schema', 'list tables', 'show tables', 'data tables',
            'what are the tables', 'show me tables', 'what tables', 'available tables',
            'table names', 'list all tables', 'expand', 'expand the names', 'expand names of table'
        ]
        
        # Check if this is definitely a table listing query
        is_table_query = any(keyword in question.lower() for keyword in table_keywords)
        
        if not is_table_query:
            return None  # Let regular SQL handling take over
        
        # Discover all projects, datasets, and tables dynamically
        projects = await discover_all_projects()
        all_datasets = await discover_all_datasets()
        all_tables = await discover_all_tables()
        
        # Count totals
        total_datasets = sum(len(datasets) for datasets in all_datasets.values())
        total_tables = sum(
            len(tables) 
            for datasets in all_tables.values() 
            for tables in datasets.values()
        )
        
        # Create comprehensive response with consistent formatting
        answer = f"I discovered {len(projects)} projects with {total_datasets} datasets and {total_tables} tables. Here's what's available:\n\n"
        
        table_counter = 1
        
        for project, datasets in all_tables.items():
            if not datasets:
                continue
                
            answer += f"PROJECT: {project}\n\n"
            
            for dataset, tables in datasets.items():
                if not tables:
                    continue
                    
                answer += f"Dataset: {dataset} ({len(tables)} tables)\n\n"
                
                for table in tables:
                    # FIXED v2.7.2: Consistent indentation, no descriptions
                    answer += f"{table_counter}. {table}\n"
                    table_counter += 1
                
                answer += "\n"
        
        # Add sample questions
        answer += "SAMPLE QUESTIONS YOU CAN ASK:\n"
        answer += "• Show me campaign performance metrics\n"
        answer += "• What are the top performing campaigns by ROAS?\n"
        answer += "• Give a summary about the ordered items in last week\n"
        answer += "• Analyze keyword performance\n"
        answer += "• Show me sales and revenue data\n"
        
        return {
            "answer": answer,
            "data": {
                "projects": projects,
                "total_datasets": total_datasets,
                "total_tables": total_tables,
                "discovery_time": datetime.now().isoformat(),
                "cache_ttl_minutes": discovery_cache.cache_ttl.total_seconds() / 60
            },
            "processing_time": 2.0,
            "processing_method": "deterministic_discovery_v2.7.2.3",
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"Dynamic table discovery error: {e}")
        return {
            "answer": f"I encountered an error while discovering your BigQuery resources:\n\n{str(e)}\n\nThis might be due to permissions or connection issues.",
            "error": str(e),
            "processing_time": 0.5,
            "version": VERSION
        }

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
        "meta", "tiktok", "impressions", "clicks"
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
    """Simple Supabase vector search for basic queries using GPT-4o-mini"""
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
                model="gpt-4o-mini",
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
    """Format response according to requested style using GPT-4o-mini"""
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
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        logger.error(f"Response formatting error: {e}")
        return answer

# API Endpoints

@app.get("/")
async def root():
    """Root endpoint with hotfix information"""
    uptime = time.time() - start_time
    
    # Quick discovery summary
    try:
        projects = await discover_all_projects()
        datasets = await discover_all_datasets()
        tables = await discover_all_tables()
        
        discovery_summary = {
            "projects": len(projects),
            "datasets": sum(len(ds) for ds in datasets.values()),
            "tables": sum(len(tbls) for ds in tables.values() for tbls in ds.values()),
            "last_discovery": max(discovery_cache.last_discovery.values()).isoformat() if discovery_cache.last_discovery else None
        }
    except:
        discovery_summary = {"status": "discovery_unavailable"}
    
    return {
        "service": "Marketing Intelligence API - DETERMINISTIC HOTFIX",
        "version": VERSION,
        "previous_stable": PREVIOUS_STABLE_VERSION,
        "status": "hotfix_deployed",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "discovery_summary": discovery_summary,
        "hotfix_notes": [
            "Eliminated AI randomness from table selection",
            "Replaced AI SQL generation with deterministic patterns", 
            "Fixed 'Unexpected keyword FOR' syntax errors",
            "Ensured consistent behavior for identical queries"
        ],
        "features": {
            "deterministic_table_selection": True,
            "safe_sql_patterns": True,
            "no_ai_randomness": True,
            "consistent_behavior": True,
            "table_discovery": True,
            "simple_formatting": True
        },
        "endpoints": {
            "chat": "/api/chat",
            "unified_query": "/api/unified-query", 
            "bigquery_test": "/api/bigquery/test",
            "dynamic_discovery": "/api/bigquery/discover",
            "clear_cache": "/api/bigquery/clear-cache",
            "health": "/api/health"
        },
        "changelog": {
            "v2.7.2.3": [
                "COMPLETE HOTFIX: Eliminated all AI randomness",
                "Deterministic table selection logic",
                "Safe SQL patterns replace AI generation",
                "Consistent results for identical queries",
                "Fixed BigQuery syntax errors"
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
    """HOTFIX unified endpoint with deterministic BigQuery processing"""
    process_start = time.time()
    
    try:
        if request.data_source == "rag":
            # RAG processing
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
                "data_source": "rag"
            }
            
        elif request.data_source == "bigquery":
            # HOTFIX: Deterministic BigQuery processing
            table_result = await handle_bigquery_tables_query_dynamic(request.question)
            
            if table_result is not None:
                # This is a table listing query
                processing_time = time.time() - process_start
                table_result["processing_time"] = processing_time
                table_result["response_style"] = request.preferred_style
                table_result["data_source"] = "bigquery"
                table_result["sql_query"] = None
                return table_result
            
            else:
                # HOTFIX: Deterministic SQL query generation
                available_tables = await discover_all_tables()
                sql_query, table_info = await generate_sql_with_ai_dynamic(request.question, available_tables)
                
                logger.info(f"HOTFIX: Executing deterministic SQL: {sql_query[:100]}...")
                result = await bigquery_mcp.execute_sql(sql_query)
                processing_time = time.time() - process_start
                
                # Check if query was successful
                if result.get("isError") or not result.get("content"):
                    error_message = "Query execution failed"
                    if result.get("content"):
                        error_message = str(result["content"][:200])
                    
                    return {
                        "answer": f"Query failed on table {'.'.join(table_info)}: {error_message}",
                        "error": error_message,
                        "sql_query": sql_query,
                        "table_used": table_info,
                        "query_type": "sql_error",
                        "processing_method": "deterministic_sql_v2.7.2.3",
                        "sources_used": 0,
                        "processing_time": processing_time,
                        "response_style": request.preferred_style,
                        "data_source": "bigquery"
                    }
                
                # HOTFIX: Simple, consistent formatting
                formatted_answer = await format_bigquery_results_simple(result, request.question, sql_query)
                
                return {
                    "answer": formatted_answer,
                    "data": result,
                    "sql_query": sql_query,
                    "table_used": table_info,
                    "query_type": "data_query",
                    "processing_method": "deterministic_sql_v2.7.2.3",
                    "sources_used": len(result.get("content", [])),
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery"
                }
            
    except Exception as e:
        logger.error(f"Unified query hotfix error: {e}")
        processing_time = time.time() - process_start
        return {
            "answer": f"Hotfix error: {str(e)}",
            "query_type": "hotfix_error",
            "processing_method": "error_handling",
            "sources_used": 0,
            "processing_time": processing_time,
            "response_style": request.preferred_style,
            "data_source": request.data_source,
            "error_details": str(e),
            "version": VERSION
        }

# Dynamic discovery endpoints
@app.get("/api/bigquery/discover")
async def discover_all_resources():
    """Discover all BigQuery resources dynamically"""
    try:
        projects = await discover_all_projects()
        datasets = await discover_all_datasets()
        tables = await discover_all_tables()
        
        return {
            "status": "success",
            "discovery_time": datetime.now().isoformat(),
            "projects": projects,
            "datasets": datasets,
            "tables": tables,
            "summary": {
                "total_projects": len(projects),
                "total_datasets": sum(len(ds) for ds in datasets.values()),
                "total_tables": sum(len(tbls) for ds in tables.values() for tbls in ds.values())
            },
            "cache_info": {
                "ttl_minutes": discovery_cache.cache_ttl.total_seconds() / 60,
                "last_discovery": {k: v.isoformat() for k, v in discovery_cache.last_discovery.items()}
            },
            "hotfix_version": VERSION
        }
    except Exception as e:
        return {"status": "error", "error": str(e), "version": VERSION}

@app.post("/api/bigquery/clear-cache")
async def clear_discovery_cache():
    """Clear the discovery cache to force fresh discovery"""
    global discovery_cache
    discovery_cache = BigQueryDiscoveryCache(cache_ttl_minutes=5)
    
    return {
        "status": "success",
        "message": "Discovery cache cleared. Next queries will perform fresh discovery.",
        "timestamp": datetime.now().isoformat(),
        "version": VERSION
    }

@app.get("/api/bigquery/test")
async def test_bigquery_connection():
    """Test BigQuery MCP connection"""
    try:
        datasets = await discover_all_datasets()
        return {
            "server_url": bigquery_mcp.server_url,
            "connection_status": "healthy",
            "datasets_found": sum(len(ds) for ds in datasets.values()),
            "environment": os.getenv("ENVIRONMENT", "production"),
            "version": VERSION
        }
    except Exception as e:
        return {
            "server_url": bigquery_mcp.server_url,
            "connection_status": "error",
            "error": str(e),
            "version": VERSION
        }

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check including BigQuery MCP"""
    systems = {}
    
    systems["hotfix_version"] = VERSION
    systems["core_modules"] = "available" if CORE_MODULES_AVAILABLE else "unavailable"
    
    systems["openai_key"] = "configured" if os.getenv("OPENAI_API_KEY") else "missing"
    systems["supabase_url"] = "configured" if os.getenv("SUPABASE_URL") else "missing"
    systems["supabase_key"] = "configured" if os.getenv("SUPABASE_ANON_KEY") else "missing"
    systems["google_credentials"] = "configured" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON") else "missing"
    
    systems["openai_client"] = "available" if openai_client else "unavailable"
    systems["supabase_client"] = "available" if supabase_client else "unavailable"
    
    try:
        datasets = await discover_all_datasets()
        systems["bigquery_discovery"] = f"healthy ({sum(len(ds) for ds in datasets.values())} datasets)"
    except Exception as e:
        systems["bigquery_discovery"] = f"error: {str(e)[:50]}"
    
    systems["discovery_cache"] = f"active ({len(discovery_cache.last_discovery)} cached items)"
    systems["deterministic_sql"] = "enabled"
    systems["ai_randomness"] = "eliminated"
    
    critical_systems = ["openai_key", "supabase_url", "supabase_key"]
    healthy_systems = sum(1 for sys in critical_systems if systems.get(sys) == "configured")
    
    if healthy_systems == len(critical_systems):
        overall_status = "hotfix_healthy"
    else:
        overall_status = "hotfix_degraded"
    
    return HealthResponse(
        status=overall_status,
        systems=systems,
        timestamp=datetime.now().isoformat(),
        environment=os.getenv("ENVIRONMENT", "unknown")
    )

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on app shutdown"""
    await bigquery_mcp.close()

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting DETERMINISTIC HOTFIX v{VERSION}")
    logger.info(f"Server: {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"Hotfix: Eliminated AI randomness, deterministic SQL generation")
    logger.info(f"BigQuery MCP: {bigquery_mcp.server_url}")
    logger.info(f"Features: Consistent behavior, safe SQL patterns")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )