# main.py - SOLID AI SQL Generation v3.0.0 - MCP-Powered Like Claude Desktop
# Version Control: 
# v2.8.0 - Enhanced Dynamic System: MCP-powered schema discovery, GPT-4o upgrade, zero hardcoding
# v2.8.1 - FIXED: Platform routing errors, added union query support, enhanced validation
# v3.0.0 - MCP-POWERED: Uses same intelligent table selection as Claude Desktop MCP

import os
import logging
import time
import json
import re
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Literal, List, Any, Tuple
from dotenv import load_dotenv
import asyncio

# Version tracking
VERSION = "3.0.0"
PREVIOUS_VERSION = "2.8.1"

# MCP Integration - UPGRADED
from api.enhanced_bigquery_mcp import intelligent_bigquery_mcp
from api.bigquery_mcp import bigquery_mcp

# Request/Response models (unchanged)
class UnifiedQueryRequest(BaseModel):
    question: str
    data_source: Literal["rag", "bigquery"] = "rag"
    preferred_style: str = "standard"

class QueryRequest(BaseModel):
    question: str
    preferred_style: Optional[str] = "standard"
    context: Optional[str] = None

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

# Load environment
load_dotenv()

# Configure logging
logging.basicConfig(
    level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import core modules
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

# Import dashboard routes (unchanged)
try:
    from api.routes.dashboard import router as dashboard_router
    DASHBOARD_AVAILABLE = True
    logger.info("Dashboard routes loaded successfully")
except ImportError as e:
    DASHBOARD_AVAILABLE = False
    logger.error(f"Dashboard routes not available: {e}")

# Initialize FastAPI
app = FastAPI(
    title="Marketing Intelligence API - MCP-Powered Like Claude Desktop",
    description=f"MCP-Powered AI SQL Generation v{VERSION} - Same Intelligence as Claude Desktop",
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

# Include dashboard routes
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

# ===== MCP-POWERED TABLE LISTING (like Claude Desktop) =====

async def handle_bigquery_tables_query_mcp_powered(question: str) -> Optional[dict]:
    """Handle table listing using MCP intelligence like Claude Desktop"""
    try:
        # Enhanced keywords for table listing
        table_keywords = [
            'tables', 'datasets', 'schema', 'list tables', 'show tables', 'data tables',
            'what are the tables', 'show me tables', 'what tables', 'available tables',
            'table names', 'list all tables', 'expand', 'expand the names', 'expand names of table',
            'what data do you have', 'what can I query', 'available data'
        ]
        
        # Check if this is a table listing query
        is_table_query = any(keyword in question.lower() for keyword in table_keywords)
        
        if not is_table_query:
            return None  # Let MCP SQL handling take over
        
        # Use MCP to discover tables intelligently
        from api.enhanced_bigquery_mcp import MCPPoweredTableSelector
        table_selector = MCPPoweredTableSelector()
        
        logger.info("MCP-Powered table discovery starting...")
        discovery_start = time.time()
        
        # Get all tables with schemas
        all_tables_with_info = await table_selector._discover_all_tables_with_schemas()
        discovery_time = time.time() - discovery_start
        
        logger.info(f"MCP discovery completed in {discovery_time:.2f}s")
        
        # Count and organize results
        total_projects = len(all_tables_with_info)
        total_datasets = 0
        total_tables = 0
        
        organized_tables = {}
        
        for project, datasets in all_tables_with_info.items():
            organized_tables[project] = {}
            
            for dataset, tables in datasets.items():
                if tables:  # Only count datasets with tables
                    total_datasets += 1
                    total_tables += len(tables)
                    organized_tables[project][dataset] = list(tables.keys())
        
        logger.info(f"MCP Discovery Results: {total_projects} projects, {total_datasets} datasets, {total_tables} tables")
        
        # Create comprehensive response
        if total_tables == 0:
            answer = "No tables were discovered using MCP. This might indicate:\n\n"
            answer += "• BigQuery MCP connection issues\n"
            answer += "• Permission problems with the service account\n" 
            answer += "• No accessible tables in the datasets\n\n"
            answer += "Please check the MCP server logs for detailed information."
        else:
            answer = f"MCP-Powered Discovery found {total_projects} projects with {total_datasets} datasets and {total_tables} tables:\n\n"
            
            table_counter = 1
            
            # List by project -> dataset -> tables
            for project, datasets in organized_tables.items():
                if datasets:  # Only show projects with tables
                    answer += f"PROJECT: {project}\n\n"
                    
                    for dataset, tables in datasets.items():
                        if tables:
                            answer += f"Dataset: {dataset} ({len(tables)} tables)\n\n"
                            
                            # Show tables with platform identification
                            for table in sorted(tables):
                                platform = identify_table_platform(table)
                                answer += f"{table_counter}. {table} [{platform}]\n"
                                table_counter += 1
                            
                            answer += "\n"
            
            # Add sample questions based on discovered tables
            answer += "INTELLIGENT SAMPLE QUESTIONS (MCP-powered suggestions):\n\n"
            
            sample_questions = generate_intelligent_sample_questions(organized_tables)
            for question in sample_questions[:6]:
                answer += f"• {question}\n"
            
            answer += f"\nDiscovered using MCP intelligence in {discovery_time:.2f} seconds"
        
        return {
            "answer": answer,
            "data": {
                "projects": list(all_tables_with_info.keys()),
                "organized_tables": organized_tables,
                "total_projects": total_projects,
                "total_datasets": total_datasets,
                "total_tables": total_tables,
                "discovery_method": "mcp_powered",
                "discovery_time_seconds": discovery_time,
                "all_table_names": [
                    f"{proj}.{ds}.{table}" 
                    for proj, datasets in organized_tables.items() 
                    for ds, tables in datasets.items() 
                    for table in tables
                ]
            },
            "processing_time": discovery_time,
            "processing_method": f"mcp_powered_v{VERSION}",
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"MCP-powered table listing error: {e}")
        logger.error(f"Exception details:", exc_info=True)
        return {
            "answer": f"MCP-powered table discovery encountered an error:\n\n{str(e)}\n\nFalling back to basic discovery methods.",
            "error": str(e),
            "processing_time": 0.5,
            "version": f"{VERSION}_error",
            "discovery_method": "error_fallback"
        }

def identify_table_platform(table_name: str) -> str:
    """Identify platform from table name"""
    table_lower = table_name.lower()
    
    if 'shipstation' in table_lower:
        return 'ShipStation'
    elif 'gads' in table_lower:
        return 'Google Ads'
    elif 'msads' in table_lower:
        return 'Microsoft Ads'
    elif 'facebook' in table_lower:
        return 'Facebook Ads'
    elif 'zoho' in table_lower:
        return 'Zoho CRM'
    elif 'amazon' in table_lower:
        return 'Amazon'
    elif 'ga4' in table_lower:
        return 'GA4 Analytics'
    elif 'consolidated' in table_lower:
        return 'Consolidated Analytics'
    else:
        return 'Unknown'

def generate_intelligent_sample_questions(organized_tables: Dict) -> List[str]:
    """Generate intelligent sample questions based on discovered tables"""
    questions = []
    
    # Track platforms found
    platforms_found = set()
    
    for project, datasets in organized_tables.items():
        for dataset, tables in datasets.items():
            for table in tables:
                platform = identify_table_platform(table)
                platforms_found.add(platform)
    
    # Generate platform-specific questions
    if 'ShipStation' in platforms_found:
        questions.append("Show me recent ShipStation order fulfillment data")
        questions.append("What are the shipping metrics for last week?")
    
    if 'Google Ads' in platforms_found:
        questions.append("What are our top performing Google Ads campaigns by ROAS?")
        questions.append("Show me Google Ads performance metrics for this month")
    
    if 'Microsoft Ads' in platforms_found:
        questions.append("How are our Microsoft Ads campaigns performing?")
        questions.append("Compare Bing Ads performance vs last month")
    
    # Multi-platform questions if multiple platforms exist
    if len(platforms_found) > 1:
        questions.append("Compare performance across all advertising platforms")
        questions.append("Show me a unified view of marketing performance")
    
    if 'Consolidated Analytics' in platforms_found:
        questions.append("Show me the consolidated dashboard summary")
        questions.append("What's our overall marketing performance?")
    
    return questions

# ===== RESULT FORMATTING (unchanged but improved) =====

async def format_bigquery_results_mcp_enhanced(result: dict, question: str, sql_query: str, table_info: Tuple) -> str:
    """Enhanced result formatting with MCP context"""
    
    if not openai_client:
        return f"MCP-Powered Results for: {question}\n\nFound {len(result.get('content', []))} records"
    
    try:
        if not result.get('content'):
            return f"No data found for: {question}"
        
        # Extract data
        raw_data = []
        for item in result['content']:
            if isinstance(item, dict) and 'text' in item:
                raw_data.append(item['text'])
        
        data_text = '\n'.join(raw_data[:20])  # Limit for context
        
        # Enhanced analysis prompt with MCP context
        analysis_prompt = f"""You are analyzing data from a MCP-powered BigQuery system that intelligently selected the optimal table.

ORIGINAL QUESTION: {question}

TABLE SELECTED BY MCP: {'.'.join(table_info) if isinstance(table_info, tuple) else table_info}

RAW DATA:
{data_text}

FORMATTING REQUIREMENTS:
1. Use ONLY plain text - NO markdown formatting
2. Start with EXECUTIVE SUMMARY in caps
3. Use bullet points with "•" character  
4. Use line breaks for readability
5. Include specific numbers and insights
6. Mention that this was MCP-powered table selection

Provide a professional, readable analysis for chat interface."""

        response = openai_client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": analysis_prompt}],
            max_tokens=800,
            temperature=0.2
        )
        
        formatted_response = response.choices[0].message.content.strip()
        
        # Add MCP attribution
        formatted_response += f"\n\n---\nMCP-Powered Analysis: {len(result['content'])} records from intelligently selected table"
        if isinstance(table_info, tuple) and len(table_info) == 3:
            formatted_response += f"\nTable: {table_info[0]}.{table_info[1]}.{table_info[2]}"
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"MCP result formatting error: {e}")
        return f"Retrieved {len(result.get('content', []))} records using MCP-powered table selection for: {question}"

# ===== RAG FUNCTIONS (unchanged) =====

def classify_query(question: str) -> Dict[str, str]:
    """Enhanced query classification - unchanged"""
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
    """Simple Supabase search - unchanged"""
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
    """Advanced RAG search - unchanged"""
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
    """Format response by style - unchanged"""
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

# ===== DASHBOARD ENDPOINTS (unchanged) =====

def fix_dashboard_parsing(result: dict) -> list:
    """Enhanced parsing for dashboard endpoints - unchanged"""
    parsed_data = []
    
    if not result.get("content"):
        return parsed_data
    
    for item in result["content"]:
        if isinstance(item, dict) and "text" in item:
            try:
                row_data = json.loads(item["text"])
                
                store_name = (
                    row_data.get("online_store") or 
                    row_data.get("store") or 
                    row_data.get("Store_ID") or 
                    row_data.get("account_name") or 
                    "Unknown Store"
                )
                
                parsed_data.append({
                    "online_store": store_name,
                    "revenue": float(row_data.get("total_revenue", 0) or 0),
                    "users": int(row_data.get("total_users", 0)),
                    "sessions": int(row_data.get("total_sessions", 0)),
                    "conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0),
                    **row_data
                })
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON parse error: {e}")
                continue
    
    return parsed_data

# ===== ALL DASHBOARD ENDPOINTS (Complete Set) =====

@app.get("/api/dashboard/consolidated/by-store")
async def get_consolidated_by_store():
    """Get performance metrics grouped by online store (Last 4 weeks)"""
    try:
        query = """
        SELECT 
            online_store,
            SUM(Users) as total_users,
            SUM(Sessions) as total_sessions,
            SUM(Transactions) as total_transactions,
            SUM(Revenue) as total_revenue,
            AVG(Conversion_rate) as avg_conversion_rate,
            AVG(Bounce_rate) as avg_bounce_rate,
            SUM(New_users) as total_new_users,
            SUM(Pageviews) as total_pageviews
        FROM `data-tables-for-zoho.new_data_tables.consolidated_master`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        GROUP BY online_store
        ORDER BY total_revenue DESC
        """
        
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            return {"stores": [], "error": "Query execution failed"}
        
        store_data = fix_dashboard_parsing(result)
        logger.info(f"Store data parsed successfully: {len(store_data)} stores")
        return {"stores": store_data}
        
    except Exception as e:
        logger.error(f"Dashboard by store error: {str(e)}")
        return {"stores": [], "error": str(e)}

@app.get("/api/dashboard/consolidated/by-channel")
async def get_consolidated_by_channel():
    """Get performance metrics grouped by channel (Last 4 weeks)"""
    try:
        query = """
        SELECT 
            Channel_group,
            SUM(Users) as total_users,
            SUM(Sessions) as total_sessions,
            SUM(Transactions) as total_transactions,
            SUM(Revenue) as total_revenue,
            AVG(Conversion_rate) as avg_conversion_rate,
            AVG(Bounce_rate) as avg_bounce_rate,
            SUM(Add_To_Carts) as total_add_to_carts
        FROM `data-tables-for-zoho.new_data_tables.consolidated_master`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        GROUP BY Channel_group
        ORDER BY total_revenue DESC
        """
        
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            return {"channels": [], "error": "Query execution failed"}
        
        channel_data = []
        
        if result["content"]:
            try:
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        channel_data.append({
                            "channel": row_data.get("Channel_group", "Unknown"),
                            "users": int(row_data.get("total_users", 0)),
                            "sessions": int(row_data.get("total_sessions", 0)),
                            "transactions": int(row_data.get("total_transactions", 0)),
                            "revenue": float(row_data.get("total_revenue", 0) or 0),
                            "conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0),
                            "bounce_rate": float(row_data.get("avg_bounce_rate", 0) or 0),
                            "add_to_carts": int(row_data.get("total_add_to_carts", 0))
                        })
            except Exception as parse_error:
                logger.error(f"Channel data parse error: {parse_error}")
                return {"channels": [], "error": f"Parse error: {str(parse_error)}"}
        
        logger.info(f"Channel data parsed successfully: {len(channel_data)} channels")
        return {"channels": channel_data}
        
    except Exception as e:
        logger.error(f"Dashboard by channel error: {str(e)}")
        return {"channels": [], "error": str(e)}

@app.get("/api/dashboard/consolidated/time-series")
async def get_consolidated_time_series():
    """Get daily performance metrics for time series charts (Last 4 weeks)"""
    try:
        query = """
        SELECT 
            Date,
            SUM(Users) as daily_users,
            SUM(Sessions) as daily_sessions,
            SUM(Transactions) as daily_transactions,
            SUM(Revenue) as daily_revenue,
            AVG(Conversion_rate) as avg_conversion_rate,
            SUM(New_users) as daily_new_users
        FROM `data-tables-for-zoho.new_data_tables.consolidated_master`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        GROUP BY Date
        ORDER BY Date ASC
        """
        
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            return {"time_series": [], "error": "Query execution failed"}
        
        time_series_data = []
        
        if result["content"]:
            try:
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        
                        date_value = row_data.get("Date")
                        if isinstance(date_value, str):
                            formatted_date = date_value
                        else:
                            formatted_date = str(date_value)
                        
                        time_series_data.append({
                            "date": formatted_date,
                            "users": int(row_data.get("daily_users", 0)),
                            "sessions": int(row_data.get("daily_sessions", 0)),
                            "transactions": int(row_data.get("daily_transactions", 0)),
                            "revenue": float(row_data.get("daily_revenue", 0) or 0),
                            "conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0),
                            "new_users": int(row_data.get("daily_new_users", 0))
                        })
            except Exception as parse_error:
                logger.error(f"Time series data parse error: {parse_error}")
                return {"time_series": [], "error": f"Parse error: {str(parse_error)}"}
        
        logger.info(f"Time series data parsed successfully: {len(time_series_data)} days")
        return {"time_series": time_series_data}
        
    except Exception as e:
        logger.error(f"Dashboard time series error: {str(e)}")
        return {"time_series": [], "error": str(e)}

@app.get("/api/dashboard/consolidated/channel-summary")
async def get_channel_summary():
    """Get summary data for channel distribution - 4 week rolling window"""
    try:
        query = """
        SELECT 
            Channel_group,
            SUM(Users) as users,
            SUM(Sessions) as sessions,
            ROUND(AVG(Avg_session_duration), 2) as avg_session_duration,
            ROUND(AVG(Bounce_rate), 2) as bounce_rate,
            ROUND((SUM(Sessions) / NULLIF(SUM(Users), 0)), 2) as sessions_per_user,
            ROUND(AVG(Conversion_rate), 2) as avg_conversion_rate
        FROM `data-tables-for-zoho.new_data_tables.consolidated_master`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
          AND Channel_group IS NOT NULL
        GROUP BY Channel_group
        ORDER BY sessions DESC
        """
        
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            return {"channel_summary": [], "error": "Query execution failed"}
        
        channel_summary = []
        
        if result["content"]:
            try:
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        channel_summary.append({
                            "channel": row_data.get("Channel_group", "Unknown"),
                            "users": int(row_data.get("users", 0)),
                            "sessions": int(row_data.get("sessions", 0)),
                            "avg_session_duration": float(row_data.get("avg_session_duration", 0) or 0),
                            "bounce_rate": float(row_data.get("bounce_rate", 0) or 0),
                            "sessions_per_user_percent": float(row_data.get("sessions_per_user", 0) or 0) * 100,
                            "avg_conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0)
                        })
            except Exception as parse_error:
                logger.error(f"Channel summary parse error: {parse_error}")
                return {"channel_summary": [], "error": f"Parse error: {str(parse_error)}"}
        
        logger.info(f"Channel summary parsed successfully: {len(channel_summary)} channels")
        return {"channel_summary": channel_summary}
        
    except Exception as e:
        logger.error(f"Channel summary error: {str(e)}")
        return {"channel_summary": [], "error": str(e)}

@app.get("/api/dashboard/consolidated/summary")
async def get_consolidated_summary():
    """Get high-level summary metrics - unchanged from working version"""
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
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        """
        
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            logger.error(f"Query execution failed: {result}")
            raise Exception("Query execution failed")
        
        if result["content"] and len(result["content"]) > 0:
            try:
                data_item = result["content"][0]
                if isinstance(data_item, dict) and "text" in data_item:
                    row_data = json.loads(data_item["text"])
                    
                    return {
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
                    
            except json.JSONDecodeError as parse_error:
                logger.error(f"JSON parse error: {parse_error}")
        
        # Fallback
        return {
            "total_stores": 0, "total_days": 0, "total_users": 0,
            "total_sessions": 0, "total_transactions": 0, "total_revenue": 0.0,
            "avg_conversion_rate": 0.0, "avg_bounce_rate": 0.0,
            "total_new_users": 0, "total_add_to_carts": 0
        }
            
    except Exception as e:
        logger.error(f"Dashboard summary error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Dashboard error: {str(e)}")

# ===== MAIN API ENDPOINTS =====

@app.get("/")
async def root():
    """Root endpoint with MCP-Powered information"""
    uptime = time.time() - start_time
    
    try:
        # Test MCP discovery
        from api.enhanced_bigquery_mcp import MCPPoweredTableSelector
        table_selector = MCPPoweredTableSelector()
        
        discovery_start = time.time()
        all_tables = await table_selector._discover_all_tables_with_schemas()
        discovery_time = time.time() - discovery_start
        
        total_tables = sum(len(tables) for datasets in all_tables.values() for tables in datasets.values())
        
        discovery_summary = {
            "projects": len(all_tables),
            "total_tables": total_tables,
            "discovery_time_seconds": round(discovery_time, 2),
            "mcp_status": "operational"
        }
    except Exception as e:
        discovery_summary = {"mcp_status": f"error: {str(e)[:100]}"}
    
    return {
        "service": "Marketing Intelligence API - MCP-Powered Like Claude Desktop",
        "version": VERSION,
        "previous_version": PREVIOUS_VERSION,
        "status": "mcp_powered_operational",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "mcp_discovery_test": discovery_summary,
        "new_features": {
            "mcp_powered_table_selection": "Uses same intelligence as Claude Desktop MCP",
            "intelligent_platform_detection": "Dynamically identifies correct platform tables",
            "union_query_intelligence": "Smart multi-platform query detection and generation",
            "schema_aware_sql_generation": "Uses actual table schemas for better queries"
        },
        "maintained_features": {
            "dashboard_endpoints": "All working dashboard endpoints preserved",
            "rag_pipeline": "Full RAG system maintained",
            "response_formatting": "Chat-friendly formatting with GPT-4o",
            "error_handling": "Comprehensive error handling and fallbacks"
        },
        "endpoints": {
            "chat": "/api/chat",
            "unified_query": "/api/unified-query", 
            "mcp_test": "/api/mcp/test",
            "health": "/api/health"
        },
        "changelog": {
            "v3.0.0_MCP_POWERED": [
                "MAJOR: Uses same MCP-powered table selection as Claude Desktop",
                "INTELLIGENT: Dynamic platform detection based on table schemas",
                "SMART: Union query detection and generation",
                "ENHANCED: Schema-aware SQL generation",
                "MAINTAINED: All dashboard and RAG functionality"
            ]
        }
    }

@app.post("/api/chat", response_model=QueryResponse)
async def chat(request: QueryRequest):
    """Chat endpoint - unchanged"""
    process_start = time.time()
    
    try:
        if not CORE_MODULES_AVAILABLE and not EXTERNAL_CLIENTS_AVAILABLE:
            raise HTTPException(
                status_code=503, 
                detail="RAG system not available. Check server configuration."
            )
        
        logger.info(f"Processing chat query: {request.question[:100]}...")
        
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
    """MCP-POWERED unified endpoint - main enhancement"""
    process_start = time.time()
    
    try:
        if request.data_source == "rag":
            # RAG processing - unchanged
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
            # MCP-POWERED BigQuery processing
            
            # Check for table listing
            table_result = await handle_bigquery_tables_query_mcp_powered(request.question)
            
            if table_result is not None:
                processing_time = time.time() - process_start
                table_result["processing_time"] = processing_time
                table_result["response_style"] = request.preferred_style
                table_result["data_source"] = "bigquery"
                table_result["sql_query"] = None
                return table_result
            
            # MCP-POWERED SQL GENERATION
            logger.info(f"MCP-POWERED: Starting intelligent query processing...")
            mcp_start = time.time()
            
            try:
                # Use MCP intelligence like Claude Desktop
                sql_query, table_info = await intelligent_bigquery_mcp(request.question)
                mcp_time = time.time() - mcp_start
                
                logger.info(f"MCP SQL generation took: {mcp_time:.2f}s")
                logger.info(f"Generated SQL: {sql_query[:200]}...")
                
                # Execute SQL
                sql_exec_start = time.time()
                result = await bigquery_mcp.execute_sql(sql_query)
                sql_exec_time = time.time() - sql_exec_start
                
                logger.info(f"SQL execution took: {sql_exec_time:.2f}s")
                
                # Check execution success
                if result.get("isError") or not result.get("content"):
                    error_message = "Query execution failed"
                    if result.get("content"):
                        error_message = str(result["content"][:300])
                    
                    processing_time = time.time() - process_start
                    return {
                        "answer": f"MCP-powered query failed: {error_message}",
                        "error": error_message,
                        "sql_query": sql_query,
                        "table_used": table_info,
                        "query_type": "mcp_sql_error",
                        "processing_method": f"mcp_powered_v{VERSION}",
                        "sources_used": 0,
                        "processing_time": processing_time,
                        "response_style": request.preferred_style,
                        "data_source": "bigquery"
                    }
                
                # Format results with MCP context
                format_start = time.time()
                formatted_answer = await format_bigquery_results_mcp_enhanced(
                    result, request.question, sql_query, table_info
                )
                format_time = time.time() - format_start
                
                logger.info(f"MCP result formatting took: {format_time:.2f}s")
                
                # Final response
                processing_time = time.time() - process_start
                
                # Determine query type
                if isinstance(table_info, tuple) and table_info[0] == "union_query":
                    query_type = "multi_platform_union"
                else:
                    query_type = "mcp_single_table"
                
                return {
                    "answer": formatted_answer,
                    "data": result,
                    "sql_query": sql_query,
                    "table_used": table_info,
                    "query_type": query_type,
                    "processing_method": f"mcp_powered_v{VERSION}",
                    "sources_used": len(result.get("content", [])),
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery",
                    "mcp_intelligence": "enabled",
                    "timing_breakdown": {
                        "mcp_processing": round(mcp_time, 2),
                        "sql_execution": round(sql_exec_time, 2),
                        "result_formatting": round(format_time, 2)
                    }
                }
                
            except Exception as mcp_error:
                logger.error(f"MCP-powered processing failed: {mcp_error}")
                processing_time = time.time() - process_start
                
                return {
                    "answer": f"MCP-powered system encountered an error: {str(mcp_error)}",
                    "query_type": "mcp_error",
                    "processing_method": "mcp_error_fallback",
                    "sources_used": 0,
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery",
                    "error_details": str(mcp_error),
                    "version": VERSION
                }
            
    except Exception as e:
        processing_time = time.time() - process_start
        logger.error(f"Unified query MCP-powered error: {e}")
        return {
            "answer": f"MCP-Powered System error: {str(e)}",
            "query_type": "system_error",
            "processing_method": "error_handling",
            "sources_used": 0,
            "processing_time": processing_time,
            "response_style": request.preferred_style,
            "data_source": request.data_source,
            "error_details": str(e),
            "version": VERSION
        }

# ===== MCP TEST ENDPOINT =====

@app.get("/api/mcp/test")
async def test_mcp_system():
    """Test the MCP-powered system"""
    try:
        from api.enhanced_bigquery_mcp import MCPPoweredTableSelector, MCPUnionQueryDetector
        
        # Test table selector
        table_selector = MCPPoweredTableSelector()
        
        # Test with ShipStation query
        test_questions = [
            "Show me recent ShipStation orders",
            "What are our Google Ads campaigns performing?",
            "Compare Google Ads vs Microsoft Ads performance"
        ]
        
        results = {}
        
        for question in test_questions:
            test_start = time.time()
            try:
                scored_tables = await table_selector.intelligent_table_discovery(question)
                test_time = time.time() - test_start
                
                if scored_tables:
                    best_table = scored_tables[0]
                    results[question] = {
                        "status": "success",
                        "best_table": f"{best_table[0]}.{best_table[1]}.{best_table[2]}",
                        "score": best_table[3],
                        "alternatives": len(scored_tables) - 1,
                        "processing_time": round(test_time, 3)
                    }
                else:
                    results[question] = {
                        "status": "no_tables_found",
                        "processing_time": round(test_time, 3)
                    }
                    
            except Exception as e:
                results[question] = {
                    "status": "error",
                    "error": str(e),
                    "processing_time": round(time.time() - test_start, 3)
                }
        
        # Test union detection
        union_detector = MCPUnionQueryDetector(table_selector)
        union_test = await union_detector.analyze_union_requirements("Compare Google Ads vs Microsoft Ads")
        
        return {
            "mcp_system_status": "operational",
            "version": VERSION,
            "table_discovery_tests": results,
            "union_detection_test": {
                "needs_union": union_test.get("needs_union", False),
                "detected_platforms": union_test.get("detected_platforms", []),
                "union_type": union_test.get("union_type", "none")
            },
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "mcp_system_status": "error",
            "error": str(e),
            "version": VERSION,
            "timestamp": datetime.now().isoformat()
        }

# ===== HEALTH CHECK =====

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check with MCP system status"""
    systems = {}
    
    systems["mcp_powered_version"] = VERSION
    systems["mcp_intelligence"] = "enabled - same as Claude Desktop"
    
    # Test MCP system
    try:
        from api.enhanced_bigquery_mcp import MCPPoweredTableSelector
        table_selector = MCPPoweredTableSelector()
        test_tables = await table_selector._discover_all_tables_with_schemas()
        total_tables = sum(len(tables) for datasets in test_tables.values() for tables in datasets.values())
        systems["mcp_table_discovery"] = f"healthy ({total_tables} tables discovered)"
    except Exception as e:
        systems["mcp_table_discovery"] = f"error: {str(e)[:50]}"
    
    # Test basic MCP connection
    try:
        test_result = await bigquery_mcp.execute_sql("SELECT 1 as test")
        if test_result.get("isError"):
            systems["mcp_connection"] = "error: query failed"
        else:
            systems["mcp_connection"] = "healthy"
    except Exception as e:
        systems["mcp_connection"] = f"error: {str(e)[:50]}"
    
    systems["core_modules"] = "available" if CORE_MODULES_AVAILABLE else "unavailable"
    systems["dashboard_routes"] = "available" if DASHBOARD_AVAILABLE else "unavailable"
    
    systems["openai_key"] = "configured" if os.getenv("OPENAI_API_KEY") else "missing"
    systems["supabase_url"] = "configured" if os.getenv("SUPABASE_URL") else "missing"
    systems["google_credentials"] = "configured" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON") else "missing"
    
    systems["openai_client"] = "available (gpt-4o)" if openai_client else "unavailable"
    systems["supabase_client"] = "available" if supabase_client else "unavailable"
    
    # Test dashboard connectivity
    try:
        test_query = "SELECT COUNT(*) as test_count FROM `data-tables-for-zoho.new_data_tables.consolidated_master` LIMIT 1"
        test_result = await bigquery_mcp.execute_sql(test_query)
        if test_result.get("isError"):
            systems["dashboard_data"] = "error: query failed"
        else:
            systems["dashboard_data"] = "healthy"
    except Exception as e:
        systems["dashboard_data"] = f"error: {str(e)[:50]}"
    
    # Overall health determination
    critical_systems = ["mcp_connection", "mcp_table_discovery", "openai_key", "dashboard_data"]
    healthy_systems = sum(1 for sys in critical_systems if "healthy" in str(systems.get(sys, "")) or "configured" in str(systems.get(sys, "")))
    
    if healthy_systems >= 3:
        overall_status = "mcp_powered_healthy"
    elif healthy_systems >= 2:
        overall_status = "mcp_powered_degraded"  
    else:
        overall_status = "mcp_powered_offline"
    
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
    
    logger.info(f"Starting MCP-Powered System v{VERSION}")
    logger.info(f"Server: {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"MCP Intelligence: Enabled - Same as Claude Desktop")
    logger.info(f"BigQuery MCP: {bigquery_mcp.server_url}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )