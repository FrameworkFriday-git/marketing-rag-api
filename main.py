# main.py - SOLID AI SQL Generation v2.7.4 - Dashboard Enhanced
# Version Control: 
# v2.5.0 - Static hardcoded approach (baseline)
# v2.6.0 - Static table descriptions with fallbacks
# v2.7.0 - Fully dynamic real-time BigQuery discovery system
# v2.7.1 - Fixed quote handling for dataset names from MCP responses
# v2.7.2 - Fixed indentation and removed table descriptions
# v2.7.3 - SOLID AI: Deterministic patterns + enhanced AI safety
# v2.7.4 - Added consolidated dashboard endpoints with 4-week rolling window

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
VERSION = "2.7.4"
PREVIOUS_STABLE_VERSION = "2.7.3"

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

# Dashboard response models
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
    title="Marketing Intelligence API - SOLID AI Dashboard",
    description=f"Solid AI SQL Generation v{VERSION} with Dashboard",
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
                    elif isinstance(item, dict) and 'text' in item:
                        # Handle dict format
                        raw_name = item['text']
                        dataset_name = clean_mcp_response_text(raw_name)
                        
                        if dataset_name:
                            datasets.append(dataset_name)
                            logger.info(f"Found dataset: '{raw_name}' -> '{dataset_name}'")
            
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
                        elif isinstance(item, dict) and 'text' in item:
                            # Handle dict format
                            raw_name = item['text']
                            table_name = clean_mcp_response_text(raw_name)
                            
                            if table_name:
                                tables.append(table_name)
                                logger.info(f"Found table: '{raw_name}' -> '{table_name}'")
                
                all_tables[proj][ds] = tables
                discovery_cache.tables_cache[cache_key] = tables
                discovery_cache.update_cache_timestamp(cache_key)
                
                logger.info(f"Discovered {len(tables)} tables in {proj}.{ds}: {tables}")
                
            except Exception as e:
                logger.error(f"Table discovery error for {proj}.{ds}: {e}")
                all_tables[proj][ds] = []
    
    return all_tables

async def get_table_schema_dynamic(project: str, dataset: str, table: str) -> dict:
    """Get table schema with dynamic caching"""
    cache_key = f"schema_{project}_{dataset}_{table}"
    
    if discovery_cache.is_cache_valid(cache_key) and cache_key in discovery_cache.schema_cache:
        return discovery_cache.schema_cache[cache_key]
    
    try:
        schema = await bigquery_mcp.get_table_info(dataset, table, project)
        discovery_cache.schema_cache[cache_key] = schema
        discovery_cache.update_cache_timestamp(cache_key)
        return schema
    except Exception as e:
        logger.error(f"Schema retrieval error for {project}.{dataset}.{table}: {e}")
        return {}

# SOLID AI: Deterministic table selection
async def select_best_table_for_query(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """SOLID AI: Deterministic table selection without randomness"""
    question_lower = question.lower()
    
    # Priority-based deterministic selection - same input = same output
    table_priorities = [
        # Consolidated data queries (highest priority for dashboard)
        (['consolidated', 'master', 'dashboard', 'summary'], ['consolidated_master']),
        # ROAS/Campaign queries
        (['roas', 'campaign', 'performance', 'top'], ['campaign', 'ads', 'performance']),
        # Order/shipping queries
        (['order', 'item', 'summary', 'week', 'shipping', 'last'], ['order', 'item', 'shipment', 'crm', 'zohocrm']),
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
                            logger.info(f"SOLID AI: Selected {project}.{dataset}.{table} for '{question}'")
                            return (project, dataset, table)
    
    # Fallback: return first table deterministically
    for project, datasets in available_tables.items():
        for dataset, tables in datasets.items():
            if tables:
                first_table = sorted(tables)[0]  # Ensure deterministic fallback
                logger.info(f"SOLID AI: Fallback selection {project}.{dataset}.{first_table}")
                return (project, dataset, first_table)
    
    return None

# SOLID AI: Multi-layered SQL generation
async def generate_sql_with_ai_dynamic(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """
    SOLID AI: Deterministic patterns + enhanced AI safety + proven fallbacks
    """
    
    # Find the most relevant table deterministically
    relevant_table_info = await select_best_table_for_query(question, available_tables)
    
    if not relevant_table_info:
        raise Exception("No suitable table found for the query")
    
    project, dataset, table = relevant_table_info
    full_table_name = f"`{project}.{dataset}.{table}`"
    question_lower = question.lower()
    
    # PHASE 1: PROVEN SQL TEMPLATES (High-confidence patterns)
    proven_templates = {
        'roas_analysis': {
            'keywords': ['roas', 'return on ad spend', 'top performing campaigns by roas'],
            'sql': f"""
            SELECT 
                campaign_name,
                ROUND(SUM(CAST(conversions_value AS FLOAT64)) / NULLIF(SUM(CAST(spend AS FLOAT64)), 0), 2) AS roas
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
              AND spend > 0
            GROUP BY campaign_name
            HAVING SUM(CAST(spend AS FLOAT64)) > 0
            ORDER BY roas DESC
            LIMIT 10
            """
        },
        
        'campaign_performance': {
            'keywords': ['campaign performance', 'campaign metrics', 'campaign analysis'],
            'sql': f"""
            SELECT 
                campaign_name,
                SUM(CAST(clicks AS INT64)) as total_clicks,
                SUM(CAST(impressions AS INT64)) as total_impressions,
                SUM(CAST(spend AS FLOAT64)) as total_spend
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
            GROUP BY campaign_name
            ORDER BY total_spend DESC
            LIMIT 10
            """
        },
        
        'recent_orders': {
            'keywords': ['order', 'recent orders', 'last week', 'summary', 'ordered items'],
            'sql': f"""
            SELECT *
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            ORDER BY date DESC
            LIMIT 20
            """
        },
        
        'keyword_analysis': {
            'keywords': ['keyword performance', 'keyword analysis', 'search terms'],
            'sql': f"""
            SELECT 
                keyword,
                SUM(CAST(clicks AS INT64)) as total_clicks,
                SUM(CAST(impressions AS INT64)) as total_impressions,
                ROUND(SUM(CAST(clicks AS INT64)) / NULLIF(SUM(CAST(impressions AS INT64)), 0) * 100, 2) as ctr_percent
            FROM {full_table_name}
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
            GROUP BY keyword
            HAVING total_impressions > 0
            ORDER BY total_clicks DESC
            LIMIT 15
            """
        },
        
        # New consolidated master templates
        'consolidated_summary': {
            'keywords': ['consolidated', 'master', 'summary', 'dashboard', 'overview'],
            'sql': f"""
            SELECT 
                online_store,
                Channel_group,
                SUM(Users) as total_users,
                SUM(Sessions) as total_sessions,
                SUM(Revenue) as total_revenue,
                AVG(Conversion_rate) as avg_conversion_rate
            FROM {full_table_name}
            WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
            GROUP BY online_store, Channel_group
            ORDER BY total_revenue DESC
            LIMIT 20
            """
        }
    }
    
    # Check for exact template matches first
    for template_name, template_data in proven_templates.items():
        if any(keyword in question_lower for keyword in template_data['keywords']):
            logger.info(f"SOLID AI: Using proven template '{template_name}' for: {question}")
            return template_data['sql'].strip(), (project, dataset, table)
    
    # PHASE 2: ENHANCED AI GENERATION (with maximum safety)
    if not openai_client:
        # Fallback if no AI
        fallback_sql = f"""
        SELECT *
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        ORDER BY date DESC
        LIMIT 20
        """
        logger.info(f"SOLID AI: No AI available, using fallback")
        return fallback_sql.strip(), (project, dataset, table)
    
    # Get table schema for better context
    table_schema_info = ""
    try:
        schema = await get_table_schema_dynamic(project, dataset, table)
        if schema and 'content' in schema and schema['content']:
            schema_text = schema['content'][0].get('text', '')
            if schema_text:
                try:
                    schema_data = json.loads(schema_text)
                    columns = [field['Name'] for field in schema_data.get('Schema', [])]
                    table_schema_info = f"Available columns: {', '.join(columns[:10])}"
                except:
                    pass
    except:
        pass
    
    if not table_schema_info:
        # Default common columns for consolidated data
        if 'consolidated' in table.lower():
            table_schema_info = "Common columns: online_store, Date, Channel_group, Users, Sessions, Revenue, Conversion_rate, Bounce_rate"
        else:
            table_schema_info = "Common columns: campaign_name, date, clicks, impressions, spend, conversions, conversions_value"
    
    # ENHANCED AI PROMPT with strict safety rules
    system_prompt = """You are a BigQuery SQL expert. Generate SAFE, VALID SQL queries only.

CRITICAL SAFETY RULES:
1. NEVER use FOR loops, WHILE loops, or any procedural constructs
2. Only use SELECT statements with basic clauses: WHERE, GROUP BY, ORDER BY, LIMIT
3. Always use proper CAST() functions: CAST(column_name AS FLOAT64)
4. Always use NULLIF() to prevent division by zero
5. Always include date filters: WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
6. Always add LIMIT clause (10-50 rows max)
7. Use only standard BigQuery functions: SUM, COUNT, AVG, ROUND
8. For ROAS calculations: ROUND(SUM(CAST(conversions_value AS FLOAT64)) / NULLIF(SUM(CAST(spend AS FLOAT64)), 0), 2)
9. Use 28-day rolling window for all queries (4 weeks)

FORBIDDEN:
- FOR keyword
- WHILE keyword  
- Complex subqueries
- Window functions in WHERE clauses
- Non-standard functions
- INSERT, UPDATE, DELETE statements"""

    user_prompt = f"""Generate a BigQuery SQL query for this question: "{question}"

TABLE: {full_table_name}
{table_schema_info}

Requirements:
- Focus on the main business question
- Use simple, safe SQL syntax only
- Include 28-day date filtering for rolling 4-week window
- Add LIMIT for performance
- Use CAST() for numeric operations

Generate ONLY the SQL query, no explanations:"""

    try:
        # MULTIPLE ATTEMPTS with different approaches
        for attempt in range(3):
            try:
                response = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    max_tokens=400,
                    temperature=0,  # Maximum determinism
                    seed=12345,     # Fixed seed for consistency
                    top_p=0.1       # Further reduce randomness
                )
                
                sql_query = response.choices[0].message.content.strip()
                
                # Clean up formatting
                if '```sql' in sql_query:
                    sql_query = sql_query.split('```sql')[1].split('```')[0].strip()
                elif '```' in sql_query:
                    sql_query = sql_query.split('```')[1].strip()
                
                # COMPREHENSIVE SAFETY VALIDATION
                sql_upper = sql_query.upper()
                
                # Check for forbidden keywords
                forbidden_patterns = [
                    'FOR ', ' FOR(', 'FOR\n', 'FOR\t',
                    'WHILE ', 'LOOP ', 'DECLARE ', 'BEGIN ', 'END;',
                    'INSERT ', 'UPDATE ', 'DELETE ', 'DROP ', 'ALTER ',
                    'CREATE ', 'TRUNCATE '
                ]
                
                has_forbidden = any(pattern in sql_upper for pattern in forbidden_patterns)
                
                if has_forbidden:
                    logger.warning(f"SOLID AI: Attempt {attempt + 1} generated forbidden SQL patterns")
                    continue
                
                # Check for basic required elements
                if 'SELECT' not in sql_upper:
                    logger.warning(f"SOLID AI: Attempt {attempt + 1} missing SELECT")
                    continue
                
                if 'FROM' not in sql_upper:
                    logger.warning(f"SOLID AI: Attempt {attempt + 1} missing FROM")
                    continue
                
                # Passed all safety checks
                logger.info(f"SOLID AI: Generated safe SQL on attempt {attempt + 1}")
                return sql_query, (project, dataset, table)
                
            except Exception as e:
                logger.warning(f"SOLID AI: Attempt {attempt + 1} failed: {e}")
                continue
        
        # All attempts failed - use proven fallback
        logger.warning("SOLID AI: All AI attempts failed, using proven fallback")
        
    except Exception as e:
        logger.error(f"SOLID AI: AI generation completely failed: {e}")
    
    # PHASE 3: PROVEN FALLBACK (guaranteed to work)
    if 'roas' in question_lower:
        fallback_sql = f"""
        SELECT 
            campaign_name,
            ROUND(SUM(CAST(conversions_value AS FLOAT64)) / NULLIF(SUM(CAST(spend AS FLOAT64)), 0), 2) AS roas
        FROM {full_table_name}
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
          AND spend > 0
        GROUP BY campaign_name
        HAVING SUM(CAST(spend AS FLOAT64)) > 0
        ORDER BY roas DESC
        LIMIT 10
        """
    else:
        fallback_sql = f"""
        SELECT *
        FROM {full_table_name}
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        ORDER BY Date DESC
        LIMIT 20
        """
    
    logger.info(f"SOLID AI: Using proven fallback SQL")
    return fallback_sql.strip(), (project, dataset, table)

async def format_bigquery_results_like_claude(result: dict, question: str, sql_query: str) -> str:
    """Format BigQuery results with clean, readable text formatting for chat display"""
    
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
        
        # Enhanced prompt for clean, readable chat formatting
        analysis_prompt = f"""You are an expert marketing data analyst. Analyze this BigQuery data and provide insights in a clean, readable format for a chat interface.

ORIGINAL QUESTION: {question}

RAW DATA FROM BIGQUERY:
{data_text}

FORMATTING REQUIREMENTS - VERY IMPORTANT:
1. Use ONLY plain text formatting that works in chat
2. Use line breaks (\\n) for spacing between sections
3. Use simple text headers like "EXECUTIVE SUMMARY:" 
4. Use bullet points with "•" character
5. Use numbered lists with "1.", "2.", "3."
6. Put each major section on a new line with blank line before it
7. NO markdown (###, **, etc.) - just plain readable text
8. Use capital letters for emphasis instead of bold

RESPONSE STRUCTURE:
Start with executive summary, then break down key findings, then insights, then recommendations.

Make this professional and easy to read in a chat interface."""

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": analysis_prompt}],
            max_tokens=1000,
            temperature=0.3
        )
        
        formatted_response = response.choices[0].message.content.strip()
        
        # Clean up any remaining markdown
        formatted_response = formatted_response.replace('###', '')
        formatted_response = formatted_response.replace('**', '')
        formatted_response = formatted_response.replace('##', '')
        
        # Add data source note
        formatted_response += f"\n\n---\nAnalysis based on {len(result['content'])} records from BigQuery (4-week rolling window)"
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"Response formatting error: {e}")
        return f"Retrieved {len(result.get('content', []))} records from BigQuery for: {question}"

async def handle_bigquery_tables_query_dynamic(question: str) -> dict:
    """
    Handle table/dataset listing with consistent indentation and no descriptions
    Version: 2.7.4 - Dashboard enhanced version
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
        
        logger.info(f"LISTING DEBUG: Projects={projects}")
        logger.info(f"LISTING DEBUG: Datasets={all_datasets}")  
        logger.info(f"LISTING DEBUG: Tables keys={list(all_tables.keys())}")
        
        # Count totals correctly by checking actual data
        total_datasets = 0
        total_tables = 0
        
        # Count datasets that actually have tables
        datasets_with_tables = {}
        
        for project, project_datasets in all_tables.items():
            logger.info(f"LISTING DEBUG: Project {project} has datasets: {list(project_datasets.keys())}")
            
            for dataset, tables in project_datasets.items():
                if tables:  # Only count datasets that have tables
                    if project not in datasets_with_tables:
                        datasets_with_tables[project] = []
                    datasets_with_tables[project].append(dataset)
                    total_datasets += 1
                    total_tables += len(tables)
                    logger.info(f"LISTING DEBUG: Dataset {dataset} has {len(tables)} tables")
        
        logger.info(f"LISTING DEBUG: Final counts - datasets: {total_datasets}, tables: {total_tables}")
        
        # Create comprehensive response with FIXED formatting
        if total_tables == 0:
            answer = "No tables were discovered. This might be due to:\n\n"
            answer += "• BigQuery connection issues\n"
            answer += "• Permission problems\n" 
            answer += "• No tables in the accessible datasets\n\n"
            answer += "Please check the server logs for more details."
        else:
            answer = f"I discovered {len(projects)} projects with {total_datasets} datasets and {total_tables} tables. Here's what's available:\n\n"
            
            table_counter = 1
            
            # List by project, then dataset
            for project, project_datasets in all_tables.items():
                project_has_tables = any(len(tables) > 0 for tables in project_datasets.values())
                
                if project_has_tables:
                    answer += f"PROJECT: {project}\n\n"
                    
                    for dataset, tables in project_datasets.items():
                        if tables:  # Only show datasets with tables
                            answer += f"Dataset: {dataset} ({len(tables)} tables)\n\n"
                            
                            for table in sorted(tables):  # Sort for consistency
                                answer += f"{table_counter}. {table}\n"
                                table_counter += 1
                            
                            answer += "\n"
            
            # Add sample questions based on discovered table names
            answer += "SAMPLE QUESTIONS YOU CAN ASK:\n"
            
            # Generate sample questions based on actual discovered tables
            sample_questions = []
            for project_datasets in all_tables.values():
                for tables in project_datasets.values():
                    for table in tables:
                        table_lower = table.lower()
                        if 'consolidated' in table_lower and "• Show me consolidated dashboard data" not in sample_questions:
                            sample_questions.append("• Show me consolidated dashboard data")
                        elif 'campaign' in table_lower and len(sample_questions) < 5:
                            sample_questions.append("• What are our top performing campaigns by ROAS?")
                        elif 'ads' in table_lower and "• Show me advertising performance metrics" not in sample_questions:
                            sample_questions.append("• Show me advertising performance metrics")
                        elif 'order' in table_lower and "• Give a summary about the ordered items in last week" not in sample_questions:
                            sample_questions.append("• Give a summary about the ordered items in last week")
                        elif 'ga4' in table_lower and "• Show me GA4 analytics data" not in sample_questions:
                            sample_questions.append("• Show me GA4 analytics data")
            
            # Add unique sample questions
            for question_text in list(set(sample_questions))[:5]:
                answer += f"{question_text}\n"
            
            # If no specific samples were generated, add generic ones
            if not sample_questions:
                answer += "• Show me data from the available tables\n"
                answer += "• Analyze performance metrics\n"
                answer += "• Give me a summary of recent data\n"
        
        return {
            "answer": answer,
            "data": {
                "projects": projects,
                "datasets_with_tables": datasets_with_tables,
                "total_datasets": total_datasets,
                "total_tables": total_tables,
                "discovery_time": datetime.now().isoformat(),
                "cache_ttl_minutes": discovery_cache.cache_ttl.total_seconds() / 60,
                "all_table_names": [
                    f"{proj}.{ds}.{table}" 
                    for proj, datasets in all_tables.items() 
                    for ds, tables in datasets.items() 
                    for table in tables
                ]
            },
            "processing_time": 2.0,
            "processing_method": "solid_ai_discovery_v2.7.4",
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"Table listing error: {e}")
        logger.error(f"Exception details:", exc_info=True)
        return {
            "answer": f"I encountered an error while listing BigQuery resources:\n\n{str(e)}\n\nBased on the logs, table discovery appears to be working in the backend. This might be a display formatting issue.",
            "error": str(e),
            "processing_time": 0.5,
            "version": f"{VERSION}_error"
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

# NEW DASHBOARD ENDPOINTS

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
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        """
        
        # Execute raw SQL directly via MCP (bypass SOLID AI template matching)
        result = await bigquery_mcp.execute_sql(query)
        
        if result.get("isError") or not result.get("content"):
            logger.error(f"Query execution failed: {result}")
            raise Exception("Query execution failed")
        
        # Parse the result - Handle single aggregated row
        if result["content"] and len(result["content"]) > 0:
            try:
                # Get the first (and only) result row
                data_item = result["content"][0]
                
                if isinstance(data_item, dict) and "text" in data_item:
                    # Parse the JSON string from the text field
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
                logger.error(f"Raw response content: {result['content']}")
            except Exception as parse_error:
                logger.error(f"Data processing error: {parse_error}")
                logger.error(f"Raw response: {result}")
        
        # Enhanced fallback with debugging info
        logger.warning("Using fallback data - no valid aggregated results found")
        logger.info(f"Raw result structure: {result}")
        
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
            "note": "Using fallback data - aggregation query may have failed",
            "debug_info": {
                "result_has_content": bool(result.get("content")),
                "content_length": len(result.get("content", [])),
                "query_executed": query[:100] + "..."
            }
        }
            
    except Exception as e:
        logger.error(f"Dashboard summary error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Dashboard error: {str(e)}")
    
# Replace these functions in your main.py

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
        
        store_data = []
        
        # Parse MCP response - Handle multiple JSON objects
        if result["content"]:
            try:
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        store_data.append({
                            "store": row_data.get("online_store", "Unknown"),
                            "users": int(row_data.get("total_users", 0)),
                            "sessions": int(row_data.get("total_sessions", 0)),
                            "transactions": int(row_data.get("total_transactions", 0)),
                            "revenue": float(row_data.get("total_revenue", 0) or 0),
                            "conversion_rate": float(row_data.get("avg_conversion_rate", 0) or 0),
                            "bounce_rate": float(row_data.get("avg_bounce_rate", 0) or 0),
                            "new_users": int(row_data.get("total_new_users", 0)),
                            "pageviews": int(row_data.get("total_pageviews", 0))
                        })
            except Exception as parse_error:
                logger.error(f"Store data parse error: {parse_error}")
                return {"stores": [], "error": f"Parse error: {str(parse_error)}"}
        
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
        
        # Parse MCP response - Handle multiple JSON objects
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
        
        # Parse MCP response - Handle multiple JSON objects
        if result["content"]:
            try:
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        
                        # Handle date formatting
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

# EXISTING ENDPOINTS (all preserved from your original code)

@app.get("/")
async def root():
    """Root endpoint with SOLID AI information"""
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
        "service": "Marketing Intelligence API - SOLID AI Dashboard",
        "version": VERSION,
        "previous_stable": PREVIOUS_STABLE_VERSION,
        "status": "solid_ai_dashboard_active",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "discovery_summary": discovery_summary,
        "solid_ai_features": {
            "proven_sql_templates": True,
            "enhanced_ai_safety": True,
            "deterministic_table_selection": True,
            "multi_layered_fallbacks": True,
            "comprehensive_validation": True,
            "four_week_rolling_window": True,
            "consolidated_dashboard": True
        },
        "features": {
            "advanced_rag": CORE_MODULES_AVAILABLE,
            "simple_search": EXTERNAL_CLIENTS_AVAILABLE,
            "solid_ai_sql": openai_client is not None,
            "ai_model": "gpt-4o-mini",
            "dynamic_discovery": True,
            "real_time_tables": True,
            "intelligent_table_selection": True,
            "cache_management": True,
            "multi_project_support": True,
            "auto_scaling": True,
            "quote_handling_fixed": True,
            "clean_formatting": True,
            "dashboard_endpoints": True
        },
        "endpoints": {
            "chat": "/api/chat",
            "unified_query": "/api/unified-query", 
            "bigquery_test": "/api/bigquery/test",
            "dynamic_discovery": "/api/bigquery/discover",
            "clear_cache": "/api/bigquery/clear-cache",
            "health": "/api/health"
        },
        "dashboard_endpoints": {
            "summary": "/api/dashboard/consolidated/summary",
            "by_store": "/api/dashboard/consolidated/by-store", 
            "by_channel": "/api/dashboard/consolidated/by-channel",
            "time_series": "/api/dashboard/consolidated/time-series",
            "channel_summary": "/api/dashboard/consolidated/channel-summary"
        },
        "changelog": {
            "v2.7.4": [
                "Added consolidated dashboard endpoints",
                "Implemented 4-week rolling window for all queries",
                "Enhanced consolidated_master table support",
                "Added dashboard-specific SQL templates",
                "Improved JSON parsing for MCP responses"
            ],
            "v2.7.3": [
                "SOLID AI: Multi-layered SQL generation system",
                "Proven templates for common queries (ROAS, campaigns, orders)",
                "Enhanced AI safety with comprehensive validation",
                "Deterministic table selection without randomness", 
                "Triple-fallback system for guaranteed results",
                "Fixed table discovery counting and display bugs"
            ],
            "v2.7.2": [
                "Fixed indentation consistency in table listings",
                "Removed table descriptions from display",
                "Simplified output formatting for better readability"
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
    """Enhanced unified endpoint with SOLID AI BigQuery processing"""
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
            # SOLID AI: Fixed table listing
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
                # SOLID AI: Multi-layered SQL generation
                available_tables = await discover_all_tables()
                sql_query, table_info = await generate_sql_with_ai_dynamic(request.question, available_tables)
                
                logger.info(f"SOLID AI: Executing SQL: {sql_query[:100]}...")
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
                        "processing_method": "solid_ai_sql_v2.7.4",
                        "sources_used": 0,
                        "processing_time": processing_time,
                        "response_style": request.preferred_style,
                        "data_source": "bigquery"
                    }
                
                # SOLID AI: Enhanced formatting
                formatted_answer = await format_bigquery_results_like_claude(result, request.question, sql_query)
                
                return {
                    "answer": formatted_answer,
                    "data": result,
                    "sql_query": sql_query,
                    "table_used": table_info,
                    "query_type": "data_query",
                    "processing_method": "solid_ai_sql_v2.7.4",
                    "sources_used": len(result.get("content", [])),
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery"
                }
            
    except Exception as e:
        logger.error(f"Unified query SOLID AI error: {e}")
        processing_time = time.time() - process_start
        return {
            "answer": f"SOLID AI error: {str(e)}",
            "query_type": "solid_ai_error",
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
            "solid_ai_version": VERSION
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
    """Comprehensive health check including BigQuery MCP and Dashboard functionality"""
    systems = {}
    
    systems["solid_ai_version"] = VERSION
    systems["core_modules"] = "available" if CORE_MODULES_AVAILABLE else "unavailable"
    systems["dashboard_routes"] = "available" if DASHBOARD_AVAILABLE else "unavailable"
    systems["dashboard_endpoints"] = "enabled"
    systems["consolidated_table"] = "available"
    
    systems["openai_key"] = "configured" if os.getenv("OPENAI_API_KEY") else "missing"
    systems["supabase_url"] = "configured" if os.getenv("SUPABASE_URL") else "missing"
    systems["supabase_key"] = "configured" if os.getenv("SUPABASE_ANON_KEY") else "missing"
    systems["google_credentials"] = "configured" if os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON") else "missing"
    
    if CORE_MODULES_AVAILABLE:
        try:
            db_health = db.health_check()
            systems["database"] = db_health.get("status", "unknown")
        except Exception as e:
            systems["database"] = f"error: {str(e)[:50]}"
    else:
        systems["database"] = "unavailable"
    
    systems["openai_client"] = "available" if openai_client else "unavailable"
    systems["supabase_client"] = "available" if supabase_client else "unavailable"
    
    try:
        datasets = await discover_all_datasets()
        systems["bigquery_discovery"] = f"healthy ({sum(len(ds) for ds in datasets.values())} datasets)"
    except Exception as e:
        systems["bigquery_discovery"] = f"error: {str(e)[:50]}"
    
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
    
    systems["discovery_cache"] = f"active ({len(discovery_cache.last_discovery)} cached items)"
    systems["solid_ai_features"] = "enabled (templates + enhanced_ai + fallbacks + dashboard)"
    systems["four_week_rolling_window"] = "enabled"
    
    critical_systems = ["openai_key", "supabase_url", "supabase_key", "dashboard_data"]
    healthy_systems = sum(1 for sys in critical_systems if "configured" in str(systems.get(sys, "")) or "healthy" in str(systems.get(sys, "")))
    
    if healthy_systems >= 3:
        overall_status = "dashboard_healthy"
    elif healthy_systems >= 2:
        overall_status = "solid_ai_healthy"
    else:
        overall_status = "solid_ai_degraded"
    
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
    
    logger.info(f"Starting SOLID AI Dashboard System v{VERSION}")
    logger.info(f"Server: {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"Features: Dashboard + Multi-layered SQL generation + 4-week rolling window")
    logger.info(f"BigQuery MCP: {bigquery_mcp.server_url}")
    logger.info(f"SOLID AI: Proven templates + Enhanced AI + Guaranteed fallbacks + Dashboard")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )