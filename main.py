# main.py - Fixed Quote Handling for Dataset Names v2.7.1
# Version Control: 
# v2.5.0 - Static hardcoded approach (baseline)
# v2.6.0 - Static table descriptions with fallbacks
# v2.7.0 - Fully dynamic real-time BigQuery discovery system
# v2.7.1 - Fixed quote handling for dataset names from MCP responses

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
VERSION = "2.7.1"
PREVIOUS_STABLE_VERSION = "2.7.0"

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
    title="Marketing Intelligence API",
    description=f"Fixed Quote Handling for BigQuery Discovery v{VERSION}",
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

# NEW v2.7.1: Proper quote cleaning for MCP responses
def clean_mcp_response_text(text: str) -> str:
    """
    Clean text from MCP responses that might include quotes or other formatting
    v2.7.1 - Handles quotes from JSON responses properly
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
            result = await bigquery_mcp.list_datasets(proj)
            datasets = []
            
            if result and 'content' in result:
                for item in result['content']:
                    if isinstance(item, dict) and 'text' in item:
                        # FIX v2.7.1: Clean quotes from dataset names
                        raw_name = item['text']
                        dataset_name = clean_mcp_response_text(raw_name)
                        
                        if dataset_name:  # Only add non-empty dataset names
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
                # FIX v2.7.1: Use cleaned dataset name for API call
                logger.info(f"Listing tables for dataset: '{ds}' in project: '{proj}'")
                result = await bigquery_mcp.list_tables(ds, proj)
                tables = []
                
                if result and 'content' in result:
                    for item in result['content']:
                        if isinstance(item, dict) and 'text' in item:
                            # FIX v2.7.1: Clean quotes from table names too
                            raw_name = item['text']
                            table_name = clean_mcp_response_text(raw_name)
                            
                            if table_name:  # Only add non-empty table names
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

def generate_table_description_v2(table_name: str, dataset_name: str = None, project_name: str = None) -> str:
    """
    Enhanced dynamic table description generator v2.7.1
    Uses project, dataset, and table context for better descriptions
    """
    table_lower = table_name.lower()
    dataset_lower = dataset_name.lower() if dataset_name else ""
    
    # Context-aware descriptions using dataset information
    if 'marketing' in dataset_lower or 'ads' in dataset_lower:
        base_context = "Marketing"
    elif 'analytics' in dataset_lower or 'ga4' in dataset_lower:
        base_context = "Analytics"
    elif 'sales' in dataset_lower or 'revenue' in dataset_lower:
        base_context = "Sales"
    elif 'customer' in dataset_lower or 'user' in dataset_lower:
        base_context = "Customer"
    elif 'shipstation' in dataset_lower:
        base_context = "Shipping"
    else:
        base_context = "Business"
    
    # Enhanced pattern matching with dataset context
    if table_lower.startswith('ads_'):
        table_type = table_lower.replace('ads_', '').replace('_daily', '').replace('_', ' ')
        return f"Google Ads {table_type} {base_context.lower()} data"
    
    elif table_lower.startswith('ga4_'):
        table_type = table_lower.replace('ga4_', '').replace('_', ' ')
        return f"Google Analytics 4 {table_type} {base_context.lower()}"
    
    elif table_lower.startswith('facebook_') or table_lower.startswith('fb_'):
        table_type = table_lower.replace('facebook_', '').replace('fb_', '').replace('_', ' ')
        return f"Facebook Ads {table_type} {base_context.lower()}"
    
    elif table_lower.startswith('linkedin_'):
        table_type = table_lower.replace('linkedin_', '').replace('_', ' ')
        return f"LinkedIn Ads {table_type} {base_context.lower()}"
    
    elif table_lower.startswith('tiktok_'):
        table_type = table_lower.replace('tiktok_', '').replace('_', ' ')
        return f"TikTok Ads {table_type} {base_context.lower()}"
    
    elif 'shipment' in table_lower or 'shipping' in table_lower:
        return f"Shipment and logistics {base_context.lower()} data"
    
    elif table_lower.startswith('dim_'):
        dimension = table_lower.replace('dim_', '').replace('_', ' ')
        return f"Dimension table for {dimension} analysis"
    
    elif 'performance' in table_lower:
        if '_' in table_lower:
            platform = table_lower.split('_')[0].title()
            return f"{platform} performance {base_context.lower()} data"
        else:
            return f"{base_context} performance metrics"
    
    elif 'campaign' in table_lower:
        return f"Campaign-level {base_context.lower()} data"
    
    elif 'keyword' in table_lower:
        return f"Keyword and search term {base_context.lower()} data"
    
    elif 'user' in table_lower or 'customer' in table_lower:
        return f"User behavior and demographic {base_context.lower()} data"
    
    elif 'conversion' in table_lower:
        return f"Conversion tracking and attribution {base_context.lower()} data"
    
    elif 'revenue' in table_lower or 'sales' in table_lower:
        return f"Revenue and sales {base_context.lower()} data"
    
    elif 'audience' in table_lower:
        return f"Audience segmentation {base_context.lower()} data"
    
    elif 'creative' in table_lower or 'asset' in table_lower:
        return f"Creative assets and ad {base_context.lower()} data"
    
    elif table_lower.endswith('_daily'):
        base_name = table_lower.replace('_daily', '').replace('_', ' ')
        return f"Daily {base_name} {base_context.lower()} metrics"
    
    elif table_lower.endswith('_weekly'):
        base_name = table_lower.replace('_weekly', '').replace('_', ' ')
        return f"Weekly {base_name} {base_context.lower()} aggregates"
    
    elif table_lower.endswith('_monthly'):
        base_name = table_lower.replace('_monthly', '').replace('_', ' ')
        return f"Monthly {base_name} {base_context.lower()} summary"
    
    elif 'master' in table_lower or 'consolidated' in table_lower:
        return f"Consolidated {base_context.lower()} data from multiple sources"
    
    else:
        # Intelligent fallback using table name structure
        clean_name = table_lower.replace('_', ' ').title()
        return f"{clean_name} - {base_context} analytics table"

async def generate_sql_with_ai_dynamic(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """Use GPT-4o-mini to convert natural language to BigQuery SQL with dynamic table selection"""
    if not openai_client:
        raise Exception("OpenAI client not available for SQL generation")
    
    # Find the most relevant table based on the question
    relevant_table_info = await select_best_table_for_query(question, available_tables)
    
    if not relevant_table_info:
        raise Exception("No suitable table found for the query")
    
    project, dataset, table = relevant_table_info
    
    # Get schema for the selected table
    table_schema = await get_table_schema_dynamic(project, dataset, table)
    
    # Extract column information from schema
    columns_info = []
    if "content" in table_schema and table_schema["content"]:
        try:
            schema_text = table_schema["content"][0]["text"]
            schema_data = json.loads(schema_text)
            for field in schema_data.get("Schema", []):
                columns_info.append({
                    "name": field["Name"],
                    "type": field["Type"],
                    "description": field.get("Description", "")
                })
        except Exception as e:
            logger.error(f"Schema parsing error: {e}")
    
    columns_text = "\n".join([f"- {col['name']} ({col['type']})" for col in columns_info])
    
    prompt = f"""You are an expert BigQuery SQL analyst. Convert this natural language question into a valid BigQuery SQL query.

DATABASE SCHEMA:
Table: `{project}.{dataset}.{table}`
Available Columns:
{columns_text}

QUESTION: {question}

REQUIREMENTS:
1. Use proper BigQuery syntax and functions
2. Always include the full table name: `{project}.{dataset}.{table}`
3. Use appropriate aggregation functions (SUM, AVG, COUNT, etc.)
4. Include reasonable date filters (typically last 30 days unless specified otherwise)
5. Add LIMIT clause to prevent huge results (typically 10-50 rows)
6. Handle potential division by zero with NULLIF()
7. Use exact column names from the schema above
8. For percentage calculations, multiply by 100 and round appropriately
9. Round decimal values to 2 decimal places using ROUND()
10. Use proper GROUP BY clauses when using aggregation functions
11. Order results by the most relevant metric

Generate ONLY the SQL query, no explanations or markdown:"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.1
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # Clean up the response
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
        
        logger.info(f"Generated SQL for table {project}.{dataset}.{table}: {sql_query[:100]}...")
        return sql_query, (project, dataset, table)
        
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        # Fallback to simple query
        fallback_sql = f"""
        SELECT *
        FROM `{project}.{dataset}.{table}`
        ORDER BY 1 DESC
        LIMIT 20
        """
        return fallback_sql, (project, dataset, table)

async def select_best_table_for_query(question: str, available_tables: Dict[str, Dict[str, List[str]]]) -> tuple:
    """Select the most relevant table for a given query using AI"""
    if not openai_client:
        # Fallback logic
        question_lower = question.lower()
        
        for project, datasets in available_tables.items():
            for dataset, tables in datasets.items():
                for table in tables:
                    if any(keyword in question_lower for keyword in ['campaign', 'ads']) and 'campaign' in table:
                        return (project, dataset, table)
                    elif 'performance' in question_lower and 'performance' in table:
                        return (project, dataset, table)
        
        # Return first available table if no match
        for project, datasets in available_tables.items():
            for dataset, tables in datasets.items():
                if tables:
                    return (project, dataset, tables[0])
        
        return None
    
    # Prepare table options for AI selection
    table_options = []
    for project, datasets in available_tables.items():
        for dataset, tables in datasets.items():
            for table in tables:
                description = generate_table_description_v2(table, dataset, project)
                table_options.append({
                    "project": project,
                    "dataset": dataset, 
                    "table": table,
                    "description": description
                })
    
    if not table_options:
        return None
    
    options_text = "\n".join([
        f"- {opt['project']}.{opt['dataset']}.{opt['table']}: {opt['description']}"
        for opt in table_options
    ])
    
    prompt = f"""Given this question: "{question}"

Available tables:
{options_text}

Select the MOST RELEVANT table for answering this question. Respond with only the project.dataset.table format (e.g., "project-name.dataset-name.table-name").

If the question is about campaigns, ads performance, or marketing metrics, choose ads/campaign tables.
If it's about user behavior or analytics, choose ga4 or analytics tables.
If it's about consolidated reporting, choose master or consolidated tables.

Your response:"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=50,
            temperature=0.1
        )
        
        selected = response.choices[0].message.content.strip()
        
        # Parse the response
        if '.' in selected:
            parts = selected.split('.')
            if len(parts) == 3:
                return tuple(parts)
        
    except Exception as e:
        logger.error(f"Table selection error: {e}")
    
    # Fallback to first table
    return (table_options[0]['project'], table_options[0]['dataset'], table_options[0]['table'])

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
        formatted_response += f"\n\n---\nAnalysis based on {len(result['content'])} records from BigQuery"
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"Response formatting error: {e}")
        return f"Retrieved {len(result.get('content', []))} records from BigQuery for: {question}"

async def handle_bigquery_tables_query_dynamic(question: str) -> dict:
    """
    Handle table/dataset listing with fully dynamic discovery
    Version: 2.7.1 - Fixed quote handling in MCP responses
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
        
        # Create comprehensive response
        answer = f"I discovered {len(projects)} projects with {total_datasets} datasets and {total_tables} tables. Here's what's available:\n\n"
        
        table_counter = 1
        
        for project, datasets in all_tables.items():
            if not datasets:
                continue
                
            answer += f"PROJECT: {project}\n"
            
            for dataset, tables in datasets.items():
                if not tables:
                    continue
                    
                answer += f"\n  Dataset: {dataset} ({len(tables)} tables)\n"
                
                for table in tables:
                    description = generate_table_description_v2(table, dataset, project)
                    answer += f"  {table_counter}. {table}\n     {description}\n"
                    table_counter += 1
                
                answer += "\n"
        
        # Add dynamic sample questions based on discovered tables
        answer += "SAMPLE QUESTIONS YOU CAN ASK:\n"
        
        # Generate dynamic sample questions based on actual table names
        sample_questions = []
        for datasets in all_tables.values():
            for tables in datasets.values():
                for table in tables:
                    if 'campaign' in table.lower():
                        sample_questions.append("• Show me campaign performance metrics")
                    elif 'performance' in table.lower():
                        sample_questions.append("• What are the top performing campaigns?")
                    elif 'conversion' in table.lower():
                        sample_questions.append("• Compare conversion rates by device")
                    elif 'revenue' in table.lower() or 'sales' in table.lower():
                        sample_questions.append("• Analyze revenue trends")
                    elif 'user' in table.lower() or 'customer' in table.lower():
                        sample_questions.append("• Show me user behavior patterns")
                    elif 'shipment' in table.lower():
                        sample_questions.append("• Analyze shipping performance")
                
                # Limit to 5 unique sample questions
                if len(set(sample_questions)) >= 5:
                    break
            if len(set(sample_questions)) >= 5:
                break
        
        # Add the unique sample questions
        for question in list(set(sample_questions))[:5]:
            answer += f"{question}\n"
        
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
            "processing_method": "dynamic_discovery_v2.7.1",
            "version": VERSION
        }
        
    except Exception as e:
        logger.error(f"Dynamic table discovery error: {e}")
        return {
            "answer": f"I encountered an error while discovering your BigQuery resources:\n\n{str(e)}\n\nThis might be due to permissions or connection issues. The system is designed to automatically discover all projects, datasets, and tables, but requires proper BigQuery access.",
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
    """Enhanced unified endpoint with fully dynamic BigQuery discovery"""
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
            # Dynamic table query detection with fixed quote handling
            table_result = await handle_bigquery_tables_query_dynamic(request.question)
            
            if table_result is not None:
                # This is a table listing query
                processing_time = time.time() - process_start
                
                return {
                    **table_result,
                    "query_type": "metadata_query",
                    "sources_used": 1,
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery",
                    "sql_query": None
                }
            
            else:
                # Regular SQL query with dynamic table selection
                available_tables = await discover_all_tables()
                sql_query, table_info = await generate_sql_with_ai_dynamic(request.question, available_tables)
                result = await bigquery_mcp.execute_sql(sql_query)
                processing_time = time.time() - process_start
                
                # Check if query was successful
                if result.get("isError"):
                    error_message = "Unknown error"
                    if result.get("content") and len(result["content"]) > 0:
                        error_content = result["content"][0]
                        if isinstance(error_content, dict) and "text" in error_content:
                            error_message = error_content["text"]
                    
                    return {
                        "answer": f"I encountered an error executing your BigQuery query on table {'.'.join(table_info)}. The error was: {error_message}",
                        "error": error_message,
                        "sql_query": sql_query,
                        "table_used": table_info,
                        "query_type": "quantitative_error",
                        "processing_method": "dynamic_sql_error",
                        "sources_used": 0,
                        "processing_time": processing_time,
                        "response_style": request.preferred_style,
                        "data_source": "bigquery"
                    }
                
                # Use enhanced Claude-style formatting
                claude_style_answer = await format_bigquery_results_like_claude(result, request.question, sql_query)
                
                return {
                    "answer": claude_style_answer,
                    "data": result,
                    "sql_query": sql_query,
                    "table_used": table_info,
                    "query_type": "quantitative",
                    "processing_method": "dynamic_table_selection_v2.7.1",
                    "sources_used": 1,
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery"
                }
            
    except Exception as e:
        logger.error(f"Unified query failed: {e}")
        processing_time = time.time() - process_start
        return {
            "answer": f"Sorry, I encountered an error: {str(e)}",
            "query_type": "error",
            "processing_method": "error",
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
            }
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/api/bigquery/clear-cache")
async def clear_discovery_cache():
    """Clear the discovery cache to force fresh discovery"""
    global discovery_cache
    discovery_cache = BigQueryDiscoveryCache(cache_ttl_minutes=5)
    
    return {
        "status": "success",
        "message": "Discovery cache cleared. Next queries will perform fresh discovery.",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/bigquery/test")
async def test_bigquery_connection():
    """Test BigQuery MCP connection"""
    try:
        test_result = await bigquery_mcp.test_connection()
        return {
            "server_url": bigquery_mcp.server_url,
            "connection_test": test_result,
            "environment": os.getenv("ENVIRONMENT", "production")
        }
    except Exception as e:
        return {
            "server_url": bigquery_mcp.server_url,
            "error": str(e),
            "status": "failed"
        }

@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Comprehensive health check including BigQuery MCP"""
    systems = {}
    
    systems["core_modules"] = "available" if CORE_MODULES_AVAILABLE else "unavailable"
    systems["dashboard_routes"] = "available" if DASHBOARD_AVAILABLE else "unavailable"
    
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
        test_result = await bigquery_mcp.test_connection()
        systems["bigquery_mcp"] = test_result.get("status", "unknown")
    except Exception as e:
        systems["bigquery_mcp"] = f"error: {str(e)[:50]}"
    
    # v2.7.1: Cache health check
    systems["discovery_cache"] = f"active ({len(discovery_cache.last_discovery)} cached items)"
    
    critical_systems = ["openai_key", "supabase_url", "supabase_key"]
    healthy_systems = sum(1 for sys in critical_systems if systems.get(sys) == "configured")
    
    if healthy_systems == len(critical_systems):
        if systems.get("database") == "healthy" or systems.get("supabase_client") == "available":
            overall_status = "healthy"
        else:
            overall_status = "degraded"
    else:
        overall_status = "unhealthy"
    
    return HealthResponse(
        status=overall_status,
        systems=systems,
        timestamp=datetime.now().isoformat(),
        environment=os.getenv("ENVIRONMENT", "unknown")
    )

@app.get("/")
async def root():
    """Root endpoint with dynamic discovery information"""
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
        "service": "Marketing Intelligence API",
        "version": VERSION,
        "previous_stable": PREVIOUS_STABLE_VERSION,
        "status": "running",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "discovery_summary": discovery_summary,
        "features": {
            "advanced_rag": CORE_MODULES_AVAILABLE,
            "simple_search": EXTERNAL_CLIENTS_AVAILABLE,
            "ai_sql_generation": openai_client is not None,
            "ai_model": "gpt-4o-mini",
            "dynamic_discovery": True,
            "real_time_tables": True,
            "intelligent_table_selection": True,
            "cache_management": True,
            "multi_project_support": True,
            "auto_scaling": True,
            "quote_handling_fixed": True  # NEW v2.7.1
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
            "v2.7.1": [
                "Fixed quote handling for dataset names in MCP responses",
                "Added proper text cleaning for all MCP JSON responses",
                "Enhanced logging for dataset/table discovery debugging",
                "Improved error handling for malformed dataset names",
                "Fixed BigQuery API compatibility issues"
            ],
            "v2.7.0": [
                "Fully dynamic BigQuery resource discovery",
                "Real-time table and dataset detection",
                "Intelligent table selection for queries",
                "Multi-project support",
                "Smart caching with TTL",
                "Automatic scaling for new resources"
            ]
        }
    }

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on app shutdown"""
    await bigquery_mcp.close()

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting Fixed Quote Handling System v{VERSION}")
    logger.info(f"Server: {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"Features: Dynamic discovery with fixed quote handling")
    logger.info(f"Cache TTL: 5 minutes")
    logger.info(f"Multi-project support: Enabled")
    logger.info(f"BigQuery MCP: {bigquery_mcp.server_url}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )