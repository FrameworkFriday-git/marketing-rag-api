# main.py - Complete Enhanced FastAPI Application with GPT-4o-mini Integration
import os
import logging
import time
import json
import re
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Literal, List, Any
from dotenv import load_dotenv

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
    description="Production RAG system with GPT-4o-mini BigQuery integration",
    version="2.4.0",
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
table_schema_cache = {}

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

async def get_cached_table_schema():
    """Get table schema with caching"""
    cache_key = "ads_campaign_performance_daily"
    
    if cache_key not in table_schema_cache:
        table_schema_cache[cache_key] = await bigquery_mcp.get_table_info(
            dataset="new_data_tables",
            table="ads_campaign_performance_daily",
            project="data-tables-for-zoho"
        )
    
    return table_schema_cache[cache_key]

async def generate_sql_with_ai(question: str, table_schema: dict) -> str:
    """Use GPT-4o-mini to convert natural language to BigQuery SQL"""
    if not openai_client:
        raise Exception("OpenAI client not available for SQL generation")
    
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
Table: `data-tables-for-zoho.new_data_tables.ads_campaign_performance_daily`
Available Columns:
{columns_text}

QUESTION: {question}

REQUIREMENTS:
1. Use proper BigQuery syntax and functions
2. Always include the full table name: `data-tables-for-zoho.new_data_tables.ads_campaign_performance_daily`
3. Use appropriate aggregation functions (SUM, AVG, COUNT, etc.)
4. Include reasonable date filters (typically last 30 days unless specified otherwise)
5. Add LIMIT clause to prevent huge results (typically 10-50 rows)
6. Handle potential division by zero with NULLIF()
7. Use exact column names from the schema above (e.g., 'cost_dollars', not 'cost')
8. For percentage calculations, multiply by 100 and round appropriately
9. Round decimal values to 2 decimal places using ROUND()
10. Use proper GROUP BY clauses when using aggregation functions
11. Order results by the most relevant metric (cost, roas, date, etc.)

COLUMN NAME MAPPING:
- Use 'cost_dollars' for cost/spend
- Use 'conversion_value' for revenue
- Use 'ctr_percent' for CTR (already as percentage)
- Use 'avg_cpc_dollars' for CPC
- Use 'conversion_rate_percent' for conversion rate (already as percentage)
- Use 'segments_device' for device analysis
- Use 'campaign_advertising_channel_type' for campaign type analysis

EXAMPLES:
- "top campaigns by cost" → ORDER BY SUM(cost_dollars) DESC
- "campaigns with highest ROAS" → ORDER BY AVG(roas) DESC  
- "device performance" → GROUP BY segments_device
- "daily trends" → GROUP BY DATE(date), ORDER BY date
- "conversion rates above 5%" → WHERE conversion_rate_percent > 5

Generate ONLY the SQL query, no explanations or markdown:"""

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=600,
            temperature=0.1
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # Clean up the response (remove markdown formatting if present)
        if sql_query.startswith("```sql"):
            sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        elif sql_query.startswith("```"):
            sql_query = sql_query.replace("```", "").strip()
        
        logger.info(f"Generated SQL: {sql_query[:100]}...")
        return sql_query
        
    except Exception as e:
        logger.error(f"SQL generation failed: {e}")
        # Fallback to simple query
        return """
        SELECT campaign_name, date, impressions, clicks, cost_dollars, conversions, roas
        FROM `data-tables-for-zoho.new_data_tables.ads_campaign_performance_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ORDER BY date DESC
        LIMIT 20
        """

async def extract_campaign_metrics(result: dict) -> list:
    """Extract structured campaign metrics from BigQuery results"""
    campaigns = []
    
    try:
        for item in result.get('content', []):
            if isinstance(item, dict) and 'text' in item:
                text_data = item['text']
                
                # Try to parse JSON-like data
                if '{' in text_data and '}' in text_data:
                    # Extract key metrics using regex
                    metrics = {}
                    
                    # Extract common metrics
                    patterns = {
                        'conversion_rate': r'"avg_conversion_rate_percent":([0-9.]+)',
                        'cpc': r'"avg_cpc_dollars":([0-9.]+)',
                        'ctr': r'"avg_ctr_percent":([0-9.]+)',
                        'roas': r'"avg_roas":([0-9.]+)',
                        'cost': r'"total_cost_dollars":([0-9.]+)',
                        'clicks': r'"total_clicks":([0-9]+)',
                        'conversions': r'"total_conversions":([0-9.]+)',
                        'campaign_name': r'"campaign_name":"([^"]+)"'
                    }
                    
                    for metric, pattern in patterns.items():
                        match = re.search(pattern, text_data)
                        if match:
                            metrics[metric] = match.group(1)
                    
                    if metrics:
                        campaigns.append(metrics)
        
        return campaigns
        
    except Exception as e:
        logger.error(f"Metrics extraction error: {e}")
        return []

async def format_bigquery_results_like_claude(result: dict, question: str, sql_query: str) -> str:
    """Format BigQuery results with clean text formatting for chat display"""
    
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
        
        # Enhanced prompt for clean text formatting (no HTML/markdown)
        analysis_prompt = f"""You are an expert marketing data analyst. Analyze this BigQuery campaign performance data and provide insights in a clean, structured text format.

ORIGINAL QUESTION: {question}

RAW DATA FROM BIGQUERY:
{data_text}

RESPONSE REQUIREMENTS:
1. Start with a brief executive summary (2-3 sentences)
2. Use clean text formatting:
   - Use "EXECUTIVE SUMMARY" for main headers (all caps)
   - Use "Performance Breakdown" for section headers (title case)
   - Use bullet points (•) for lists
   - Use numbers (1., 2., 3.) for recommendations
   - Use **bold text** for emphasis on key numbers
3. Include specific numbers and percentages
4. Group by account/property if multiple are present
5. End with actionable recommendations

STRUCTURE YOUR RESPONSE EXACTLY LIKE THIS:

EXECUTIVE SUMMARY
[Brief 2-3 sentence overview]

Performance Breakdown

BudgetMailboxes.com:
• Top campaign: **[Campaign name]** - [Key metrics]
• Key metric: **[Specific number with context]**
• Notable finding: [Insight]

MailboxWorks.com: (if applicable)
• [Similar structure]

Key Insights
• [Insight 1 with supporting data]
• [Insight 2 with supporting data]  
• [Insight 3 with supporting data]

Recommendations
1. **[Actionable recommendation 1]**
2. **[Actionable recommendation 2]**
3. **[Actionable recommendation 3]**

Make this professional, data-driven, and easy to read in a chat interface."""

        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": analysis_prompt}],
            max_tokens=1000,
            temperature=0.3
        )
        
        formatted_response = response.choices[0].message.content.strip()
        
        # Add data freshness note with clean formatting
        formatted_response += f"\n\n---\n*Analysis based on {len(result['content'])} records from BigQuery*"
        
        return formatted_response
        
    except Exception as e:
        logger.error(f"Claude-style formatting error: {e}")
        # Enhanced fallback formatting
        try:
            row_count = len(result['content']) if result.get('content') else 0
            
            if 'campaign' in question.lower():
                data_type = "CAMPAIGN PERFORMANCE ANALYSIS"
            elif 'cost' in question.lower() or 'spend' in question.lower():
                data_type = "COST ANALYSIS"
            elif 'conversion' in question.lower():
                data_type = "CONVERSION METRICS ANALYSIS"
            elif 'roas' in question.lower():
                data_type = "ROAS PERFORMANCE ANALYSIS"
            else:
                data_type = "DATA ANALYSIS"
            
            return f"{data_type}\n\nRetrieved **{row_count} records** from BigQuery. The raw data shows various campaign metrics including cost, conversions, and performance indicators.\n\n*Unable to provide detailed analysis due to processing limitations.*"
            
        except:
            return f"Here are the BigQuery results for: {question}"

async def handle_bigquery_tables_query(question: str) -> dict:
    """Handle table/dataset listing with natural language responses like Claude MCP"""
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
        
        # List datasets first
        datasets_result = await bigquery_mcp.list_datasets("data-tables-for-zoho")
        
        if datasets_result and 'content' in datasets_result:
            datasets_list = []
            for item in datasets_result['content']:
                if isinstance(item, dict) and 'text' in item:
                    dataset_name = item['text'].strip()
                    datasets_list.append(dataset_name)
            
            # Get tables for the main dataset
            try:
                tables_result = await bigquery_mcp.list_tables("new_data_tables", "data-tables-for-zoho")
                tables_list = []
                
                if tables_result and 'content' in tables_result:
                    for item in tables_result['content']:
                        if isinstance(item, dict) and 'text' in item:
                            table_name = item['text'].strip()
                            tables_list.append(table_name)
                
                # Create natural language response like Claude MCP
                answer = f"I found {len(datasets_list)} datasets in your BigQuery project. Here are the main data tables available:\n\n"
                
                # List tables with descriptions in natural language
                table_descriptions = {
                    'ads_account_performance_daily': 'Google Ads account-level daily performance metrics',
                    'ads_campaign_performance_daily': 'Daily campaign performance data with cost, clicks, conversions',
                    'ads_ad_group_performance_daily': 'Ad group level performance tracking',
                    'ads_ad_performance_daily': 'Individual ad performance metrics',
                    'ads_keyword_performance_daily': 'Keyword-level performance data',
                    'ga4_item_performance': 'Google Analytics 4 product/item performance data',
                    'ga4_user_summary': 'GA4 user behavior and journey summary',
                    'consolidated_master': 'Master table combining data from multiple sources',
                    'dim_data': 'Dimension data for analytics',
                }
                
                for i, table in enumerate(tables_list, 1):
                    description = table_descriptions.get(table, 'Marketing and analytics data')
                    answer += f"{i}. **{table}** - {description}\n"
                
                # Add context about the dataset
                main_dataset = 'new_data_tables'
                if len(datasets_list) > 1:
                    for ds in datasets_list:
                        if 'new_data' in ds.lower():
                            main_dataset = ds
                            break
                
                answer += f"\nThese tables are stored in the '{main_dataset}' dataset and contain comprehensive marketing performance data spanning Google Ads campaigns, Google Analytics, and consolidated reporting metrics."
                
                # Add helpful context
                answer += "\n\nYou can ask me questions like:"
                answer += "\n• 'Show me campaign performance metrics'"
                answer += "\n• 'What are the top performing campaigns?'"
                answer += "\n• 'Compare conversion rates by device'"
                answer += "\n• 'Give me a summary about budget mailboxes'"
                
                return {
                    "answer": answer,
                    "data": {
                        "content": [
                            {
                                "type": "datasets",
                                "count": len(datasets_list),
                                "items": datasets_list
                            },
                            {
                                "type": "tables", 
                                "dataset": "new_data_tables",
                                "count": len(tables_list),
                                "items": tables_list
                            }
                        ]
                    },
                    "processing_time": 1.5,
                    "processing_method": "mcp_metadata_query"
                }
                
            except Exception as tables_error:
                answer = f"I found {len(datasets_list)} datasets, but encountered an issue accessing table details: {str(tables_error)}"
                return {
                    "answer": answer,
                    "processing_time": 1.0,
                    "processing_method": "mcp_metadata_partial"
                }
        else:
            return {
                "answer": "I couldn't access your BigQuery datasets. This might be due to permissions or connection issues with the BigQuery service.",
                "error": "Could not retrieve dataset information",
                "processing_time": 0.5
            }
            
    except Exception as e:
        logger.error(f"Table listing error: {e}")
        return {
            "answer": f"I encountered an error while trying to access your BigQuery resources: {str(e)}. Please check your BigQuery connection and try again.",
            "error": str(e),
            "processing_time": 0.5
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
    """Enhanced unified endpoint with better query classification"""
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
            # Enhanced table query detection
            table_result = await handle_bigquery_tables_query(request.question)
            
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
                # Regular SQL query
                table_schema = await get_cached_table_schema()
                sql_query = await generate_sql_with_ai(request.question, table_schema)
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
                        "answer": f"I encountered an error executing your BigQuery query. The error was: {error_message}",
                        "error": error_message,
                        "sql_query": sql_query,
                        "query_type": "quantitative_error",
                        "processing_method": "ai_generated_sql_error",
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
                    "query_type": "quantitative",
                    "processing_method": "gpt4o_mini_analysis",
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
            "error_details": str(e)
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

@app.get("/api/bigquery/table-schema")
async def get_table_schema():
    """Get the schema of the campaign performance table"""
    try:
        result = await get_cached_table_schema()
        return {
            "table_schema": result,
            "server_url": bigquery_mcp.server_url
        }
    except Exception as e:
        return {
            "error": str(e),
            "server_url": bigquery_mcp.server_url
        }

@app.get("/api/bigquery/datasets")
async def list_bigquery_datasets():
    """List available BigQuery datasets"""
    try:
        result = await bigquery_mcp.list_datasets("data-tables-for-zoho")
        return {
            "status": "success",
            "datasets": result,
            "server_url": bigquery_mcp.server_url
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "server_url": bigquery_mcp.server_url
        }

@app.get("/api/bigquery/tables/{dataset}")
async def list_bigquery_tables(dataset: str):
    """List tables in a specific dataset"""
    try:
        result = await bigquery_mcp.list_tables(dataset, "data-tables-for-zoho")
        return {
            "status": "success",
            "dataset": dataset,
            "tables": result,
            "server_url": bigquery_mcp.server_url
        }
    except Exception as e:
        return {
            "status": "error", 
            "error": str(e),
            "dataset": dataset,
            "server_url": bigquery_mcp.server_url
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
    """Root endpoint"""
    uptime = time.time() - start_time
    return {
        "service": "Marketing Intelligence API",
        "version": "2.4.0",
        "status": "running",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "features": {
            "advanced_rag": CORE_MODULES_AVAILABLE,
            "simple_search": EXTERNAL_CLIENTS_AVAILABLE,
            "ai_sql_generation": openai_client is not None,
            "ai_model": "gpt-4o-mini",
            "claude_style_formatting": "enhanced_clean_text",
            "natural_language_tables": True,
            "bigquery_mcp": True,
            "intelligent_routing": True,
            "response_formatting": True,
            "table_metadata": True,
            "structured_responses": True
        },
        "endpoints": {
            "chat": "/api/chat",
            "unified_query": "/api/unified-query", 
            "bigquery_test": "/api/bigquery/test",
            "bigquery_datasets": "/api/bigquery/datasets",
            "bigquery_tables": "/api/bigquery/tables/{dataset}",
            "health": "/api/health",
            "docs": "/docs" if os.getenv("ENVIRONMENT") == "development" else "disabled"
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
    
    logger.info(f"Starting server on {host}:{port}")
    logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'unknown')}")
    logger.info(f"Advanced RAG available: {CORE_MODULES_AVAILABLE}")
    logger.info(f"External clients available: {EXTERNAL_CLIENTS_AVAILABLE}")
    logger.info(f"AI Model: GPT-4o-mini")
    logger.info(f"Natural language table responses: Enabled")
    logger.info(f"BigQuery MCP: {bigquery_mcp.server_url}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )