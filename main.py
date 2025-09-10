# main.py - Updated Production FastAPI Application with AI-Powered BigQuery
import os
import logging
import time
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, Dict, Literal
from dotenv import load_dotenv

# BigQuery MCP integration
from api.bigquery_mcp import bigquery_mcp

# Add this request model
class UnifiedQueryRequest(BaseModel):
    question: str
    data_source: Literal["rag", "bigquery"] = "rag"
    preferred_style: str = "standard"

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
    description="Production RAG system with AI-powered BigQuery integration",
    version="2.0.0",
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

# Pydantic models
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
    """Use OpenAI to convert natural language to BigQuery SQL"""
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
            model="gpt-4-turbo-preview",
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
    """Simple Supabase vector search for basic queries"""
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
                model="gpt-4-turbo-preview",
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
    """Format response according to requested style"""
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
            model="gpt-4-turbo-preview",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.3
        )
        
        return response.choices[0].message.content.strip()
    
    except Exception as e:
        logger.error(f"Response formatting error: {e}")
        return answer

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

@app.post("/api/unified-query")
async def unified_query(request: UnifiedQueryRequest):
    """Unified endpoint with AI-powered BigQuery SQL generation"""
    process_start = time.time()
    
    try:
        if request.data_source == "rag":
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
            # Get table schema for AI SQL generation
            table_schema = await get_cached_table_schema()
            
            # Generate SQL using AI
            sql_query = await generate_sql_with_ai(request.question, table_schema)
            
            # Execute the generated SQL
            result = await bigquery_mcp.execute_sql(sql_query)
            
            processing_time = time.time() - process_start
            
            # Check if query was successful
            if result.get("isError"):
                return {
                    "answer": f"I encountered an error with your BigQuery query: {request.question}",
                    "error": result.get("content", [{}])[0].get("text", "Unknown error"),
                    "sql_query": sql_query,
                    "query_type": "quantitative_error",
                    "processing_method": "ai_generated_sql",
                    "sources_used": 0,
                    "processing_time": processing_time,
                    "response_style": request.preferred_style,
                    "data_source": "bigquery"
                }
            
            return {
                "answer": f"Here are the BigQuery results for: {request.question}",
                "data": result,
                "sql_query": sql_query,
                "query_type": "quantitative",
                "processing_method": "ai_generated_sql",
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
        "version": "2.0.0",
        "status": "running",
        "uptime_seconds": round(uptime, 2),
        "environment": os.getenv("ENVIRONMENT", "unknown"),
        "features": {
            "advanced_rag": CORE_MODULES_AVAILABLE,
            "simple_search": EXTERNAL_CLIENTS_AVAILABLE,
            "ai_sql_generation": True,
            "bigquery_mcp": True,
            "intelligent_routing": True,
            "response_formatting": True
        },
        "endpoints": {
            "chat": "/api/chat",
            "unified_query": "/api/unified-query", 
            "bigquery_test": "/api/bigquery/test",
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
    logger.info(f"AI SQL Generation: {openai_client is not None}")
    
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=os.getenv("ENVIRONMENT") == "development",
        log_level="info"
    )