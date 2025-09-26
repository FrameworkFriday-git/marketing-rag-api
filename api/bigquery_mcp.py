# 1. First, fix api/bigquery_mcp.py - Remove the circular import
# api/bigquery_mcp.py - Basic Working MCP Connector (FIXED)
import asyncio
import json
import aiohttp
import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class BigQueryMCP:
    def __init__(self):
        self.server_url = os.getenv("BIGQUERY_MCP_URL", "https://bigquery.fridaysolutions.ai/mcp")
        self.session = None
        self.request_id = 0

    async def get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=120)
            self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def call_tool(self, tool_name: str, parameters: dict) -> dict:
        """Call a tool on the MCP server via HTTP"""
        self.request_id += 1
        
        request_data = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": parameters
            }
        }

        try:
            session = await self.get_session()
            
            async with session.post(
                self.server_url,
                json=request_data,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
            ) as response:
                
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"HTTP {response.status}: {error_text}")
                    return {
                        "isError": True,
                        "error": f"HTTP {response.status}: {error_text}"
                    }
                
                response_data = await response.json()
                
                if "error" in response_data:
                    error_msg = response_data['error'].get('message', 'Unknown error')
                    logger.error(f"MCP Error for {tool_name}: {error_msg}")
                    return {
                        "isError": True,
                        "error": error_msg
                    }
                
                return response_data.get("result", {})
                
        except asyncio.TimeoutError:
            logger.error(f"MCP call timeout: {tool_name}")
            return {
                "isError": True,
                "error": "Request timeout"
            }
        except Exception as e:
            logger.error(f"MCP call failed: {tool_name} - {e}")
            return {
                "isError": True,
                "error": str(e)
            }

    async def list_dataset_ids(self, project: str = None) -> dict:
        """List BigQuery datasets"""
        parameters = {}
        if project:
            parameters["project"] = project
            
        logger.info(f"Listing datasets for project: {project or 'default'}")
        return await self.call_tool("list_dataset_ids", parameters)

    async def list_table_ids(self, dataset: str, project: str = None) -> dict:
        """List tables in a dataset"""
        parameters = {"dataset": dataset}
        if project:
            parameters["project"] = project
            
        logger.info(f"Listing tables in dataset: {dataset}")
        return await self.call_tool("list_table_ids", parameters)

    async def get_table_info(self, dataset: str, table: str, project: str = None) -> dict:
        """Get table schema and metadata"""
        parameters = {
            "dataset": dataset,
            "table": table
        }
        if project:
            parameters["project"] = project
            
        logger.info(f"Getting table info: {dataset}.{table}")
        return await self.call_tool("get_table_info", parameters)

    async def execute_sql(self, sql: str, dry_run: bool = False) -> dict:
        """Execute SQL query"""
        parameters = {
            "sql": sql,
            "dry_run": dry_run
        }
        
        logger.info(f"Executing SQL: {sql[:100]}...")
        
        result = await self.call_tool("execute_sql", parameters)
        
        # Log result summary
        if result.get("isError"):
            logger.error(f"SQL execution failed: {result.get('error')}")
        else:
            content_count = len(result.get("content", []))
            logger.info(f"SQL execution successful: {content_count} rows returned")
        
        return result

    async def get_dataset_info(self, dataset: str, project: str = None) -> dict:
        """Get dataset metadata"""
        parameters = {"dataset": dataset}
        if project:
            parameters["project"] = project
            
        logger.info(f"Getting dataset info: {dataset}")
        return await self.call_tool("get_dataset_info", parameters)

    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.info("BigQuery MCP session closed")

# Global instance - FIXED: No circular import
bigquery_mcp = BigQueryMCP()

# 2. Create api/enhanced_bigquery_mcp.py with proper imports
# api/enhanced_bigquery_mcp.py - MCP-Powered Intelligent System (FIXED IMPORTS)
import os
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import asyncio

# Import the MCP client directly from the module
from .bigquery_mcp import BigQueryMCP

logger = logging.getLogger(__name__)

class MCPPoweredTableSelector:
    """Uses MCP to intelligently discover and select tables like Claude Desktop"""
    
    def __init__(self):
        self.schema_cache = {}
        self.table_analysis_cache = {}
        # Create our own MCP instance to avoid circular imports
        self.mcp_client = BigQueryMCP()
        
    async def intelligent_table_discovery(self, question: str) -> List[Tuple[str, str, str, float]]:
        """Discover relevant tables using MCP's intelligent capabilities"""
        
        # Get all available tables
        available_tables = await self._discover_all_tables_with_schemas()
        
        # Analyze question intent
        question_analysis = self._analyze_question_semantics(question)
        
        # Score tables based on MCP-powered analysis
        scored_tables = []
        
        for project, datasets in available_tables.items():
            for dataset, tables in datasets.items():
                for table_name, table_info in tables.items():
                    score = await self._calculate_mcp_table_score(
                        question, question_analysis, table_name, table_info
                    )
                    
                    if score > 0:
                        scored_tables.append((project, dataset, table_name, score))
        
        # Sort by score (highest first)
        scored_tables.sort(key=lambda x: x[3], reverse=True)
        
        logger.info(f"MCP Table Discovery Results:")
        for proj, ds, table, score in scored_tables[:5]:
            logger.info(f"  {proj}.{ds}.{table}: {score} points")
        
        return scored_tables
    
    async def _discover_all_tables_with_schemas(self) -> Dict[str, Dict[str, Dict[str, Dict]]]:
        """Discover all tables with their schema information using MCP"""
        
        # Get projects
        projects = await self._get_projects()
        all_tables = {}
        
        for project in projects:
            all_tables[project] = {}
            
            # Get datasets
            datasets_result = await self.mcp_client.list_dataset_ids(project)
            datasets = self._extract_names_from_mcp_result(datasets_result)
            
            for dataset in datasets:
                all_tables[project][dataset] = {}
                
                # Get tables
                tables_result = await self.mcp_client.list_table_ids(dataset, project)
                table_names = self._extract_names_from_mcp_result(tables_result)
                
                for table_name in table_names:
                    # Get schema for each table
                    table_info = await self._get_table_schema_info(project, dataset, table_name)
                    all_tables[project][dataset][table_name] = table_info
        
        return all_tables
    
    async def _get_table_schema_info(self, project: str, dataset: str, table: str) -> Dict:
        """Get comprehensive table schema information using MCP"""
        cache_key = f"{project}.{dataset}.{table}"
        
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
        
        try:
            # Method 1: Try MCP get_table_info
            schema_result = await self.mcp_client.get_table_info(dataset, table, project)
            
            if schema_result and 'content' in schema_result and schema_result['content']:
                schema_text = schema_result['content'][0].get('text', '')
                if schema_text:
                    schema_data = json.loads(schema_text)
                    
                    table_info = {
                        'columns': [],
                        'column_names': [],
                        'numeric_columns': [],
                        'date_columns': [],
                        'string_columns': [],
                        'table_description': schema_data.get('Description', ''),
                        'row_count': schema_data.get('RowCount', 0),
                        'discovery_method': 'mcp_table_info'
                    }
                    
                    for field in schema_data.get('Schema', []):
                        col_name = field.get('Name', '')
                        col_type = field.get('Type', '').upper()
                        
                        table_info['columns'].append({
                            'name': col_name,
                            'type': col_type,
                            'description': field.get('Description', '')
                        })
                        table_info['column_names'].append(col_name)
                        
                        # Categorize columns
                        if col_type in ['INTEGER', 'INT64', 'FLOAT', 'FLOAT64', 'NUMERIC']:
                            table_info['numeric_columns'].append(col_name)
                        elif col_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                            table_info['date_columns'].append(col_name)
                        elif col_type in ['STRING', 'TEXT']:
                            table_info['string_columns'].append(col_name)
                    
                    self.schema_cache[cache_key] = table_info
                    return table_info
        
        except Exception as e:
            logger.warning(f"MCP schema discovery failed for {table}: {e}")
        
        # Method 2: Information Schema fallback
        return await self._get_schema_via_information_schema(project, dataset, table)
    
    async def _get_schema_via_information_schema(self, project: str, dataset: str, table: str) -> Dict:
        """Fallback: Get schema using INFORMATION_SCHEMA"""
        try:
            schema_query = f"""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table}'
            ORDER BY ordinal_position
            LIMIT 100
            """
            
            result = await self.mcp_client.execute_sql(schema_query)
            
            if not result.get("isError") and result.get("content"):
                table_info = {
                    'columns': [],
                    'column_names': [],
                    'numeric_columns': [],
                    'date_columns': [],
                    'string_columns': [],
                    'discovery_method': 'information_schema'
                }
                
                for item in result["content"]:
                    if isinstance(item, dict) and "text" in item:
                        row_data = json.loads(item["text"])
                        col_name = row_data.get("column_name", "")
                        data_type = row_data.get("data_type", "").upper()
                        
                        if col_name:
                            table_info['column_names'].append(col_name)
                            table_info['columns'].append({
                                'name': col_name,
                                'type': data_type
                            })
                            
                            # Categorize
                            if data_type in ['INTEGER', 'INT64', 'FLOAT', 'FLOAT64', 'NUMERIC']:
                                table_info['numeric_columns'].append(col_name)
                            elif data_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                                table_info['date_columns'].append(col_name)
                            elif data_type in ['STRING', 'TEXT']:
                                table_info['string_columns'].append(col_name)
                
                return table_info
        
        except Exception as e:
            logger.error(f"Information schema fallback failed for {table}: {e}")
        
        # Return minimal info
        return {
            'columns': [],
            'column_names': [],
            'numeric_columns': [],
            'date_columns': [],
            'string_columns': [],
            'discovery_method': 'failed'
        }
    
    def _analyze_question_semantics(self, question: str) -> Dict[str, Any]:
        """Analyze question semantics like Claude Desktop does"""
        question_lower = question.lower()
        
        # Platform detection
        platform_indicators = {
            'shipstation': ['shipstation', 'shipping', 'shipments', 'orders shipped', 'fulfillment'],
            'google_ads': ['google ads', 'gads', 'adwords', 'google advertising'],
            'microsoft_ads': ['microsoft ads', 'bing ads', 'msads', 'microsoft advertising'],
            'facebook_ads': ['facebook ads', 'meta ads', 'instagram ads', 'facebook advertising'],
            'analytics': ['analytics', 'ga4', 'google analytics', 'traffic', 'sessions'],
            'zoho_crm': ['zoho', 'crm', 'sales orders', 'purchase orders', 'quotes'],
            'amazon': ['amazon', 'amazon ads', 'amazon seller', 'sponsored products']
        }
        
        # Data type detection
        data_types = {
            'orders': ['order', 'orders', 'purchase', 'sales', 'transactions'],
            'campaigns': ['campaign', 'campaigns', 'advertising', 'ads'],
            'performance': ['performance', 'metrics', 'analytics', 'roas', 'ctr'],
            'inventory': ['inventory', 'items', 'products', 'stock'],
            'shipping': ['shipping', 'shipment', 'fulfillment', 'delivery'],
            'financial': ['revenue', 'cost', 'spend', 'profit', 'roas']
        }
        
        # Temporal indicators
        temporal_indicators = ['daily', 'weekly', 'monthly', 'last week', 'yesterday', 'recent']
        
        # Multi-platform indicators
        multi_platform_indicators = ['compare', 'vs', 'versus', 'comparison', 'both', 'all platforms']
        
        # Analyze question
        detected_platforms = []
        detected_data_types = []
        has_temporal = False
        is_multi_platform = False
        
        for platform, keywords in platform_indicators.items():
            if any(keyword in question_lower for keyword in keywords):
                detected_platforms.append(platform)
        
        for data_type, keywords in data_types.items():
            if any(keyword in question_lower for keyword in keywords):
                detected_data_types.append(data_type)
        
        has_temporal = any(indicator in question_lower for indicator in temporal_indicators)
        is_multi_platform = any(indicator in question_lower for indicator in multi_platform_indicators)
        
        return {
            'platforms': detected_platforms,
            'data_types': detected_data_types,
            'has_temporal': has_temporal,
            'is_multi_platform': is_multi_platform or len(detected_platforms) > 1,
            'question_words': question_lower.split()
        }
    
    async def _calculate_mcp_table_score(self, question: str, question_analysis: Dict, 
                                       table_name: str, table_info: Dict) -> float:
        """Calculate table relevance score using MCP-powered analysis"""
        score = 0.0
        table_name_lower = table_name.lower()
        
        # Platform matching (highest weight)
        for platform in question_analysis['platforms']:
            platform_score = self._calculate_platform_match_score(platform, table_name_lower)
            score += platform_score
            
            if platform_score > 50:  # Strong platform match
                logger.info(f"Strong platform match for {table_name}: {platform} (+{platform_score})")
        
        # Data type matching
        for data_type in question_analysis['data_types']:
            data_type_score = self._calculate_data_type_match_score(data_type, table_name_lower, table_info)
            score += data_type_score
        
        # Column relevance
        column_score = self._calculate_column_relevance_score(question_analysis['question_words'], table_info)
        score += column_score
        
        # Temporal data preference
        if question_analysis['has_temporal'] and 'daily' in table_name_lower:
            score += 15
        
        # Table freshness (prefer tables with recent data)
        if table_info.get('row_count', 0) > 0:
            score += 5
        
        return score
    
    def _calculate_platform_match_score(self, platform: str, table_name: str) -> float:
        """Calculate platform-specific matching score"""
        platform_patterns = {
            'shipstation': {
                'exact': ['shipstation'],
                'partial': ['ship', 'order', 'fulfillment'],
                'negative': ['gads', 'msads', 'facebook', 'zoho', 'amazon']
            },
            'google_ads': {
                'exact': ['gads'],
                'partial': ['google', 'ads'],
                'negative': ['msads', 'facebook', 'shipstation', 'amazon']
            },
            'microsoft_ads': {
                'exact': ['msads'],
                'partial': ['microsoft', 'bing'],
                'negative': ['gads', 'facebook', 'shipstation', 'amazon']
            },
            'zoho_crm': {
                'exact': ['zohocrm', 'zoho'],
                'partial': ['crm', 'sales', 'purchase'],
                'negative': ['gads', 'msads', 'facebook', 'shipstation']
            },
            'amazon': {
                'exact': ['amazon'],
                'partial': ['seller', 'sponsored'],
                'negative': ['gads', 'msads', 'facebook', 'shipstation']
            }
        }
        
        patterns = platform_patterns.get(platform, {})
        
        # Exact matches (very high score)
        for exact_pattern in patterns.get('exact', []):
            if exact_pattern in table_name:
                return 100.0
        
        # Negative matches (heavy penalty)
        for negative_pattern in patterns.get('negative', []):
            if negative_pattern in table_name:
                return -50.0
        
        # Partial matches
        score = 0.0
        for partial_pattern in patterns.get('partial', []):
            if partial_pattern in table_name:
                score += 20.0
        
        return score
    
    def _calculate_data_type_match_score(self, data_type: str, table_name: str, table_info: Dict) -> float:
        """Calculate data type matching score"""
        data_type_patterns = {
            'orders': ['order', 'sales', 'purchase', 'transaction'],
            'campaigns': ['campaign', 'ads', 'advertising'],
            'performance': ['performance', 'analytics', 'metrics'],
            'shipping': ['shipment', 'shipping', 'fulfillment'],
            'inventory': ['inventory', 'items', 'product']
        }
        
        patterns = data_type_patterns.get(data_type, [])
        score = 0.0
        
        # Check table name
        for pattern in patterns:
            if pattern in table_name:
                score += 10.0
        
        # Check column names
        column_names = table_info.get('column_names', [])
        for pattern in patterns:
            for col_name in column_names:
                if pattern in col_name.lower():
                    score += 5.0
        
        return score
    
    def _calculate_column_relevance_score(self, question_words: List[str], table_info: Dict) -> float:
        """Calculate column relevance score"""
        column_names = [col.lower() for col in table_info.get('column_names', [])]
        score = 0.0
        
        for word in question_words:
            if len(word) > 3:  # Skip short words
                for col_name in column_names:
                    if word in col_name or col_name in word:
                        score += 3.0
        
        return score
    
    async def _get_projects(self) -> List[str]:
        """Get available projects"""
        return [os.getenv("GOOGLE_CLOUD_PROJECT", "data-tables-for-zoho")]
    
    def _extract_names_from_mcp_result(self, mcp_result: Dict) -> List[str]:
        """Extract clean names from MCP result"""
        names = []
        
        if mcp_result and 'content' in mcp_result:
            for item in mcp_result['content']:
                if isinstance(item, str):
                    name = self._clean_mcp_name(item)
                    if name:
                        names.append(name)
                elif isinstance(item, dict) and 'text' in item:
                    name = self._clean_mcp_name(item['text'])
                    if name:
                        names.append(name)
        
        return names
    
    def _clean_mcp_name(self, raw_name: str) -> str:
        """Clean name from MCP response"""
        if not raw_name:
            return ""
        
        # Remove quotes and whitespace
        cleaned = raw_name.strip().strip('"').strip("'")
        return cleaned

# Main integration function
async def intelligent_bigquery_mcp(question: str) -> Tuple[str, Tuple[str, str, str]]:
    """
    Main function that uses MCP capabilities like Claude Desktop
    Returns: (sql_query, (project, dataset, table))
    """
    
    # Initialize MCP-powered components
    table_selector = MCPPoweredTableSelector()
    
    # Single table query
    scored_tables = await table_selector.intelligent_table_discovery(question)
    
    if not scored_tables:
        raise Exception("No suitable tables found using MCP discovery")
    
    # Select best table
    project, dataset, table, score = scored_tables[0]
    logger.info(f"Selected table: {project}.{dataset}.{table} (score: {score})")
    
    # Generate SQL for selected table
    table_info = await table_selector._get_table_schema_info(project, dataset, table)
    sql_query = await _generate_smart_sql(question, project, dataset, table, table_info)
    
    return sql_query, (project, dataset, table)

async def _generate_smart_sql(question: str, project: str, dataset: str, 
                            table: str, table_info: Dict) -> str:
    """Generate smart SQL using table schema information"""
    
    table_full_name = f"`{project}.{dataset}.{table}`"
    column_names = table_info.get('column_names', [])
    date_columns = table_info.get('date_columns', [])
    numeric_columns = table_info.get('numeric_columns', [])
    
    # Find date column
    date_col = None
    for col in date_columns:
        if any(word in col.lower() for word in ['date', 'time', 'created', 'modified']):
            date_col = col
            break
    
    if not date_col and date_columns:
        date_col = date_columns[0]
    
    # Basic query structure
    if 'shipstation' in table.lower():
        # ShipStation specific query
        select_columns = []
        if 'order_date' in column_names:
            select_columns.append('DATE(order_date) as order_date')
        if 'store_id' in column_names:
            select_columns.append('store_id')
        if 'order_total' in column_names:
            select_columns.append('SUM(order_total) as total_orders_value')
        
        if not select_columns:
            select_columns = ['*']
        
        sql = f"""
        SELECT {', '.join(select_columns)}
        FROM {table_full_name}
        """
        
        if date_col:
            sql += f" WHERE DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)"
        
        if len(select_columns) > 1 and select_columns[0] != '*':
            sql += f" GROUP BY DATE({date_col}), store_id" if 'store_id' in column_names else f" GROUP BY DATE({date_col})"
            sql += f" ORDER BY DATE({date_col}) DESC"
        
        sql += " LIMIT 50"
        
    else:
        # Generic query
        if len(column_names) <= 8:
            select_clause = '*'
        else:
            # Select most relevant columns
            important_cols = [col for col in column_names[:8]]
            select_clause = ', '.join(important_cols)
        
        sql = f"""
        SELECT {select_clause}
        FROM {table_full_name}
        """
        
        if date_col:
            sql += f" WHERE DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)"
            sql += f" ORDER BY DATE({date_col}) DESC"
        
        sql += " LIMIT 50"
    
    return sql.strip()