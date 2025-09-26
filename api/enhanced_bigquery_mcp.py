# api/enhanced_bigquery_mcp.py - MCP-Powered Intelligent System
import os
import logging
import json
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
import asyncio

# Import the MCP client class directly to avoid circular imports
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
        
        # Platform detection - CRITICAL for fixing ShipStation routing
        platform_indicators = {
            'shipstation': ['shipstation', 'shipping', 'shipments', 'orders shipped', 'fulfillment', 'shipped orders'],
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
                logger.info(f"Detected platform: {platform} from question: {question[:50]}")
        
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
        
        # Platform matching (highest weight) - CRITICAL for fixing routing
        for platform in question_analysis['platforms']:
            platform_score = self._calculate_platform_match_score(platform, table_name_lower)
            score += platform_score
            
            if platform_score > 50:  # Strong platform match
                logger.info(f"Strong platform match for {table_name}: {platform} (+{platform_score})")
            elif platform_score < 0:  # Penalty
                logger.info(f"Platform penalty for {table_name}: {platform} ({platform_score})")
        
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
        """Calculate platform-specific matching score - CRITICAL for routing fix"""
        platform_patterns = {
            'shipstation': {
                'exact': ['shipstation'],  # Exact match = +100
                'partial': ['ship', 'order', 'fulfillment'],  # Partial = +20 each
                'negative': ['gads', 'msads', 'facebook', 'zoho', 'amazon']  # Wrong platform = -50
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
        
        # Negative matches (heavy penalty) - THIS FIXES SHIPSTATION->ZOHO ROUTING
        for negative_pattern in patterns.get('negative', []):
            if negative_pattern in table_name:
                logger.warning(f"PLATFORM MISMATCH PENALTY: {platform} query blocked from {table_name} table")
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

class MCPUnionQueryDetector:
    """Detects when queries need union operations across multiple platforms"""
    
    def __init__(self, table_selector: MCPPoweredTableSelector):
        self.table_selector = table_selector
    
    async def analyze_union_requirements(self, question: str) -> Dict[str, Any]:
        """Analyze if question requires union query and which tables"""
        
        # Get question analysis
        question_analysis = self.table_selector._analyze_question_semantics(question)
        
        # Union indicators
        union_keywords = [
            'compare', 'vs', 'versus', 'comparison', 'both', 'all',
            'combined', 'total', 'overall', 'across', 'together',
            'each platform', 'all platforms', 'by platform'
        ]
        
        needs_union = (
            question_analysis['is_multi_platform'] or
            any(keyword in question.lower() for keyword in union_keywords)
        )
        
        if not needs_union:
            return {'needs_union': False}
        
        # Find relevant tables for each detected platform
        platform_tables = {}
        all_tables = await self.table_selector.intelligent_table_discovery(question)
        
        # Group tables by platform
        for project, dataset, table, score in all_tables:
            if score > 50:  # Only high-scoring tables
                table_full_name = f"{project}.{dataset}.{table}"
                platform = self._detect_table_platform(table)
                
                if platform not in platform_tables:
                    platform_tables[platform] = []
                
                platform_tables[platform].append({
                    'table': table_full_name,
                    'project': project,
                    'dataset': dataset,
                    'table_name': table,
                    'score': score
                })
        
        return {
            'needs_union': len(platform_tables) > 1,
            'platform_tables': platform_tables,
            'union_type': self._determine_union_type(question),
            'detected_platforms': list(platform_tables.keys())
        }
    
    def _detect_table_platform(self, table_name: str) -> str:
        """Detect platform from table name"""
        table_lower = table_name.lower()
        
        if 'shipstation' in table_lower:
            return 'shipstation'
        elif 'gads' in table_lower:
            return 'google_ads'
        elif 'msads' in table_lower:
            return 'microsoft_ads'
        elif 'facebook' in table_lower:
            return 'facebook_ads'
        elif 'zoho' in table_lower:
            return 'zoho_crm'
        elif 'amazon' in table_lower:
            return 'amazon'
        elif 'ga4' in table_lower or 'analytics' in table_lower:
            return 'analytics'
        else:
            return 'unknown'
    
    def _determine_union_type(self, question: str) -> str:
        """Determine type of union operation needed"""
        question_lower = question.lower()
        
        comparison_words = ['compare', 'vs', 'versus', 'difference', 'better', 'best']
        aggregation_words = ['total', 'sum', 'combined', 'overall', 'all together']
        
        if any(word in question_lower for word in comparison_words):
            return 'comparison'
        elif any(word in question_lower for word in aggregation_words):
            return 'aggregation'
        else:
            return 'analysis'

# Main integration function
async def intelligent_bigquery_mcp(question: str) -> Tuple[str, Tuple[str, str, str]]:
    """
    Main function that uses MCP capabilities like Claude Desktop
    Returns: (sql_query, (project, dataset, table))
    """
    
    # Initialize MCP-powered components
    table_selector = MCPPoweredTableSelector()
    union_detector = MCPUnionQueryDetector(table_selector)
    
    # Check if union query is needed
    union_analysis = await union_detector.analyze_union_requirements(question)
    
    if union_analysis['needs_union']:
        logger.info(f"Union query required for platforms: {union_analysis['detected_platforms']}")
        sql_query = await _generate_union_sql(question, union_analysis)
        return sql_query, ("union_query", "multi_platform", "combined")
    
    # Single table query
    scored_tables = await table_selector.intelligent_table_discovery(question)
    
    if not scored_tables:
        raise Exception("No suitable tables found using MCP discovery")
    
    # Select best table
    project, dataset, table, score = scored_tables[0]
    logger.info(f"MCP Selected table: {project}.{dataset}.{table} (score: {score})")
    
    # Validate the selection - CRITICAL validation
    if 'shipstation' in question.lower() and 'shipstation' not in table.lower():
        logger.error(f"MCP VALIDATION FAILED: ShipStation query incorrectly routed to {table}")
        # Look for correct ShipStation table
        for proj, ds, tbl, sc in scored_tables:
            if 'shipstation' in tbl.lower():
                logger.info(f"CORRECTED: Using {proj}.{ds}.{tbl} instead")
                project, dataset, table, score = proj, ds, tbl, sc
                break
    
    # Generate SQL for selected table
    table_info = await table_selector._get_table_schema_info(project, dataset, table)
    sql_query = await _generate_smart_sql(question, project, dataset, table, table_info)
    
    return sql_query, (project, dataset, table)

async def _generate_union_sql(question: str, union_analysis: Dict) -> str:
    """Generate union SQL query"""
    
    platform_tables = union_analysis['platform_tables']
    union_type = union_analysis['union_type']
    
    union_parts = []
    
    for platform, tables in platform_tables.items():
        if tables:  # Use highest scoring table for each platform
            best_table = max(tables, key=lambda x: x['score'])
            table_full_name = best_table['table']
            
            # Generate standardized SELECT for each platform
            if platform == 'shipstation':
                select_part = f"""
                SELECT 
                    '{platform}' as platform,
                    DATE(order_date) as date,
                    store_id as identifier,
                    COUNT(*) as orders,
                    SUM(order_total) as total_value
                FROM `{table_full_name}`
                WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
                GROUP BY platform, date, identifier
                """
            elif platform in ['google_ads', 'microsoft_ads']:
                cost_col = 'Cost' if platform == 'google_ads' else 'Spend'
                select_part = f"""
                SELECT 
                    '{platform}' as platform,
                    DATE(Date) as date,
                    Campaign_name as identifier,
                    SUM(CAST(Clicks AS FLOAT64)) as clicks,
                    SUM(CAST({cost_col} AS FLOAT64)) as total_value
                FROM `{table_full_name}`
                WHERE DATE(Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
                GROUP BY platform, date, identifier
                """
            else:
                # Generic format
                select_part = f"""
                SELECT 
                    '{platform}' as platform,
                    DATE(Date) as date,
                    'N/A' as identifier,
                    COUNT(*) as metric_count,
                    0 as total_value
                FROM `{table_full_name}`
                WHERE DATE(Date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
                GROUP BY platform, date
                """
            
            union_parts.append(select_part)
    
    # Combine with UNION ALL
    final_sql = ' UNION ALL '.join(union_parts)
    final_sql += " ORDER BY date DESC, platform, total_value DESC LIMIT 100"
    
    return final_sql

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
        # ShipStation specific query - FIXED routing should get here
        logger.info(f"Generating ShipStation-specific query for table: {table}")
        
        select_columns = []
        group_by_columns = []
        
        # Look for ShipStation-specific columns
        for col in column_names:
            col_lower = col.lower()
            if 'order_date' in col_lower or 'create' in col_lower:
                select_columns.append(f'DATE({col}) as order_date')
                group_by_columns.append(f'DATE({col})')
                date_col = col
            elif 'store' in col_lower:
                select_columns.append(col)
                group_by_columns.append(col)
            elif 'order_total' in col_lower or 'total' in col_lower:
                select_columns.append(f'SUM({col}) as total_orders_value')
            elif 'order_number' in col_lower or 'order_id' in col_lower:
                select_columns.append(f'COUNT(DISTINCT {col}) as order_count')
        
        if not select_columns:
            select_columns = ['*']
            group_by_columns = []
        
        sql = f"SELECT {', '.join(select_columns)} FROM {table_full_name}"
        
        if date_col:
            sql += f" WHERE DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)"
        
        if group_by_columns:
            sql += f" GROUP BY {', '.join(group_by_columns)}"
            sql += f" ORDER BY {group_by_columns[0]} DESC"
        
        sql += " LIMIT 50"
        
    else:
        # Generic query for other platforms
        if len(column_names) <= 8:
            select_clause = '*'
        else:
            # Select most relevant columns
            important_cols = [col for col in column_names[:8]]
            select_clause = ', '.join(important_cols)
        
        sql = f"SELECT {select_clause} FROM {table_full_name}"
        
        if date_col:
            sql += f" WHERE DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)"
            sql += f" ORDER BY DATE({date_col}) DESC"
        
        sql += " LIMIT 50"
    
    logger.info(f"Generated SQL for {table}: {sql}")
    return sql.strip()