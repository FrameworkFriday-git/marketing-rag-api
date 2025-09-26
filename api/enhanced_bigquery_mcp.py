# api/enhanced_bigquery_mcp.py - COMPLETE Enhanced Dynamic Intelligent Query Generation v3.3.0
import asyncio
import json
import aiohttp
import os
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import re

logger = logging.getLogger(__name__)

class IntelligentBigQueryMCP:
    def __init__(self):
        self.server_url = os.getenv("BIGQUERY_MCP_URL", "https://bigquery.fridaysolutions.ai/mcp")
        self.session = None
        self.request_id = 0
        
        # Caches for metadata
        self._table_cache = {}
        self._schema_cache = {}
        self._relationship_cache = {}
        self._semantic_mapping = {}
        self._cache_expiry = 30 * 60  # 30 minutes
        self._last_discovery = None
        
        # Initialize semantic patterns and account aliases
        self._initialize_semantic_patterns()
        self._initialize_account_aliases()

    def _initialize_semantic_patterns(self):
        """Initialize semantic patterns for intelligent table discovery."""
        self._semantic_mapping = {
            'google_ads': {
                'keywords': ['google', 'adwords', 'gads', 'search_ads'],
                'platforms': ['Google Ads', 'AdWords', 'Google AdWords'],
                'metrics': ['clicks', 'impressions', 'cost', 'conversions', 'ctr', 'cpc'],
                'table_patterns': ['gads', 'google_ads', 'adwords']
            },
            'microsoft_ads': {
                'keywords': ['microsoft', 'bing', 'msads', 'bing_ads'],
                'platforms': ['Microsoft Ads', 'Bing Ads'],
                'metrics': ['clicks', 'impressions', 'spend', 'conversions', 'ctr', 'cpc'],
                'table_patterns': ['msads', 'microsoft_ads', 'bing']
            },
            'shipstation': { 
                'keywords': ['shipstation', 'shipping', 'shipment', 'orders', 'fulfillment'],
                'platforms': ['ShipStation', 'Shipping'],
                'metrics': ['orders', 'shipments', 'items'],
                'table_patterns': ['shipstation_', 'shipstation', 'shipping_'] 
            },
            'facebook_ads': {
                'keywords': ['facebook', 'meta', 'fb', 'instagram'],
                'platforms': ['Facebook', 'Meta', 'Instagram'],
                'metrics': ['clicks', 'impressions', 'spend', 'reach', 'frequency'],
                'table_patterns': ['facebook_ads', 'facebook', 'meta', 'fb_ads', 'fb']
            },
            'analytics': {
                'keywords': ['ga4', 'analytics', 'google_analytics', 'sessions'],
                'platforms': ['Google Analytics', 'GA4'],
                'metrics': ['sessions', 'users', 'pageviews', 'bounce_rate', 'conversion_rate'],
                'table_patterns': ['ga4', 'analytics', 'google_analytics']
            },
            'amazon_ads': {
                'keywords': ['amazon', 'amazon_ads', 'sponsored'],
                'platforms': ['Amazon Ads', 'Amazon Advertising'],
                'metrics': ['clicks', 'impressions', 'cost', 'sales', 'acos', 'roas'],
                'table_patterns': ['amazon', 'amazon_ads']
            }
        }

    def _initialize_account_aliases(self):
        """Initialize account name variations for intelligent matching."""
        self.account_aliases = {
            'budgetmailbox': [
                'budgetmailbox', 'budget mailbox', 'budget mail box', 
                'budgetmailboxes', 'budget mailboxes', 'budget mail boxes',
                'bm', 'bmb', 'budget boxes', 'budgetboxes',
                'budgetmailboxes.com'
            ],
            'mailboxworks': [
                'mailboxworks', 'mailbox works', 'mailboxwork', 'mailbox work',
                'mw', 'mailboxes works', 'mailboxes work'
            ]
        }

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
                    raise Exception(f"HTTP {response.status}: {error_text}")
                
                response_data = await response.json()
                
                if "error" in response_data:
                    error_msg = response_data['error'].get('message', 'Unknown error')
                    logger.error(f"MCP Error for {tool_name}: {error_msg}")
                    raise Exception(f"MCP Error: {error_msg}")
                
                return response_data.get("result", {})
                
        except Exception as e:
            logger.error(f"MCP call failed: {tool_name} - {e}")
            raise
    
    def _parse_mcp_response(self, mcp_result):
        """Parse MCP response format"""
        items = []
        
        if not mcp_result or 'content' not in mcp_result:
            return items
            
        for item in mcp_result['content']:
            if isinstance(item, dict) and 'text' in item:
                text = item['text']
                if text.startswith('"') and text.endswith('"'):
                    text = text[1:-1]
                items.append(text)
            elif isinstance(item, str):
                items.append(item)
                
        return items

    def _extract_schema_columns_enhanced(self, schema_data) -> Dict[str, List[str]]:
        """Enhanced column categorization with better semantic understanding"""
        columns = {
            'all': [],
            'date': [],
            'account': [],
            'metrics': [],
            'dimensions': [],
            'campaign': [],
            'conversion': []
        }
        
        if not schema_data:
            return columns
        
        # Handle schema in different formats
        if isinstance(schema_data, list) and schema_data:
            try:
                schema_text = schema_data[0].get('text', '{}')
                schema_json = json.loads(schema_text)
                schema_fields = schema_json.get('Schema', [])
            except (json.JSONDecodeError, KeyError, IndexError):
                logger.warning("Failed to parse schema data")
                return columns
        elif isinstance(schema_data, dict):
            schema_fields = schema_data.get('Schema', [])
        else:
            return columns
        
        # Enhanced categorization with platform-specific patterns
        for field in schema_fields:
            col_name = field.get('Name', '')
            col_type = field.get('Type', '').upper()
            
            if not col_name:
                continue
                
            columns['all'].append(col_name)
            col_lower = col_name.lower()
            
            # Date columns - more comprehensive
            if col_type in ['DATE', 'DATETIME', 'TIMESTAMP'] or 'date' in col_lower or 'time' in col_lower:
                columns['date'].append(col_name)
            
            # Account/Store identifier columns - enhanced patterns
            elif any(identifier in col_lower for identifier in [
                'account_name', 'store_name', 'online_store', 'customer_descriptive_name',
                'account', 'customer', 'advertiser'
            ]):
                columns['account'].append(col_name)
            
            # Campaign-related columns
            elif any(campaign_term in col_lower for campaign_term in ['campaign', 'ad_group', 'adgroup']):
                columns['campaign'].append(col_name)
                if col_type in ['STRING', 'TEXT']:
                    columns['dimensions'].append(col_name)
            
            # Conversion columns
            elif any(conv_term in col_lower for conv_term in ['conversion', 'purchase', 'sale', 'transaction']):
                columns['conversion'].append(col_name)
                if col_type in ['INTEGER', 'FLOAT', 'FLOAT64', 'INT64', 'NUMERIC']:
                    columns['metrics'].append(col_name)
            
            # Metric columns - enhanced with platform-specific patterns
            elif col_type in ['INTEGER', 'FLOAT', 'FLOAT64', 'INT64', 'NUMERIC']:
                if self._is_metric_column(col_lower):
                    columns['metrics'].append(col_name)
            
            # Dimension columns - categorical data
            elif col_type in ['STRING', 'TEXT']:
                if any(dim in col_lower for dim in [
                    'device', 'network', 'match_type', 'ad_type', 'status', 'state'
                ]):
                    columns['dimensions'].append(col_name)
        
        logger.info(f"Enhanced column extraction - Date: {columns['date']}, Account: {columns['account']}, "
                   f"Metrics: {len(columns['metrics'])}, Campaign: {columns['campaign']}")
        
        return columns

    def _is_metric_column(self, col_name_lower: str) -> bool:
        """Enhanced metric detection with platform-specific patterns"""
        metric_patterns = [
            # Universal patterns
            'click', 'impression', 'cost', 'spend', 'conversion', 'revenue',
            'ctr', 'cpc', 'cpm', 'roas', 'acos',
            # Microsoft Ads specific
            'all_conversion', 'conversion_value',
            # Google Ads specific  
            'interaction', 'view_through_conversion',
            # Facebook/Meta specific
            'reach', 'frequency', 'engagement',
            # Amazon specific
            'sales', 'units_sold', 'acos', 'qualified_borrow',
            # Analytics specific
            'session', 'user', 'pageview', 'bounce', 'transaction'
        ]
        
        return any(pattern in col_name_lower for pattern in metric_patterns)

    def _calculate_platform_match_score(self, query_lower: str, table_info: dict) -> int:
        """Give heavy weight to exact platform matches"""
        table_name_lower = table_info['table'].lower()
        tags = table_info.get('semantic_tags', [])
        
        platform_scores = {}
        
        # Check query for platform mentions
        for platform, config in self._semantic_mapping.items():
            query_mentions_platform = any(keyword in query_lower for keyword in config['keywords'])
            table_matches_platform = (
                platform in tags or 
                any(pattern in table_name_lower for pattern in config['table_patterns'])
            )
            
            if query_mentions_platform and table_matches_platform:
                platform_scores[platform] = 100  # Very high score for exact matches
            elif query_mentions_platform and not table_matches_platform:
                platform_scores[platform] = -50  # Penalty for wrong platform
            elif table_matches_platform:
                platform_scores[platform] = 20  # Moderate score if table matches but query unclear
        
        return max(platform_scores.values()) if platform_scores else 0

    def _analyze_query_intent_enhanced(self, query_text: str) -> Dict[str, Any]:
        """Enhanced query intent analysis"""
        query_lower = query_text.lower()
        
        # Extract all meaningful terms
        terms = [word for word in re.findall(r'\b\w+\b', query_lower) 
                 if len(word) > 2 and word not in ['the', 'and', 'for', 'with', 'from', 'show', 'get', 'give', 'some', 'data']]
        
        # Platform detection with confidence scoring
        platforms_detected = {}
        for platform, config in self._semantic_mapping.items():
            confidence = 0
            for keyword in config['keywords']:
                if keyword in query_lower:
                    confidence += 1
            if confidence > 0:
                platforms_detected[platform] = confidence
        
        # Metric intent detection
        metrics_requested = []
        for platform, config in self._semantic_mapping.items():
            for metric in config['metrics']:
                if metric in query_lower:
                    metrics_requested.append(metric)
        
        # Time period detection
        time_periods = []
        time_patterns = {
            'last_30_days': r'(?:last|past)\s*30\s*days?|last\s*month',
            'last_7_days': r'(?:last|past)\s*7\s*days?|last\s*week|past\s*week',
            'this_month': r'this\s*month|current\s*month',
            'this_year': r'this\s*year|current\s*year'
        }
        
        for period, pattern in time_patterns.items():
            if re.search(pattern, query_lower):
                time_periods.append(period)
        
        # Aggregation detection
        aggregation_keywords = ['total', 'sum', 'average', 'count', 'group', 'by']
        needs_aggregation = any(keyword in query_lower for keyword in aggregation_keywords)
        
        return {
            'terms': terms,
            'platforms_detected': platforms_detected,
            'primary_platform': max(platforms_detected.items(), key=lambda x: x[1])[0] if platforms_detected else None,
            'metrics_requested': metrics_requested,
            'time_periods': time_periods,
            'needs_aggregation': needs_aggregation,
            'query_complexity': 'high' if len(platforms_detected) > 1 or len(metrics_requested) > 3 else 'medium' if platforms_detected or metrics_requested else 'low'
        }

    def _calculate_enhanced_relevance_score(self, table_info: dict, query_intent: Dict[str, Any]) -> int:
        """Enhanced relevance scoring with better platform matching"""
        score = 0
        
        # 1. CRITICAL: Platform matching (highest weight)
        platform_score = self._calculate_platform_match_score(' '.join(query_intent['terms']), table_info)
        score += platform_score
        
        # 2. Schema-based relevance
        schema = table_info.get('schema', {})
        if schema:
            columns = self._extract_schema_columns_enhanced(schema)
            
            # Metric availability scoring
            if query_intent['metrics_requested']:
                metric_matches = 0
                for requested_metric in query_intent['metrics_requested']:
                    for available_col in columns['metrics']:
                        if requested_metric in available_col.lower():
                            metric_matches += 1
                            break
                score += metric_matches * 15
            
            # Time capability scoring  
            if query_intent['time_periods'] and columns['date']:
                score += 25
            
            # Account filtering capability
            if columns['account']:
                score += 10
                
            # Campaign analysis capability
            if columns['campaign'] and any('campaign' in term for term in query_intent['terms']):
                score += 15
        
        # 3. Table name relevance
        table_name_lower = table_info['table'].lower()
        for term in query_intent['terms']:
            if term in table_name_lower:
                score += 8
        
        # 4. Semantic tags alignment
        tags = table_info.get('semantic_tags', [])
        primary_platform = query_intent.get('primary_platform')
        if primary_platform and primary_platform in tags:
            score += 30  # Strong boost for matching platform
            
        # 5. Data type alignment
        if query_intent['needs_aggregation'] and schema:
            columns = self._extract_schema_columns_enhanced(schema)
            if len(columns['metrics']) >= 3:
                score += 20
        
        return score

    async def intelligent_table_discovery(self, query_text: str) -> Dict[str, Any]:
        """Enhanced table discovery with better relevance scoring"""
        
        await self.discover_and_cache_metadata()
        
        relevant_tables = []
        table_scores = {}
        
        # Enhanced query intent analysis
        query_intent = self._analyze_query_intent_enhanced(query_text)
        logger.info(f"Query intent: {query_intent}")
        
        for table_name, table_info in self._table_cache.items():
            score = self._calculate_enhanced_relevance_score(table_info, query_intent)
            
            if score > 0:
                table_scores[table_name] = score
                logger.info(f"Table {table_name}: Score {score} (Platform match: {self._calculate_platform_match_score(' '.join(query_intent['terms']), table_info)})")
        
        # Sort by relevance score
        sorted_tables = sorted(table_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Build response with top tables
        for table_name, score in sorted_tables[:8]:  # Increased to show more options
            table_info = self._table_cache[table_name]
            relevant_tables.append({
                'table_name': table_name,
                'dataset': table_info['dataset'],
                'table': table_info['table'],
                'relevance_score': score,
                'semantic_tags': table_info.get('semantic_tags', []),
                'schema': table_info.get('schema', {}),
                'suggested_joins': self._suggest_joins(table_name)
            })
        
        return {
            'query': query_text,
            'query_intent': query_intent,
            'relevant_tables': relevant_tables,
            'total_tables_analyzed': len(self._table_cache),
            'discovery_timestamp': self._last_discovery.isoformat() if self._last_discovery else None
        }

    def _analyze_table_semantics_enhanced(self, table_name: str, schema: Dict) -> List[str]:
        """Enhanced semantic analysis with FIXED single platform per table"""
        tags = []
        table_lower = table_name.lower()
        
        # Platform detection with confidence scoring
        platform_confidence = {}
        for platform, config in self._semantic_mapping.items():
            confidence = 0
            for pattern in config['table_patterns']:
                if pattern in table_lower:
                    confidence += 2  # Higher weight for table name matches
            
            # Check schema for platform-specific columns
            if schema:
                columns = self._extract_schema_columns_enhanced(schema)
                platform_metrics = config['metrics']
                for col in columns['metrics']:
                    col_lower = col.lower()
                    for metric in platform_metrics:
                        if metric in col_lower:
                            confidence += 1
                            break
            
            if confidence > 0:
                platform_confidence[platform] = confidence
        
        # FIXED: Add ONLY the single highest confidence platform
        if platform_confidence:
            best_platform = max(platform_confidence.items(), key=lambda x: x[1])
            platform_name = best_platform[0]
            platform_score = best_platform[1]
            
            # Only add platform if confidence is meaningful (score >= 2)
            if platform_score >= 2:
                tags.append(platform_name)
                logger.info(f"Table {table_name}: Tagged with single platform '{platform_name}' (confidence: {platform_score})")
        
        # Add functional tags based on schema analysis
        if schema:
            columns = self._extract_schema_columns_enhanced(schema)
            
            if columns['metrics']:
                tags.extend(['advertising', 'performance'])
            
            if columns['campaign']:
                tags.append('campaign_data')
                
            if columns['conversion']:
                tags.append('conversion_data')
        
        # Add structural tags
        if 'daily' in table_lower:
            tags.append('daily_data')
        if 'account' in table_lower:
            tags.append('account_level')
        if 'campaign' in table_lower:
            tags.append('campaign_level')
        if 'ad' in table_lower and 'ad_group' not in table_lower:
            tags.append('ad_level')
        
        return list(set(tags))  # Remove duplicates

    # === MULTI-TABLE ANALYSIS CAPABILITIES ===
    
    class TableRelationshipMapper:
        """Maps relationships between tables for complex multi-platform queries"""
        
        def __init__(self, mcp_instance):
            self.mcp = mcp_instance
            
        def detect_cross_platform_intent(self, query_text: str) -> Dict[str, Any]:
            """Detect if query requires multiple platforms"""
            query_lower = query_text.lower()
            
            platforms_mentioned = []
            for platform, config in self.mcp._semantic_mapping.items():
                for keyword in config['keywords']:
                    if keyword in query_lower:
                        platforms_mentioned.append(platform)
                        break
            
            # Look for comparison keywords
            comparison_keywords = ['compare', 'vs', 'versus', 'correlation', 'correlate', 'against', 'with']
            has_comparison = any(word in query_lower for word in comparison_keywords)
            
            return {
                'is_multi_platform': len(platforms_mentioned) > 1,
                'platforms_mentioned': list(set(platforms_mentioned)),
                'has_comparison_intent': has_comparison,
                'query_type': 'cross_platform_analysis' if len(platforms_mentioned) > 1 else 'single_platform'
            }
        
        def find_joinable_tables(self, primary_tables: List[Dict]) -> List[Dict[str, Any]]:
            """Find tables that can be joined for multi-platform analysis"""
            joinable_combinations = []
            
            for i, table1 in enumerate(primary_tables):
                for j, table2 in enumerate(primary_tables[i+1:], i+1):
                    # Check for common join columns
                    schema1 = self.mcp._extract_schema_columns_enhanced(table1.get('schema', {}))
                    schema2 = self.mcp._extract_schema_columns_enhanced(table2.get('schema', {}))
                    
                    # Find common columns
                    common_cols = set(schema1['all']) & set(schema2['all'])
                    
                    # Prioritize account and date columns for joins
                    join_candidates = []
                    for col in common_cols:
                        if any(identifier in col.lower() for identifier in ['account', 'date', 'customer']):
                            join_candidates.append(col)
                    
                    if join_candidates:
                        joinable_combinations.append({
                            'table1': table1,
                            'table2': table2,
                            'join_columns': join_candidates,
                            'join_type': 'INNER JOIN' if 'account' in str(join_candidates).lower() else 'LEFT JOIN'
                        })
            
            return joinable_combinations
    
    class ComplexQueryProcessor:
        """Processes complex multi-table queries"""
        
        def __init__(self, mcp_instance):
            self.mcp = mcp_instance
            self.relationship_mapper = self.mcp.TableRelationshipMapper(mcp_instance)
        
        async def process_multi_platform_query(self, query_text: str, relevant_tables: List[Dict]) -> Dict[str, Any]:
            """Process queries that span multiple platforms"""
            
            cross_platform_intent = self.relationship_mapper.detect_cross_platform_intent(query_text)
            
            if not cross_platform_intent['is_multi_platform']:
                return await self._process_single_platform_enhanced(query_text, relevant_tables)
            
            # Group tables by platform
            platform_tables = {}
            for table in relevant_tables[:4]:  # Limit to top 4 tables
                tags = table.get('semantic_tags', [])
                platform = None
                for tag in tags:
                    if tag in self.mcp._semantic_mapping:
                        platform = tag
                        break
                
                if platform:
                    if platform not in platform_tables:
                        platform_tables[platform] = []
                    platform_tables[platform].append(table)
            
            # Check if we can join tables
            if cross_platform_intent['has_comparison_intent']:
                return await self._build_comparison_query(query_text, platform_tables, cross_platform_intent)
            else:
                return await self._build_union_query(query_text, platform_tables, cross_platform_intent)
        
        async def _build_comparison_query(self, query_text: str, platform_tables: Dict, intent: Dict) -> Dict[str, Any]:
            """Build queries that compare metrics across platforms"""
            
            sql_parts = []
            cte_definitions = []
            
            for platform, tables in platform_tables.items():
                if not tables:
                    continue
                    
                primary_table = tables[0]
                schema = self.mcp._extract_schema_columns_enhanced(primary_table.get('schema', {}))
                
                # Build platform-specific CTE
                select_parts = []
                if schema['date']:
                    select_parts.append(f"DATE({schema['date'][0]}) as analysis_date")
                if schema['account']:
                    select_parts.append(f"{schema['account'][0]} as account_name")
                
                # Add key metrics with platform prefix
                metrics_added = 0
                for col in schema['metrics'][:3]:  # Limit metrics
                    select_parts.append(f"SUM(CAST({col} AS FLOAT64)) as {platform}_{col.lower()}")
                    metrics_added += 1
                
                if select_parts:
                    table_name = f"`{primary_table['dataset']}.{primary_table['table']}`"
                    cte_sql = f"""
{platform}_data AS (
  SELECT {', '.join(select_parts)}
  FROM {table_name}
  WHERE DATE({schema['date'][0]}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY analysis_date, account_name
)"""
                    cte_definitions.append(cte_sql)
            
            if len(cte_definitions) >= 2:
                # Build comparison query
                final_sql = f"""
WITH {','.join(cte_definitions)}
SELECT 
  COALESCE(t1.analysis_date, t2.analysis_date) as date,
  COALESCE(t1.account_name, t2.account_name) as account,
  t1.*,
  t2.*
FROM {list(platform_tables.keys())[0]}_data t1
FULL OUTER JOIN {list(platform_tables.keys())[1]}_data t2
  ON t1.analysis_date = t2.analysis_date 
  AND t1.account_name = t2.account_name
ORDER BY date DESC
LIMIT 100
"""
                
                return {
                    'sql': final_sql,
                    'query_type': 'cross_platform_comparison',
                    'platforms_analyzed': list(platform_tables.keys()),
                    'explanation': f"Comparing metrics across {', '.join(platform_tables.keys())} platforms"
                }
        
        async def _build_union_query(self, query_text: str, platform_tables: Dict, intent: Dict) -> Dict[str, Any]:
            """Build queries that union data from multiple platforms"""
            
            union_parts = []
            
            for platform, tables in platform_tables.items():
                if not tables:
                    continue
                    
                primary_table = tables[0]
                schema = self.mcp._extract_schema_columns_enhanced(primary_table.get('schema', {}))
                
                select_parts = [f"'{platform}' as platform"]
                if schema['date']:
                    select_parts.append(f"DATE({schema['date'][0]}) as date")
                if schema['account']:
                    select_parts.append(f"{schema['account'][0]} as account_name")
                
                # Standardize metric names
                if schema['metrics']:
                    # Find cost/spend metric
                    cost_metric = next((col for col in schema['metrics'] 
                                      if any(term in col.lower() for term in ['cost', 'spend'])), None)
                    if cost_metric:
                        select_parts.append(f"SUM(CAST({cost_metric} AS FLOAT64)) as total_spend")
                    
                    # Find clicks metric
                    clicks_metric = next((col for col in schema['metrics'] 
                                        if 'click' in col.lower()), None)
                    if clicks_metric:
                        select_parts.append(f"SUM(CAST({clicks_metric} AS FLOAT64)) as total_clicks")
                
                table_name = f"`{primary_table['dataset']}.{primary_table['table']}`"
                union_sql = f"""
SELECT {', '.join(select_parts)}
FROM {table_name}
WHERE DATE({schema['date'][0]}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY platform, date, account_name"""
                
                union_parts.append(union_sql)
            
            if len(union_parts) >= 2:
                final_sql = f"""
{' UNION ALL '.join(union_parts)}
ORDER BY date DESC, platform
LIMIT 200
"""
                
                return {
                    'sql': final_sql,
                    'query_type': 'multi_platform_union',
                    'platforms_analyzed': list(platform_tables.keys()),
                    'explanation': f"Combined data from {', '.join(platform_tables.keys())} platforms"
                }
        
        async def _process_single_platform_enhanced(self, query_text: str, relevant_tables: List[Dict]) -> Dict[str, Any]:
            """Enhanced single platform processing"""
            primary_table = relevant_tables[0]
            
            # Use the existing intelligent SQL generation
            account_name = self.mcp._extract_account_name_from_question_enhanced(query_text)
            query_intent = self.mcp._analyze_query_intent_enhanced(query_text)
            
            sql = await self.mcp._generate_intelligent_sql(query_text, primary_table, account_name, query_intent)
            
            return {
                'sql': sql,
                'query_type': 'single_platform_enhanced',
                'primary_table': primary_table['table_name'],
                'explanation': f"Enhanced analysis of {query_intent.get('primary_platform', 'data')} performance"
            }

    def _build_intelligent_select_clause(self, question: str, columns: Dict[str, List[str]], query_intent: Dict[str, Any]) -> List[str]:
        """Build SELECT clause using enhanced column analysis and query intent"""
        select_parts = []
        question_lower = question.lower()
        
        # Always include date if available
        if columns['date']:
            select_parts.append(f"DATE({columns['date'][0]}) as date")
        
        # Always include account identifier if available
        if columns['account']:
            select_parts.append(columns['account'][0])
            
        # Include campaign info if relevant
        if columns['campaign'] and any('campaign' in term for term in query_intent.get('terms', [])):
            select_parts.extend(columns['campaign'][:2])  # Take first 2 campaign columns
        
        # Add metrics based on query intent and availability
        if query_intent.get('metrics_requested'):
            # Match requested metrics to available columns
            for requested_metric in query_intent['metrics_requested']:
                for col in columns['metrics']:
                    col_lower = col.lower()
                    if requested_metric in col_lower and col not in select_parts:
                        if query_intent.get('needs_aggregation'):
                            select_parts.append(f"SUM(CAST({col} AS FLOAT64)) as total_{requested_metric}")
                        else:
                            select_parts.append(col)
                        break
        else:
            # Add common metrics if none specified
            common_metrics = ['clicks', 'impressions', 'cost', 'spend', 'conversions']
            added_metrics = 0
            for col in columns['metrics']:
                if added_metrics >= 4:  # Limit to 4 metrics
                    break
                col_lower = col.lower()
                if any(common in col_lower for common in common_metrics):
                    if query_intent.get('needs_aggregation'):
                        metric_name = col_lower.replace('_', '')[:10]  # Shortened name
                        select_parts.append(f"SUM(CAST({col} AS FLOAT64)) as total_{metric_name}")
                    else:
                        select_parts.append(col)
                    added_metrics += 1
        
        # Add important dimensions
        if columns['dimensions'] and not query_intent.get('needs_aggregation'):
            select_parts.extend(columns['dimensions'][:2])  # Add first 2 dimensions
        
        return select_parts if select_parts else ['*']

    def _build_intelligent_where_clause(self, question: str, columns: Dict[str, List[str]], 
                                      query_intent: Dict[str, Any], account_name: str = None) -> List[str]:
        """Build WHERE clause using enhanced column analysis"""
        where_conditions = []
        
        # Time filter using actual date column
        if columns['date'] and query_intent.get('time_periods'):
            date_col = columns['date'][0]
            time_period = query_intent['time_periods'][0]  # Use first detected period
            
            if time_period == 'last_30_days':
                where_conditions.append(f"DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)")
            elif time_period == 'last_7_days':
                where_conditions.append(f"DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)")
            elif time_period == 'this_month':
                where_conditions.append(f"DATE({date_col}) >= DATE_TRUNC(CURRENT_DATE(), MONTH)")
            elif time_period == 'this_year':
                where_conditions.append(f"DATE({date_col}) >= DATE_TRUNC(CURRENT_DATE(), YEAR)")
        elif columns['date']:
            # Default to last 30 days if date available but no specific period mentioned
            date_col = columns['date'][0]
            where_conditions.append(f"DATE({date_col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)")
        
        # Account filter using actual account column
        if account_name and columns['account']:
            account_col = columns['account'][0]
            
            # Get account variations
            account_variations = []
            for canonical, aliases in self.account_aliases.items():
                if canonical in account_name.lower() or any(alias in account_name.lower() for alias in aliases):
                    account_variations.extend(aliases)
                    break
            
            if not account_variations:
                account_variations = [account_name]
            
            # Build account matching conditions
            account_conditions = []
            for variation in account_variations:
                account_conditions.append(f"LOWER({account_col}) LIKE '%{variation.lower()}%'")
            
            if account_conditions:
                where_conditions.append(f"({' OR '.join(account_conditions)})")
        
        # Platform-specific filters (e.g., active campaigns only)
        primary_platform = query_intent.get('primary_platform')
        if primary_platform and columns['dimensions']:
            # Look for status/state columns
            for col in columns['dimensions']:
                if 'status' in col.lower() or 'state' in col.lower():
                    where_conditions.append(f"{col} IN ('ENABLED', 'ACTIVE', 'enabled', 'active')")
                    break
        
        return where_conditions

    def _extract_account_name_from_question_enhanced(self, question: str) -> Optional[str]:
        """Enhanced account name extraction with better pattern matching"""
        question_lower = question.lower()
        
        # Direct account alias matching first
        for canonical, aliases in self.account_aliases.items():
            for alias in aliases:
                if alias in question_lower:
                    logger.info(f"Enhanced Account Extraction: Found '{alias}' -> '{canonical}'")
                    return canonical
        
        # Pattern-based extraction
        account_patterns = [
            r'(?:for|in|on|at)\s+([a-zA-Z\s]+?)(?:\s|$|account|store)',
            r'([a-zA-Z\s]*(?:mailbox|boxes?|budget)[a-zA-Z\s]*)',
            r'([a-zA-Z\s]*works?[a-zA-Z\s]*)',
            r'\b(bm|bmb|mw)\b'
        ]
        
        for pattern in account_patterns:
            match = re.search(pattern, question_lower)
            if match:
                extracted = match.group(1).strip()
                
                # Clean up
                extracted = re.sub(r'^(for|in|on|the|a|an)\s+', '', extracted)
                extracted = re.sub(r'\s+(for|in|on|the|a|an)$', '', extracted)
                
                if len(extracted) >= 2:
                    logger.info(f"Pattern-based extraction: '{extracted}'")
                    return extracted
        
        return None

    async def _generate_intelligent_sql(self, question: str, table_info: dict, 
                                      account_name: Optional[str], query_intent: Dict[str, Any]) -> str:
        """Generate SQL with full intelligence and proper table selection"""
        
        dataset = table_info['dataset']
        table = table_info['table']
        table_name = f"`{dataset}.{table}`"
        
        # Extract columns from schema
        schema = table_info.get('schema', {})
        columns = self._extract_schema_columns_enhanced(schema)
        
        logger.info(f"Intelligent SQL Generation - Table: {table_name}")
        logger.info(f"Available columns - Date: {columns['date']}, Account: {columns['account']}, "
                   f"Metrics: {len(columns['metrics'])}, Campaign: {columns['campaign']}")
        
        # Build SELECT clause intelligently
        select_parts = self._build_intelligent_select_clause(question, columns, query_intent)
        
        # Build WHERE clause intelligently  
        where_conditions = self._build_intelligent_where_clause(question, columns, query_intent, account_name)
        
        # Build GROUP BY if aggregating
        group_by_parts = []
        if query_intent.get('needs_aggregation') or any('SUM(' in part or 'AVG(' in part or 'COUNT(' in part for part in select_parts):
            for part in select_parts:
                if not any(func in part.upper() for func in ['SUM(', 'AVG(', 'COUNT(', 'MAX(', 'MIN(']):
                    if ' as ' in part.lower():
                        group_by_parts.append(part.split(' as ')[0])
                    else:
                        group_by_parts.append(part)
        
        # Build final query
        sql_parts = [f"SELECT {', '.join(select_parts)}"]
        sql_parts.append(f"FROM {table_name}")
        
        if where_conditions:
            sql_parts.append(f"WHERE {' AND '.join(where_conditions)}")
        
        if group_by_parts:
            sql_parts.append(f"GROUP BY {', '.join(group_by_parts)}")
        
        # Intelligent ORDER BY
        if columns['date']:
            sql_parts.append(f"ORDER BY DATE({columns['date'][0]}) DESC")
        elif query_intent.get('needs_aggregation') and 'total_' in select_parts[0]:
            sql_parts.append(f"ORDER BY {select_parts[0].split(' as ')[1]} DESC")
        
        sql_parts.append("LIMIT 50")
        
        sql_query = '\n'.join(sql_parts)
        logger.info(f"Generated Intelligent SQL: {sql_query}")
        
        return sql_query

    async def discover_and_cache_metadata(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Discover and cache table metadata for intelligent querying."""
        
        # Check if cache is still valid
        if (not force_refresh and self._last_discovery and 
            (datetime.now() - self._last_discovery).seconds < self._cache_expiry):
            return {
                "status": "using_cache",
                "tables": len(self._table_cache),
                "last_discovery": self._last_discovery.isoformat()
            }

        logger.info("Starting enhanced metadata discovery...")
        
        try:
            # Discover datasets using MCP
            datasets_result = await self.call_tool("list_dataset_ids", {})
            
            if datasets_result.get("isError"):
                raise Exception(f"Failed to list datasets: {datasets_result.get('error')}")
            
            # Parse datasets from MCP format
            datasets = self._parse_mcp_response(datasets_result)
            logger.info(f"Found {len(datasets)} datasets: {datasets}")
            
            # Cache tables and their metadata
            for dataset in datasets:
                try:
                    # Get tables in dataset using MCP
                    tables_result = await self.call_tool("list_table_ids", {"dataset": dataset})
                    
                    if tables_result.get("isError"):
                        logger.warning(f"Could not list tables in {dataset}: {tables_result.get('error')}")
                        continue
                    
                    # Parse tables from MCP format
                    tables = self._parse_mcp_response(tables_result)
                    logger.info(f"Dataset {dataset}: found {len(tables)} tables")
                    
                    for table in tables:
                        full_table_name = f"{dataset}.{table}"
                        
                        # Get table schema using MCP
                        try:
                            schema_result = await self.call_tool("get_table_info", {
                                "dataset": dataset, 
                                "table": table
                            })
                            
                            if not schema_result.get("isError"):
                                schema = schema_result.get("content", {})
                                
                                # Enhanced semantic analysis
                                semantic_tags = self._analyze_table_semantics_enhanced(table, schema)
                                
                                self._table_cache[full_table_name] = {
                                    "dataset": dataset,
                                    "table": table,
                                    "schema": schema,
                                    "semantic_tags": semantic_tags
                                }
                                
                                # Cache individual schema
                                self._schema_cache[full_table_name] = schema
                                logger.info(f"Cached table: {full_table_name} with tags: {semantic_tags}")
                                
                        except Exception as e:
                            logger.warning(f"Could not get schema for {full_table_name}: {e}")
                            
                            # Cache table without schema
                            self._table_cache[full_table_name] = {
                                "dataset": dataset,
                                "table": table,
                                "schema": {},
                                "semantic_tags": self._analyze_table_semantics_enhanced(table, {})
                            }
                            
                except Exception as e:
                    logger.warning(f"Could not process dataset {dataset}: {e}")
            
            # Analyze relationships
            await self._analyze_table_relationships()
            
            self._last_discovery = datetime.now()
            
            logger.info(f"Enhanced discovery complete. Cached {len(self._table_cache)} tables")
            
            return {
                "status": "discovery_complete",
                "tables_discovered": len(self._table_cache),
                "datasets": len(datasets),
                "relationships": len(self._relationship_cache),
                "discovery_time": self._last_discovery.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Enhanced metadata discovery failed: {e}")
            raise

    async def _analyze_table_relationships(self):
        """Enhanced relationship analysis"""
        logger.info("Analyzing enhanced table relationships...")
        
        # Look for common join patterns
        for table_name, table_info in self._table_cache.items():
            schema = table_info.get('schema', {})
            if not schema:
                continue
            
            columns = self._extract_schema_columns_enhanced(schema)
            
            # Look for foreign key patterns
            for column in columns['all']:
                column_lower = column.lower()
                
                # Enhanced ID pattern detection
                if column_lower.endswith('_id') or column_lower == 'id':
                    potential_ref = column_lower.replace('_id', '')
                    
                    # Find matching tables with higher confidence
                    for other_table, other_info in self._table_cache.items():
                        if other_table == table_name:
                            continue
                            
                        other_table_lower = other_info['table'].lower()
                        
                        # More sophisticated matching
                        match_confidence = 0
                        if potential_ref in other_table_lower:
                            match_confidence = 0.8
                        elif other_table_lower in potential_ref:
                            match_confidence = 0.6
                        
                        # Check for semantic similarity
                        if set(table_info['semantic_tags']) & set(other_info['semantic_tags']):
                            match_confidence += 0.2
                        
                        if match_confidence > 0.6:
                            relationship_key = f"{table_name}:{other_table}"
                            self._relationship_cache[relationship_key] = {
                                'source_table': table_name,
                                'target_table': other_table,
                                'join_column': column,
                                'relationship_type': 'potential_foreign_key',
                                'confidence': match_confidence
                            }

    def _suggest_joins(self, table_name: str) -> List[Dict[str, Any]]:
        """Enhanced join suggestions"""
        suggestions = []
        
        for rel_key, relationship in self._relationship_cache.items():
            if relationship['source_table'] == table_name or relationship['target_table'] == table_name:
                other_table = relationship['target_table'] if relationship['source_table'] == table_name else relationship['source_table']
                suggestions.append({
                    'with_table': other_table,
                    'join_column': relationship['join_column'],
                    'join_type': 'LEFT JOIN',
                    'confidence': relationship['confidence']
                })
        
        # Sort by confidence
        suggestions.sort(key=lambda x: x['confidence'], reverse=True)
        return suggestions[:5]  # Return top 5 suggestions

    async def generate_intelligent_sql(self, query_text: str, context: Dict = None) -> Dict[str, Any]:
        """Generate SQL with enhanced intelligent table discovery and multi-table capabilities."""
        
        # Discover relevant tables with enhanced logic
        discovery_result = await self.intelligent_table_discovery(query_text)
        relevant_tables = discovery_result['relevant_tables']
        query_intent = discovery_result['query_intent']
        
        if not relevant_tables:
            return {
                "status": "no_relevant_tables",
                "message": "Could not find relevant tables for the query",
                "query": query_text,
                "query_intent": query_intent,
                "suggestion": f"Try being more specific about the data source. Detected intent: {query_intent.get('primary_platform', 'unclear')}"
            }
        
        # Initialize multi-table processor
        complex_processor = self.ComplexQueryProcessor(self)
        
        # Check if this is a multi-platform/complex query
        cross_platform_intent = complex_processor.relationship_mapper.detect_cross_platform_intent(query_text)
        
        if cross_platform_intent['is_multi_platform'] or cross_platform_intent['has_comparison_intent']:
            logger.info(f"Processing multi-platform query: {cross_platform_intent}")
            try:
                multi_result = await complex_processor.process_multi_platform_query(query_text, relevant_tables)
                
                return {
                    "status": "multi_platform_sql_generated",
                    "sql_query": multi_result['sql'],
                    "query_type": multi_result['query_type'], 
                    "platforms_analyzed": multi_result.get('platforms_analyzed', []),
                    "explanation": multi_result.get('explanation', ''),
                    "table_context": {
                        "available_tables": relevant_tables[:4],
                        "query_intent": query_intent,
                        "cross_platform_intent": cross_platform_intent,
                        "complexity": "high"
                    },
                    "discovery_result": discovery_result
                }
            except Exception as e:
                logger.error(f"Multi-platform processing failed: {e}")
                # Fall back to single table processing
        
        # Single table processing (enhanced)
        account_name = self._extract_account_name_from_question_enhanced(query_text)
        logger.info(f"Enhanced account extraction result: {account_name}")
        
        # Select best table (already ranked by enhanced relevance)
        primary_table = relevant_tables[0]
        logger.info(f"Selected primary table: {primary_table['table_name']} (score: {primary_table['relevance_score']})")
        
        # Generate SQL with enhanced intelligence
        try:
            sql_query = await self._generate_intelligent_sql(
                query_text, primary_table, account_name, query_intent
            )
        except Exception as e:
            logger.error(f"Enhanced SQL generation failed: {e}")
            # Fallback SQL with some intelligence
            table_name = f"`{primary_table['dataset']}.{primary_table['table']}`"
            sql_query = f"SELECT * FROM {table_name} LIMIT 10"
        
        # Build enhanced context
        table_context = {
            "available_tables": relevant_tables[:3],
            "query_intent": query_intent,
            "suggested_joins": [],
            "time_filters": self._extract_time_filters_enhanced(query_text, query_intent),
            "account_name": account_name,
            "generated_sql": sql_query
        }
        
        # Add join suggestions if multiple relevant tables
        if len(relevant_tables) > 1:
            for i in range(1, min(3, len(relevant_tables))):
                secondary_table = relevant_tables[i]
                joins = self._find_join_path(primary_table['table_name'], secondary_table['table_name'])
                if joins:
                    table_context["suggested_joins"].extend(joins)
        
        return {
            "status": "context_generated",
            "table_context": table_context,
            "discovery_result": discovery_result,
            "recommended_approach": self._recommend_enhanced_query_approach(query_text, relevant_tables, query_intent)
        }

    def _extract_time_filters_enhanced(self, query_text: str, query_intent: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced time filter extraction using query intent"""
        time_filters = {}
        
        if query_intent.get('time_periods'):
            period = query_intent['time_periods'][0]
            time_filters['period'] = period
            
            if period == 'last_30_days':
                time_filters['sql_template'] = "DATE({date_column}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
            elif period == 'last_7_days':
                time_filters['sql_template'] = "DATE({date_column}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)"
            elif period == 'this_month':
                time_filters['sql_template'] = "DATE({date_column}) >= DATE_TRUNC(CURRENT_DATE(), MONTH)"
            elif period == 'this_year':
                time_filters['sql_template'] = "DATE({date_column}) >= DATE_TRUNC(CURRENT_DATE(), YEAR)"
        
        return time_filters

    def _find_join_path(self, table1: str, table2: str) -> List[Dict[str, Any]]:
        """Enhanced join path finding"""
        joins = []
        
        # Direct relationship
        direct_key = f"{table1}:{table2}"
        reverse_key = f"{table2}:{table1}"
        
        if direct_key in self._relationship_cache:
            rel = self._relationship_cache[direct_key]
            joins.append({
                'from_table': table1,
                'to_table': table2,
                'join_condition': f"{table1}.{rel['join_column']} = {table2}.{rel['join_column']}",
                'join_type': 'LEFT JOIN',
                'confidence': rel['confidence']
            })
        elif reverse_key in self._relationship_cache:
            rel = self._relationship_cache[reverse_key]
            joins.append({
                'from_table': table1,
                'to_table': table2,
                'join_condition': f"{table1}.{rel['join_column']} = {table2}.{rel['join_column']}",
                'join_type': 'LEFT JOIN',
                'confidence': rel['confidence']
            })
        
        return joins

    def _recommend_enhanced_query_approach(self, query_text: str, relevant_tables: List[Dict], query_intent: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced query approach recommendation"""
        
        if not relevant_tables:
            return {"approach": "no_data", "reason": "No relevant tables found"}
        
        primary_table = relevant_tables[0]
        
        # Platform-specific single table query
        if (primary_table['relevance_score'] > 50 and 
            query_intent.get('primary_platform') and 
            query_intent['primary_platform'] in primary_table.get('semantic_tags', [])):
            return {
                "approach": "single_table",
                "primary_table": primary_table['table_name'],
                "reason": f"High confidence platform match for {query_intent['primary_platform']} (score: {primary_table['relevance_score']})",
                "suggested_sql_template": f"SELECT * FROM `{primary_table['table_name']}` WHERE {{conditions}}"
            }
        
        # Multi-table analysis
        if len(relevant_tables) > 1 and primary_table.get('suggested_joins'):
            return {
                "approach": "multi_table_join",
                "primary_table": primary_table['table_name'],
                "join_tables": [t['table_name'] for t in relevant_tables[1:3]],
                "reason": "Multiple relevant tables with relationships found",
                "complexity": "high" if query_intent['query_complexity'] == 'high' else "medium"
            }
        
        # Aggregation-focused query
        if query_intent.get('needs_aggregation'):
            return {
                "approach": "aggregation",
                "primary_table": primary_table['table_name'],
                "reason": "Query requires aggregation analysis",
                "suggested_metrics": query_intent.get('metrics_requested', [])
            }
        
        return {
            "approach": "enhanced_select",
            "primary_table": primary_table['table_name'],
            "reason": f"Enhanced data retrieval for {query_intent.get('primary_platform', 'general')} data"
        }

    # Standard MCP interface methods (unchanged)
    async def execute_sql(self, sql: str, dry_run: bool = False) -> dict:
        """Execute SQL query via MCP"""
        logger.info(f"Executing enhanced SQL: {sql[:100]}...")
        return await self.call_tool("execute_sql", {
            "sql": sql,
            "dry_run": dry_run
        })

    async def list_dataset_ids(self, project: str = None) -> dict:
        """List BigQuery datasets"""
        params = {}
        if project:
            params["project"] = project
        return await self.call_tool("list_dataset_ids", params)

    async def list_table_ids(self, dataset: str, project: str = None) -> dict:
        """List tables in a dataset"""
        params = {"dataset": dataset}
        if project:
            params["project"] = project
        return await self.call_tool("list_table_ids", params)

    async def get_table_info(self, dataset: str, table: str, project: str = None) -> dict:
        """Get table schema and metadata"""
        params = {"dataset": dataset, "table": table}
        if project:
            params["project"] = project
        return await self.call_tool("get_table_info", params)

    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

# Global instance
intelligent_bigquery_mcp = IntelligentBigQueryMCP()