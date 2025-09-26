# api/bigquery_mcp.py - Enhanced for Dashboard Support
import asyncio
import json
import aiohttp
import os
import logging

logger = logging.getLogger(__name__)

class SimpleBigQueryMCP:
    def __init__(self):
        self.server_url = os.getenv("BIGQUERY_MCP_URL", "https://bigquery.fridaysolutions.ai/mcp")
        self.session = None
        self.request_id = 0

    async def get_session(self):
        """Get or create aiohttp session"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=60)  # Increased timeout for dashboard queries
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

    async def execute_sql(self, sql: str, dry_run: bool = False) -> dict:
        """Execute SQL query via MCP"""
        logger.info(f"Executing SQL: {sql[:100]}...")
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

    async def get_dataset_info(self, dataset: str, project: str = None) -> dict:
        """Get dataset metadata"""
        params = {"dataset": dataset}
        if project:
            params["project"] = project
        return await self.call_tool("get_dataset_info", params)

    # Legacy method names for backward compatibility
    async def list_datasets(self, project: str = None) -> dict:
        """List BigQuery datasets - Legacy method"""
        return await self.list_dataset_ids(project)

    async def list_tables(self, dataset: str, project: str = None) -> dict:
        """List tables in a dataset - Legacy method"""
        return await self.list_table_ids(dataset, project)

    async def test_connection(self) -> dict:
        """Test if BigQuery MCP is working"""
        try:
            result = await self.execute_sql("SELECT 1 as test", dry_run=True)
            return {
                "status": "connected", 
                "test_result": result,
                "server_url": self.server_url
            }
        except Exception as e:
            return {
                "status": "failed", 
                "error": str(e),
                "server_url": self.server_url
            }

    async def test_dashboard_queries(self) -> dict:
        """Test dashboard-specific queries"""
        try:
            # Test consolidated table access
            test_query = "SELECT COUNT(*) as row_count FROM `data-tables-for-zoho.new_data_tables.consolidated_master` LIMIT 1"
            result = await self.execute_sql(test_query)
            
            if result.get("isError"):
                return {
                    "status": "failed",
                    "error": "Cannot access consolidated_master table",
                    "details": result
                }
            
            return {
                "status": "dashboard_ready",
                "consolidated_table": "accessible",
                "test_result": result,
                "server_url": self.server_url
            }
            
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "server_url": self.server_url
            }

    async def close(self):
        """Close the HTTP session"""
        if self.session and not self.session.closed:
            await self.session.close()

# Global instance
bigquery_mcp = SimpleBigQueryMCP()