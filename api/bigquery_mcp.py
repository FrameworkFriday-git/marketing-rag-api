# api/bigquery_mcp.py - Basic Working MCP Connector
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

# Global instance
bigquery_mcp = BigQueryMCP()