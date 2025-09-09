from fastapi import APIRouter, HTTPException
from api.data_sources import DataSources
import asyncio
from typing import Optional

router = APIRouter()
data_sources = DataSources()

@router.get("/dashboard/campaigns")
async def get_campaign_data(days: int = 30):
    """Get campaign performance data"""
    try:
        data = await data_sources.get_campaign_performance(days)
        return {"status": "success", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching campaign data: {str(e)}")

@router.get("/dashboard/trends")
async def get_account_trend(days: int = 7):
    """Get account performance trend"""
    try:
        data = await data_sources.get_account_performance_trend(days)
        return {"status": "success", "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching trend data: {str(e)}")

@router.get("/dashboard/summary")
async def get_dashboard_summary():
    """Get combined dashboard summary"""
    try:
        # Run multiple queries concurrently
        campaigns, trends = await asyncio.gather(
            data_sources.get_campaign_performance(30),
            data_sources.get_account_performance_trend(7),
            return_exceptions=True
        )
        
        result = {
            "status": "success",
            "data": {}
        }
        
        if not isinstance(campaigns, Exception):
            result["data"]["campaigns"] = campaigns
        if not isinstance(trends, Exception):
            result["data"]["trends"] = trends
            
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching summary: {str(e)}")