import json
import os
from google.cloud import bigquery
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

class DataSources:
    def __init__(self):
        # Initialize BigQuery client
        self.bq_client = bigquery.Client()
        
        # Initialize Analytics client
        self.analytics_client = BetaAnalyticsDataClient()
        
        # Your BigQuery project and dataset
        self.project_id = "data-tables-for-zoho"
        self.dataset_id = "new_data_tables"

    async def get_campaign_performance(self, days=30):
        """Get Google Ads campaign performance from BigQuery"""
        query = f"""
        SELECT 
            campaign_name,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost_micros)/1000000 as cost,
            ROUND(SUM(clicks)/SUM(impressions) * 100, 2) as ctr
        FROM `{self.project_id}.{self.dataset_id}.ads_campaign_performance_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY campaign_name
        ORDER BY cost DESC
        LIMIT 20
        """
        
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]

    async def get_account_performance_trend(self, days=30):
        """Get account performance trend over time"""
        query = f"""
        SELECT 
            date,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost_micros)/1000000 as cost
        FROM `{self.project_id}.{self.dataset_id}.ads_account_performance_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY date
        ORDER BY date DESC
        """
        
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]

    async def get_ga4_user_summary(self, days=30):
        """Get GA4 user behavior data"""
        query = f"""
        SELECT 
            date,
            new_users,
            returning_users,
            sessions,
            bounce_rate,
            avg_session_duration
        FROM `{self.project_id}.{self.dataset_id}.ga4_user_summary`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        ORDER BY date DESC
        LIMIT 100
        """
        
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]

    async def get_top_performing_keywords(self, limit=50):
        """Get top performing keywords"""
        query = f"""
        SELECT 
            keyword_text,
            SUM(impressions) as impressions,
            SUM(clicks) as clicks,
            SUM(cost_micros)/1000000 as cost,
            ROUND(SUM(clicks)/SUM(impressions) * 100, 2) as ctr
        FROM `{self.project_id}.{self.dataset_id}.ads_keyword_performance_daily`
        WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY keyword_text
        HAVING SUM(impressions) > 100
        ORDER BY clicks DESC
        LIMIT {limit}
        """
        
        query_job = self.bq_client.query(query)
        results = query_job.result()
        
        return [dict(row) for row in results]