from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from connectors.base import BaseConnector
from config.settings import settings
import pandas as pd
from datetime import datetime
import time

class MetaAdsConnector(BaseConnector):
    def __init__(self, app_id: str = None, app_secret: str = None, access_token: str = None, account_ids: str = None):
        self.app_id = app_id or settings.meta_app_id
        self.app_secret = app_secret or settings.meta_app_secret
        self.access_token = access_token or settings.meta_access_token
        self.account_ids = (account_ids or settings.meta_ad_account_ids or "").split(",") if (account_ids or settings.meta_ad_account_ids) else []
        self._api = None

    @property
    def api(self):
        if self._api is None:
             if not self.app_id or not self.access_token:
                 raise ValueError("Meta App ID and Access Token must be provided in the Spreadsheet or .env")
             
             self._api = FacebookAdsApi.init(
                 self.app_id,
                 self.app_secret,
                 self.access_token
             )
        return self._api

    def get_tables_ddl(self) -> list:
        return [
            f"""
            CREATE TABLE IF NOT EXISTS meta_campaigns (
                id String,
                name String,
                status String,
                objective String,
                account_id String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY id
            """,
            f"""
            CREATE TABLE IF NOT EXISTS meta_ad_insights (
                ad_id String,
                ad_name String,
                date_start Date,
                impressions UInt64,
                clicks UInt64,
                spend Float64,
                reach UInt64,
                ctr Float64,
                cpc Float64,
                cpm Float64,
                account_id String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (ad_id, date_start)
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        all_campaigns = []
        all_insights = []

        params = {
            'time_range': {
                'since': date_start.strftime('%Y-%m-%d'),
                'until': date_stop.strftime('%Y-%m-%d'),
            },
        }

        for acc_id in self.account_ids:
            account = AdAccount(acc_id)
            
            # Extract Campaigns
            campaigns = account.get_campaigns(fields=['id', 'name', 'status', 'objective'])
            for camp in campaigns:
                d = camp.export_all_data()
                d['account_id'] = acc_id
                all_campaigns.append(d)

            # Extract Insights (granularity: ad)
            insight_fields = [
                'ad_id', 'ad_name', 'date_start', 'impressions', 
                'clicks', 'spend', 'reach', 'ctr', 'cpc', 'cpm'
            ]
            insights_params = {
                **params,
                'level': 'ad',
                'time_increment': 1, # Diário
            }
            
            insights = account.get_insights(fields=insight_fields, params=insights_params)
            for ins in insights:
                d = ins.export_all_data()
                d['account_id'] = acc_id
                all_insights.append(d)

        return {
            "meta_campaigns": pd.DataFrame(all_campaigns),
            "meta_ad_insights": pd.DataFrame(all_insights)
        }
