from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from connectors.base import BaseConnector
from config.settings import settings
import pandas as pd
from datetime import datetime

class GoogleAdsConnector(BaseConnector):
    def __init__(self, developer_token: str = None, client_id: str = None, client_secret: str = None, refresh_token: str = None, login_customer_id: str = None, customer_ids: str = None):
        credentials = {
            "developer_token": developer_token or settings.google_ads_developer_token,
            "client_id": client_id or settings.google_ads_client_id,
            "client_secret": client_secret or settings.google_ads_client_secret,
            "refresh_token": refresh_token or settings.google_ads_refresh_token,
            "login_customer_id": login_customer_id or settings.google_ads_login_customer_id,
            "use_proto_plus": True
        }
        self.client = GoogleAdsClient.load_from_dict(credentials, version="v17")
        # customer_ids can be a comma-separated string if multiple
        current_customer_ids = customer_ids or settings.google_ads_customer_id
        self.customer_ids = current_customer_ids.split(",") if current_customer_ids else []

    def get_tables_ddl(self) -> list:
        return [
            """
            CREATE TABLE IF NOT EXISTS google_ads_campaigns (
                customer_id String,
                campaign_id String,
                campaign_name String,
                campaign_status String,
                advertising_channel_type String,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (customer_id, campaign_id)
            """,
            """
            CREATE TABLE IF NOT EXISTS google_ads_insights (
                customer_id String,
                campaign_id String,
                date Date,
                impressions UInt64,
                clicks UInt64,
                cost Float64,
                conversions Float64,
                updated_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (customer_id, campaign_id, date)
            """
        ]

    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        start_str = date_start.strftime('%Y-%m-%d')
        end_str = date_stop.strftime('%Y-%m-%d')

        all_campaigns = []
        all_insights = []

        metrics_query = f"""
            SELECT
                campaign.id,
                campaign.name,
                campaign.status,
                campaign.advertising_channel_type,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.conversions,
                segments.date
            FROM campaign
            WHERE segments.date BETWEEN '{start_str}' AND '{end_str}'
        """

        ga_service = self.client.get_service("GoogleAdsService")

        for customer_id in self.customer_ids:
            customer_id = customer_id.strip().replace("-", "")
            if not customer_id:
                continue
                
            try:
                # Use search stream for better performance on large datasets
                stream = ga_service.search_stream(customer_id=customer_id, query=metrics_query)
                
                campaign_ids_seen = set()

                for batch in stream:
                    for row in batch.results:
                        camp_id = str(row.campaign.id)
                        
                        # Add to campaigns if not seen
                        if camp_id not in campaign_ids_seen:
                            campaign_ids_seen.add(camp_id)
                            all_campaigns.append({
                                "customer_id": customer_id,
                                "campaign_id": camp_id,
                                "campaign_name": row.campaign.name,
                                "campaign_status": row.campaign.status.name,
                                "advertising_channel_type": row.campaign.advertising_channel_type.name
                            })

                        # Add to insights
                        all_insights.append({
                            "customer_id": customer_id,
                            "campaign_id": camp_id,
                            "date": row.segments.date,
                            "impressions": row.metrics.impressions,
                            "clicks": row.metrics.clicks,
                            "cost": row.metrics.cost_micros / 1000000.0, # Convert micros to currency
                            "conversions": row.metrics.conversions
                        })
            except GoogleAdsException as ex:
                print(f"Google Ads API Error on customer {customer_id}: {ex}")
                continue
            except Exception as e:
                print(f"Unknown error on customer {customer_id}: {e}")
                continue

        return {
            "google_ads_campaigns": pd.DataFrame(all_campaigns),
            "google_ads_insights": pd.DataFrame(all_insights)
        }
