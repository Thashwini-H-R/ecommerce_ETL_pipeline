"""Data ingestion module for e-commerce ETL pipeline.

Features:
- Connectors for Shopify, WooCommerce, Stripe, PayPal (REST)
- Optional inventory DB connector (Postgres via psycopg2)
- Retry and rate-limit handling via decorators
- Store raw payloads to local `staging/` folder or upload to S3

Usage:
    from ingest import Ingestor

    ing = Ingestor(staging_dir="./staging")
    ing.ingest_shopify_orders({...})

This module intentionally keeps connectors lightweight and configurable.
"""

from __future__ import annotations

import csv
import functools
import io
import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, Optional

import requests

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except Exception:
    boto3 = None  # type: ignore

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:
    psycopg2 = None  # type: ignore

logger = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def retry_rate_limit(
    max_retries: int = 5,
    backoff_factor: float = 2.0,
    min_interval: float = 0.0,
    status_forcelist: Optional[Iterable[int]] = (429, 500, 502, 503, 504),
):
    """Decorator adding retry and simple rate-limit spacing.

    - Retries on exceptions and on responses with status codes in status_forcelist.
    - If response contains `Retry-After` header, sleeps for that many seconds before retry.
    - Ensures at least `min_interval` seconds between calls to the decorated function.
    """

    def decorator(func: Callable[..., requests.Response]):
        last_called = {"t": 0.0}

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                # rate spacing
                elapsed = time.time() - last_called["t"]
                if elapsed < min_interval:
                    to_sleep = min_interval - elapsed
                    logger.debug("Rate limiter sleeping %.2fs", to_sleep)
                    time.sleep(to_sleep)

                try:
                    resp = func(*args, **kwargs)
                except requests.RequestException as exc:
                    attempts += 1
                    if attempts > max_retries:
                        logger.exception("Max retries reached for %s", func.__name__)
                        raise
                    sleep = backoff_factor ** attempts
                    logger.warning("Request exception, retrying in %.1fs (%s)", sleep, exc)
                    time.sleep(sleep)
                    continue

                # ensure we record call time only on a successful request send
                last_called["t"] = time.time()

                if resp is None:
                    attempts += 1
                    if attempts > max_retries:
                        raise RuntimeError("Empty response after retries")
                    time.sleep(backoff_factor ** attempts)
                    continue

                if resp.status_code in (status_forcelist or []):
                    attempts += 1
                    if attempts > max_retries:
                        logger.error("Max retries reached, last status=%s", resp.status_code)
                        resp.raise_for_status()

                    # handle Retry-After header
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            sleep = int(retry_after)
                        except ValueError:
                            sleep = float(retry_after)
                    else:
                        sleep = backoff_factor ** attempts

                    logger.warning(
                        "Received status %s, sleeping %.1fs before retry (%s/%s)",
                        resp.status_code,
                        sleep,
                        attempts,
                        max_retries,
                    )
                    time.sleep(sleep)
                    continue

                return resp

        return wrapper

    return decorator


class Storage:
    def __init__(self, staging_dir: str = "./staging", s3_bucket: Optional[str] = None, s3_prefix: Optional[str] = None):
        self.staging_dir = staging_dir
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix or ""
        os.makedirs(self.staging_dir, exist_ok=True)

        if self.s3_bucket and boto3 is None:
            raise RuntimeError("boto3 is required for S3 storage but is not installed")

        if self.s3_bucket:
            self.s3 = boto3.client("s3")  # type: ignore
        else:
            self.s3 = None

    def _local_path(self, filename: str) -> str:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        safe_name = f"{ts}_{filename}"
        return os.path.join(self.staging_dir, safe_name)

    def save_json(self, payload: Any, filename: str) -> str:
        path = self._local_path(filename if filename.endswith('.json') else f"{filename}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str)
        logger.info("Wrote payload to %s", path)

        if self.s3 and self.s3_bucket:
            key = os.path.join(self.s3_prefix, os.path.basename(path))
            try:
                self.s3.upload_file(path, self.s3_bucket, key)
                logger.info("Uploaded %s to s3://%s/%s", path, self.s3_bucket, key)
            except (BotoCoreError, ClientError) as exc:
                logger.exception("Failed uploading to S3: %s", exc)

        return path

    def save_csv(self, rows: Iterable[Dict[str, Any]], filename: str) -> str:
        path = self._local_path(filename if filename.endswith('.csv') else f"{filename}.csv")
        rows = list(rows)
        if not rows:
            # write an empty file
            open(path, "w", encoding="utf-8").close()
            return path

        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

        logger.info("Wrote CSV to %s", path)
        if self.s3 and self.s3_bucket:
            key = os.path.join(self.s3_prefix, os.path.basename(path))
            try:
                self.s3.upload_file(path, self.s3_bucket, key)
                logger.info("Uploaded %s to s3://%s/%s", path, self.s3_bucket, key)
            except (BotoCoreError, ClientError) as exc:
                logger.exception("Failed uploading CSV to S3: %s", exc)

        return path


class Ingestor:
    def __init__(self, staging_dir: str = "./staging", s3_bucket: Optional[str] = None, s3_prefix: Optional[str] = None):
        self.storage = Storage(staging_dir=staging_dir, s3_bucket=s3_bucket, s3_prefix=s3_prefix)
        self.session = requests.Session()

    # Copilot-style generation: Shopify ingestion function with retry
    @retry_rate_limit(max_retries=5, backoff_factor=2.0, min_interval=0.2)
    def _shopify_request(self, url: str, headers: Dict[str, str], params: Dict[str, Any]):
        return self.session.get(url, headers=headers, params=params, timeout=30)

    def ingest_shopify_orders(self, shop_name: str, access_token: Optional[str] = None, api_key: Optional[str] = None, password: Optional[str] = None, api_version: str = "2023-10", since_id: Optional[int] = None, limit: int = 250, store_prefix: str = "shopify_orders") -> None:
        """Ingest Shopify orders and store raw JSON payloads.

        Args:
            shop_name: my-shop.myshopify.com (without https://)
            access_token: private app access token (preferred)
            api_key/password: legacy basic auth creds
            api_version: Shopify API version
        """
        base = f"https://{shop_name}/admin/api/{api_version}/orders.json"
        params = {"limit": limit}
        if since_id:
            params["since_id"] = since_id

        headers = {"Content-Type": "application/json"}
        if access_token:
            headers["X-Shopify-Access-Token"] = access_token

        page = 1
        more = True
        while more:
            params_local = params.copy()
            params_local["page"] = page
            logger.info("Fetching Shopify orders page=%s", page)
            if api_key and password and not access_token:
                resp = self.session.get(base, auth=(api_key, password), params=params_local, timeout=30)
            else:
                resp = self._shopify_request(base, headers=headers, params=params_local)

            if resp.status_code != 200:
                logger.error("Shopify request failed: %s %s", resp.status_code, resp.text[:500])
                break

            data = resp.json()
            orders = data.get("orders", [])
            if not orders:
                logger.info("No more orders on page %s", page)
                break

            filename = f"{store_prefix}_page{page}.json"
            self.storage.save_json(orders, filename)

            # Shopify pagination via page parameter (legacy) — stop if less than limit
            if len(orders) < limit:
                more = False
            else:
                page += 1

    @retry_rate_limit(max_retries=5, backoff_factor=2.0, min_interval=0.2)
    def _generic_get(self, url: str, headers: Dict[str, str], params: Dict[str, Any]):
        return self.session.get(url, headers=headers, params=params, timeout=30)

    def ingest_woocommerce_orders(self, base_url: str, consumer_key: str, consumer_secret: str, per_page: int = 100, store_prefix: str = "woocommerce_orders") -> None:
        """Ingest WooCommerce orders using the REST API (WP-API).

        Example base_url: https://example.com/wp-json/wc/v3
        """
        url = f"{base_url}/orders"
        page = 1
        while True:
            params = {"per_page": per_page, "page": page}
            logger.info("Fetching WooCommerce orders page=%s", page)
            resp = self._generic_get(url, headers={}, params=params) if False else self.session.get(url, auth=(consumer_key, consumer_secret), params=params, timeout=30)

            if resp.status_code != 200:
                logger.error("WooCommerce request failed: %s %s", resp.status_code, resp.text[:300])
                break

            data = resp.json()
            if not data:
                break

            filename = f"{store_prefix}_page{page}.json"
            self.storage.save_json(data, filename)
            if len(data) < per_page:
                break
            page += 1

    def ingest_stripe_charges(self, api_key: str, limit: int = 100, store_prefix: str = "stripe_charges") -> None:
        """Ingest Stripe charges (uses pagination with starting_after).

        Note: this function uses the public charges list endpoint. For larger data volumes, use Stripe exports.
        """
        url = "https://api.stripe.com/v1/charges"
        headers = {"Authorization": f"Bearer {api_key}"}
        params = {"limit": limit}
        starting_after = None
        page = 1
        while True:
            if starting_after:
                params["starting_after"] = starting_after
            logger.info("Fetching Stripe charges page=%s", page)
            resp = self.session.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code != 200:
                logger.error("Stripe request failed: %s %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            charges = data.get("data", [])
            if not charges:
                break
            filename = f"{store_prefix}_page{page}.json"
            self.storage.save_json(charges, filename)
            if not data.get("has_more"):
                break
            starting_after = charges[-1].get("id")
            page += 1

    def ingest_paypal_transactions(self, base_url: str, access_token: str, store_prefix: str = "paypal_transactions") -> None:
        """Ingest PayPal transactions using the provided access token and base API URL.

        base_url example: https://api-m.sandbox.paypal.com
        """
        url = f"{base_url}/v1/reporting/transactions"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        params = {"start_date": (datetime.utcnow().date().isoformat())}
        logger.info("Fetching PayPal transactions")
        resp = self.session.get(url, headers=headers, params=params, timeout=60)
        if resp.status_code != 200:
            logger.error("PayPal request failed: %s %s", resp.status_code, resp.text[:300])
            return
        data = resp.json()
        filename = f"{store_prefix}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.json"
        self.storage.save_json(data, filename)

    def ingest_inventory_db(self, dsn: Optional[str] = None, host: Optional[str] = None, port: int = 5432, dbname: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None, query: str = "SELECT * FROM inventory LIMIT 1000", store_prefix: str = "inventory_snapshot") -> None:
        """Pull a snapshot from an inventory Postgres database and write CSV/JSON to staging.

        Either provide DSN or host/db/user/password.
        """
        if psycopg2 is None:
            logger.warning("psycopg2 not installed, skipping inventory DB ingestion")
            return

        conn = None
        try:
            if dsn:
                conn = psycopg2.connect(dsn)
            else:
                conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(query)
            rows = cur.fetchall()
            filename = f"{store_prefix}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
            self.storage.save_csv(rows, filename)
        except Exception:
            logger.exception("Error ingesting inventory DB")
        finally:
            if conn:
                conn.close()


if __name__ == "__main__":
    # small smoke example
    ing = Ingestor(staging_dir="./staging")
    # Example: run the Shopify ingest if you provide credentials/environment
    print("Ingest module ready. Configure and call methods from your app or scripts.")
