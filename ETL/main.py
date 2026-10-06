import os
import logging
import requests
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from dotenv import load_dotenv

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- CONFIG ---
load_dotenv()

API_URL = os.getenv("EXCHANGE_KEY", "").strip()
RAW_DB_URL = os.getenv("SUPABASE_URL", "").strip()

if not API_URL or not RAW_DB_URL:
    logger.error("Missing environment variables: EXCHANGE_KEY or SUPABASE_URL")
    exit(1)


def normalize_db_url(url_str: str) -> str:
    """Ensures dialect compatibility, URL-encodes passwords, and safely appends sslmode."""
    # Fix scheme prefix for SQLAlchemy
    if url_str.startswith("postgres://"):
        url_str = url_str.replace("postgres://", "postgresql://", 1)

    try:
        # Parse the connection components
        parsed = urlparse(url_str)
        
        # Safely URL-encode the password if present
        username = parsed.username or ""
        password = quote_plus(parsed.password) if parsed.password else ""
        user_info = f"{username}:{password}" if password else username
        
        # Reconstruct network location (host + port)
        netloc = f"{user_info}@{parsed.hostname}"
        if parsed.port:
            netloc += f":{parsed.port}"

        # Ensure sslmode=require is present in query parameters
        query_params = parse_qs(parsed.query)
        if "sslmode" not in query_params:
            query_params["sslmode"] = ["require"]
        
        new_query = urlencode(query_params, doseq=True)

        normalized_url = urlunparse((
            parsed.scheme,
            netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment
        ))

        # Validate with SQLAlchemy's URL parser before returning
        make_url(normalized_url)
        return normalized_url

    except Exception as err:
        logger.error(f"Failed to parse database URL: {err}")
        exit(1)


DB_URL = normalize_db_url(RAW_DB_URL)

# Optimized engine for Cloud/Serverless environments
engine = create_engine(
    DB_URL,
    pool_pre_ping=True,
    pool_recycle=300,  # Refresh connections before Supabase kills them
    connect_args={"connect_timeout": 10},
)


def capture_historical_rates():
    # 1. Fetch Data
    try:
        logger.info("Fetching data from API...")
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        data = response.json()

        rates_dict = data.get("conversion_rates", {})
        api_time = data.get("time_last_update_utc")

        if not rates_dict or not api_time:
            logger.error("Data missing in API response.")
            return

    except requests.exceptions.RequestException as e:
        logger.error(f"API Connection Error: {e}")
        return

    # 2. Transformation
    df = pd.DataFrame(list(rates_dict.items()), columns=["currency_code", "rate"])
    df["recorded_at"] = pd.to_datetime(api_time)
    df["rate"] = pd.to_numeric(df["rate"])

    # 3. Load into PostgreSQL with Idempotency Check
    try:
        with engine.begin() as conn:
            check_sql = text(
                "SELECT EXISTS(SELECT 1 FROM exchange_rates WHERE recorded_at = :t LIMIT 1)"
            )
            exists = conn.execute(check_sql, {"t": df["recorded_at"].iloc[0]}).scalar()

            if exists:
                logger.warning(f"Data for {api_time} already exists. Skipping upload.")
                return

            # Bulk Insert
            df.to_sql(
                name="exchange_rates",
                con=conn,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

        logger.info(f"✅ Saved {len(df)} rates for {api_time}")

    except Exception as e:
        logger.error(f"Database Error: {e}")


if __name__ == "__main__":
    capture_historical_rates()