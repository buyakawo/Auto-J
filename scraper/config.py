import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv(".env.local")

class ScraperConfig:
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY", "")
    SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

    ENABLED_SOURCES = [s.strip() for s in os.getenv("ENABLED_SOURCES", "indeed").split(",")]

    INDEED_ENABLED = os.getenv("INDEED_ENABLED", "true").lower() == "true"
    INDEED_SEARCH_QUERIES = [q.strip() for q in os.getenv(
        "INDEED_SEARCH_QUERIES",
        "software engineer,backend developer"
    ).split(",")]
    INDEED_SEARCH_LOCATIONS = [l.strip() for l in os.getenv(
        "INDEED_SEARCH_LOCATIONS",
        "Jakarta,Indonesia"
    ).split(",")]

    MAX_RESULTS_PER_SEARCH = int(os.getenv("MAX_RESULTS_PER_SEARCH", "100"))
    REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "2.0"))
    REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

    @classmethod
    def validate(cls) -> bool:
        required = ["SUPABASE_URL", "SUPABASE_API_KEY"]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            logger.error(f"Missing required config: {missing}")
            return False
        return True

    @classmethod
    def log_config(cls):
        config_summary = (
            "\n=== Scraper Configuration ===\n"
            f"Enabled Sources: {cls.ENABLED_SOURCES}\n"
            f"Indeed Queries: {len(cls.INDEED_SEARCH_QUERIES)} queries\n"
            f"Indeed Locations: {len(cls.INDEED_SEARCH_LOCATIONS)} locations\n"
            f"Max Results: {cls.MAX_RESULTS_PER_SEARCH}\n"
            f"Request Delay: {cls.REQUEST_DELAY}s\n"
            "=============================="
        )
        logger.info(config_summary)