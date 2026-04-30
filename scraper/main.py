"""
Main scraper orchestrator with email tracking
"""

import asyncio
from typing import List

from config import ScraperConfig
from database import Database
from sources.indeed import IndeedScraper
from sources.base_scraper import BaseScraper


class JobScraper:
    """
    Main orchestrator for all job scraping

    """

    def __init__(self):
        self.config = ScraperConfig
        self.db = Database()
        self.scrapers: List[BaseScraper] = []
        self._init_scrapers()

    def _init_scrapers(self):
        """Initialize enabled scrapers"""
        self.log("Initializing scrapers...")

        if "indeed" in self.config.ENABLED_SOURCES and self.config.INDEED_ENABLED:
            self.scrapers.append(
                IndeedScraper(
                    request_delay=self.config.REQUEST_DELAY,
                    timeout=self.config.REQUEST_TIMEOUT,
                )
            )
            self.log("Indeed scraper enabled")

        if not self.scrapers:
            self.log("ERROR: No scrapers enabled!")
            raise RuntimeError("No job sources configured")

    async def scrape_all(self) -> dict:
        """Run all scrapers """
        self.log("Starting job scraping...")
        self.config.print_config()

        total_stats = {
            "total_jobs": 0,
            "total_saved": 0,
            "total_duplicates": 0,
            "total_emails": 0,
            "by_source": {},
        }

        for scraper in self.scrapers:
            try:
                # Fetch jobs
                jobs = await scraper.fetch_jobs(
                    queries=[q.strip() for q in self.config.INDEED_SEARCH_QUERIES],
                    locations=[l.strip() for l in self.config.INDEED_SEARCH_LOCATIONS],
                    max_results=self.config.MAX_RESULTS_PER_SEARCH,
                )

                # Save jobs and extract emails
                save_stats = await self.db.save_jobs(jobs)

                # Log run
                self.db.log_scrape_run(
                    source=scraper.source_name,
                    status="success",
                    jobs_found=scraper.jobs_found,
                    jobs_saved=save_stats["saved"],
                    jobs_skipped=save_stats["duplicate"],
                    emails_found=save_stats["emails"],
                )

                # Accumulate stats
                total_stats["total_jobs"] += scraper.jobs_found
                total_stats["total_saved"] += save_stats["saved"]
                total_stats["total_duplicates"] += save_stats["duplicate"]
                total_stats["total_emails"] += save_stats["emails"]
                total_stats["by_source"][scraper.source_name] = {
                    "found": scraper.jobs_found,
                    "saved": save_stats["saved"],
                    "duplicates": save_stats["duplicate"],
                    "emails": save_stats["emails"],
                }

            except Exception as e:
                self.log(f"ERROR in {scraper.source_name}: {str(e)}")
                self.db.log_scrape_run(
                    source=scraper.source_name,
                    status="error",
                    jobs_found=0,
                    jobs_saved=0,
                    jobs_skipped=0,
                    emails_found=0,
                    error_message=str(e),
                )

        return total_stats

    def log(self, message: str):
        """Log messages"""
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [Scraper] {message}")

    def print_summary(self, stats: dict):
        """Print scraping summary with email stats"""
        print("\n" + "="*50)
        print("SCRAPING SUMMARY")
        print("="*50)
        print(f"Total jobs found: {stats['total_jobs']}")
        print(f"Total saved: {stats['total_saved']}")
        print(f"Total duplicates: {stats['total_duplicates']}")
        print(f"Total emails extracted: {stats['total_emails']}")  # NEW!

        print("\nBy Source:")
        for source, source_stats in stats["by_source"].items():
            print(f"  {source}:")
            print(f"    Found: {source_stats['found']}")
            print(f"    Saved: {source_stats['saved']}")
            print(f"    Duplicates: {source_stats['duplicates']}")
            print(f"    Emails: {source_stats['emails']}")  # NEW!

        # Database stats
        total_jobs_in_db = self.db.get_jobs_count()
        total_emails_in_db = self.db.get_emails_count()

        print(f"\nDatabase:")
        print(f"  Total jobs in database: {total_jobs_in_db}")
        print(f"  Total unique emails: {total_emails_in_db}")

        print("="*50 + "\n")


async def main():
    """Entry point"""

    if not ScraperConfig.validate():
        print("Configuration validation failed!")
        return

    scraper = JobScraper()
    stats = await scraper.scrape_all()
    scraper.print_summary(stats)


if __name__ == "__main__":
    asyncio.run(main())