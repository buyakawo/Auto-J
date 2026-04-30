"""
Indeed.com Job Scraper
"""

import httpx
import asyncio
import re
import json
from typing import List, Optional
from datetime import datetime
from urllib.parse import urlencode

from .base_scraper import BaseScraper, BaseScraperJob, EmailValidator


class IndeedJob(BaseScraperJob):
    """Indeed specific job data"""
    pass


class IndeedScraper(BaseScraper):
    """
    Scrapes job listings from Indeed.com
    Extracts company emails for outreach
    """

    def __init__(self, request_delay: float = 2.0, timeout: int = 30):
        super().__init__("Indeed")
        self.request_delay = request_delay
        self.timeout = timeout

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

    async def fetch_jobs(
        self,
        queries: List[str],
        locations: List[str],
        max_results: int = 100,
    ) -> List[IndeedJob]:
        """Fetch jobs from Indeed"""
        self.log(f"Starting scrape with {len(queries)} queries and {len(locations)} locations")

        all_jobs = []

        async with httpx.AsyncClient(headers=self.headers, timeout=self.timeout) as client:
            for location in locations:
                for query in queries:
                    self.log(f"Scraping: {query} in {location}")

                    try:
                        jobs = await self._scrape_search_page(
                            client, query, location, max_results
                        )
                        all_jobs.extend(jobs)

                        await asyncio.sleep(self.request_delay)

                    except Exception as e:
                        self.log(f"Error scraping {query} in {location}: {str(e)}")
                        continue

        self.jobs_found = len(all_jobs)
        self.log(f"Found {self.jobs_found} total jobs")
        return all_jobs

    async def _scrape_search_page(
        self,
        client: httpx.AsyncClient,
        query: str,
        location: str,
        max_results: int,
    ) -> List[IndeedJob]:
        """Scrape search results"""

        jobs = []
        collected = 0
        max_pages = min((max_results + 9) // 10, 100)

        for page in range(max_pages):
            if collected >= max_results:
                break

            offset = page * 10
            url = self._build_search_url(query, location, offset)

            try:
                response = await client.get(url)
                response.raise_for_status()

                page_jobs = self._parse_search_page(response.text, query, location)

                if not page_jobs:
                    self.log(f"No more results for {query} in {location}")
                    break

                jobs.extend(page_jobs)
                collected += len(page_jobs)

                self.log(f"Page {page + 1}: Found {len(page_jobs)} jobs")

                await asyncio.sleep(self.request_delay)

            except httpx.HTTPError as e:
                self.log(f"HTTP error on page {page}: {str(e)}")
                break

        return jobs[:max_results]

    def _build_search_url(self, query: str, location: str, offset: int = 0) -> str:
        """Build Indeed search URL"""
        params = {
            "q": query,
            "l": location,
            "filter": 0,
            "start": offset,
        }
        return "https://www.indeed.com/jobs?" + urlencode(params)

    def _parse_search_page(self, html: str, query: str, location: str) -> List[IndeedJob]:
        """Parse job listings and extract emails"""
        jobs = []

        try:
            pattern = r'window.mosaic.providerData\["mosaic-provider-jobcards"\]=(\{.+?\});'
            match = re.search(pattern, html, re.DOTALL)

            if not match:
                self.log("Could not find job data in page")
                return jobs

            data = json.loads(match.group(1))
            results = data["metaData"]["mosaicProviderJobCardsModel"]["results"]

            for result in results:
                job = self._parse_job_result(result)
                if job and job.is_valid():
                    # Extract emails from job description
                    emails = job.extract_emails_from_description()
                    if emails:
                        job.company_email = emails[0]  # Use first email found
                        self.emails_found += 1
                        self.log(f"Found email for {job.company_name}: {job.company_email}")

                    jobs.append(job)

        except (json.JSONDecodeError, KeyError, TypeError) as e:
            self.log(f"Error parsing page: {str(e)}")

        return jobs

    def _parse_job_result(self, result: dict) -> Optional[IndeedJob]:
        """Parse individual job result"""
        try:
            job = IndeedJob()

            job.job_title = result.get("title", "").strip()
            job.company_name = result.get("company", "").strip()
            job.location = result.get("formattedLocation", "").strip()
            job.job_url = f"https://www.indeed.com/m/basecamp/viewjob?viewtype=embedded&jk={result.get('jobkey', '')}"
            job.source = "Indeed"
            job.source_job_id = result.get("jobkey", "")

            job.job_description = result.get("snippet", "").strip()
            job.posted_date = self._parse_date(result.get("pubDate"))
            job.salary_raw = result.get("salarySnippet", {}).get("salaryTextFormatted", "")

            return job if job.is_valid() else None

        except Exception as e:
            self.log(f"Error parsing job result: {str(e)}")
            return None

    def _parse_date(self, timestamp_ms: Optional[int]) -> Optional[datetime]:
        """Convert Indeed timestamp to datetime"""
        if not timestamp_ms:
            return None

        try:
            return datetime.fromtimestamp(int(timestamp_ms) / 1000)
        except (ValueError, TypeError):
            return None