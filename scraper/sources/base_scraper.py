"""
Base scraper class - all sources inherit from this

"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import hashlib
import re


class EmailValidator:
    """Validate and extract emails"""

    EMAIL_REGEX = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'

    @staticmethod
    def is_valid(email: str) -> bool:
        """Check if email format is valid"""
        if not email:
            return False
        return bool(re.match(EmailValidator.EMAIL_REGEX, email.strip()))

    @staticmethod
    def extract_emails(text: str) -> List[str]:
        """Extract all emails from text"""
        if not text:
            return []

        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
        # Filter valid emails and remove duplicates
        return list(set([email for email in emails if EmailValidator.is_valid(email)]))

    @staticmethod
    def filter_company_emails(emails: List[str], company_name: str) -> List[str]:
        """
        Filter emails to likely company emails
        Remove common free email providers
        """
        free_providers = [
            'gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com',
            'aol.com', 'mail.com', 'protonmail.com', 'icloud.com',
            'mail.ru', 'yandex.com', '163.com', 'qq.com'
        ]

        company_emails = []
        for email in emails:
            domain = email.split('@')[1].lower()
            # Keep if not a free provider
            if domain not in free_providers:
                company_emails.append(email)

        return company_emails


class BaseScraperJob:
    """Standard job data model"""

    def __init__(self):
        self.job_title: str = ""
        self.company_name: str = ""
        self.location: str = ""
        self.job_description: str = ""

        self.salary_min: int = None
        self.salary_max: int = None
        self.salary_raw: str = ""

        self.job_type: str = None
        self.job_url: str = ""
        self.source: str = ""
        self.source_job_id: str = ""

        # Email fields
        self.company_email: Optional[str] = None
        self.company_website: Optional[str] = None
        self.company_contact_page: Optional[str] = None

        self.posted_date: datetime = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion"""
        return {
            "job_title": self.job_title,
            "company_name": self.company_name,
            "location": self.location,
            "job_description": self.job_description,
            "salary_min": self.salary_min,
            "salary_max": self.salary_max,
            "salary_currency": "USD",
            "salary_raw": self.salary_raw,
            "job_type": self.job_type,
            "job_url": self.job_url,
            "source": self.source,
            "source_job_id": self.source_job_id,
            "posted_date": self.posted_date,
            "url_hash": self.get_url_hash(),
            # Email fields (NEW!)
            "company_email": self.company_email,
            "company_website": self.company_website,
            "company_contact_page": self.company_contact_page,
        }

    def get_url_hash(self) -> str:
        """Create unique hash from URL (for deduplication)"""
        return hashlib.md5(self.job_url.encode()).hexdigest()

    def is_valid(self) -> bool:
        """Validate that job has required fields"""
        required = [
            self.job_title,
            self.company_name,
            self.location,
            self.job_url,
            self.source,
        ]
        return all(required)

    def extract_emails_from_description(self) -> List[str]:
        """Extract emails from job description"""
        emails = EmailValidator.extract_emails(self.job_description)
        # Filter to likely company emails
        company_emails = EmailValidator.filter_company_emails(emails, self.company_name)
        return company_emails


class BaseScraper(ABC):
    """
    Abstract base class for all job scrapers
    """

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.jobs_found = 0
        self.emails_found = 0

    @abstractmethod
    async def fetch_jobs(
        self,
        queries: List[str],
        locations: List[str],
        max_results: int = 100,
    ) -> List[BaseScraperJob]:
        """Fetch jobs from the source"""
        pass

    def log(self, message: str):
        """Log messages"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{self.source_name}] {message}")