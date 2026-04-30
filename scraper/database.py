"""
Supabase database connection and operations

"""

import os
from typing import List, Dict, Any
from supabase import create_client, Client
from sources.base_scraper import BaseScraperJob, EmailValidator


class Database:
    """
    Handles all database operations
    Now tracks company emails for outreach
    """

    def __init__(self):
        self.supabase: Client = create_client(
            os.getenv("SUPABASE_URL"),
            os.getenv("SUPABASE_API_KEY"),
        )
        self.log("Connected to Supabase")

    def log(self, message: str):
        timestamp = __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [Database] {message}")

    async def save_jobs(self, jobs: List[BaseScraperJob]) -> Dict[str, int]:
        """
        Save jobs to database
        Extracts and stores company emails separately
        """
        stats = {"saved": 0, "duplicate": 0, "errors": 0, "emails": 0}

        if not jobs:
            self.log("No jobs to save")
            return stats

        self.log(f"Attempting to save {len(jobs)} jobs...")

        for job in jobs:
            try:
                job_data = job.to_dict()

                # Insert job
                response = self.supabase.table("jobs").insert(job_data).execute()
                stats["saved"] += 1

                # If job has email, save it to company_emails table
                if job.company_email and EmailValidator.is_valid(job.company_email):
                    self._save_company_email(
                        company_name=job.company_name,
                        email=job.company_email,
                        source_job_id=job.source_job_id,
                    )
                    stats["emails"] += 1

            except Exception as e:
                if "duplicate" in str(e).lower() or "unique" in str(e).lower():
                    stats["duplicate"] += 1
                else:
                    stats["errors"] += 1
                    self.log(f"Error saving job: {str(e)}")

        self.log(f"Save complete - Saved: {stats['saved']}, Emails: {stats['emails']}, Duplicates: {stats['duplicate']}")
        return stats

    def _save_company_email(self, company_name: str, email: str, source_job_id: str):
        """Save company email to email tracking table"""
        try:
            email_data = {
                "company_name": company_name,
                "company_email": email,
                "source_job_id": source_job_id,
                "source_found_on": "Indeed",
                "email_valid": EmailValidator.is_valid(email),
            }

            # Try to insert (will skip if email already exists)
            self.supabase.table("company_emails").insert(email_data).execute()

        except Exception as e:
            if "duplicate" not in str(e).lower():
                self.log(f"Error saving email: {str(e)}")

    def log_scrape_run(
        self,
        source: str,
        status: str,
        jobs_found: int,
        jobs_saved: int,
        jobs_skipped: int,
        emails_found: int = 0,
        error_message: str = None,
    ):
        """Log scraping run with email statistics"""

        run_data = {
            "source": source,
            "status": status,
            "jobs_found": jobs_found,
            "jobs_saved": jobs_saved,
            "jobs_skipped_duplicates": jobs_skipped,
            "emails_found": emails_found,
            "error_message": error_message,
        }

        try:
            self.supabase.table("scrape_runs").insert(run_data).execute()
            self.log(f"Logged scrape run for {source} (emails: {emails_found})")
        except Exception as e:
            self.log(f"Error logging scrape run: {str(e)}")

    def get_company_emails(self, limit: int = 100) -> List[Dict]:
        """Get all company emails for outreach"""
        try:
            response = (
                self.supabase.table("company_emails")
                .select("*")
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data
        except Exception as e:
            self.log(f"Error fetching emails: {str(e)}")
            return []

    def get_unverified_emails(self, limit: int = 50) -> List[Dict]:
        """Get emails that haven't been verified yet"""
        try:
            response = (
                self.supabase.table("company_emails")
                .select("*")
                .eq("email_verified", False)
                .order("created_at", desc=True)
                .limit(limit)
                .execute()
            )
            return response.data
        except Exception as e:
            self.log(f"Error fetching unverified emails: {str(e)}")
            return []

    def mark_email_contacted(self, email: str):
        """Mark email as contacted"""
        try:
            self.supabase.table("company_emails").update({
                "last_contacted": "now()",
                "contact_attempts": "contact_attempts + 1",
            }).eq("company_email", email).execute()
        except Exception as e:
            self.log(f"Error updating email: {str(e)}")

    def get_jobs_count(self) -> int:
        """Get total number of jobs"""
        try:
            response = self.supabase.table("jobs").select("count(*)").execute()
            return response.count
        except Exception as e:
            self.log(f"Error getting job count: {str(e)}")
            return 0

    def get_emails_count(self) -> int:
        """Get total number of unique company emails"""
        try:
            response = self.supabase.table("company_emails").select("count(*)").execute()
            return response.count
        except Exception as e:
            self.log(f"Error getting email count: {str(e)}")
            return 0