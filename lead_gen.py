import os
import time
import json
import pandas as pd
import logging
import requests
import csv
from datetime import datetime
from typing import Dict, List, Optional, Union, Tuple
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv
from csv_data_manager import CSVDataManager
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("lead_gen.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class LeadScraper:
    """Class for scraping lead data from websites using Selenium"""
    def __init__(self, headless: bool = True, chrome_driver_path: Optional[str] = None):
        self.chrome_options = Options()
        if headless:
            self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-dev-shm-usage")
        if chrome_driver_path:
            self.service = Service(executable_path=chrome_driver_path)
            self.driver = webdriver.Chrome(service=self.service, options=self.chrome_options)
        else:
            self.driver = webdriver.Chrome(options=self.chrome_options)
        self.driver.maximize_window()
        logger.info("WebDriver initialized successfully")

    def __del__(self):
        if hasattr(self, 'driver'):
            self.driver.quit()

    def navigate_to_url(self, url: str, wait_time: int = 10) -> bool:
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            logger.info(f"Successfully navigated to {url}")
            return True
        except TimeoutException:
            logger.error(f"Timeout while loading {url}")
            return False
        except Exception as e:
            logger.error(f"Error navigating to {url}: {str(e)}")
            return False

    def scrape_linkedin_company(self, company_url: str) -> Dict:
        company_data = {
            "company_name": "",
            "industry": "",
            "company_size": "",
            "company_location": "",
            "website": "",
            "about": "",
            "scraped_at": datetime.now().isoformat()
        }
        if not self.navigate_to_url(company_url):
            return company_data
        try:
            name_element = self.driver.find_element(By.CSS_SELECTOR, ".org-top-card-summary__title")
            company_data["company_name"] = name_element.text.strip()
            try:
                industry_element = self.driver.find_element(By.CSS_SELECTOR, ".org-top-card-summary-info-list__info-item:nth-child(1)")
                company_data["industry"] = industry_element.text.strip()
            except NoSuchElementException:
                pass
            try:
                size_element = self.driver.find_element(By.CSS_SELECTOR, ".org-about-module__company-size-definition-text")
                company_data["company_size"] = size_element.text.strip()
            except NoSuchElementException:
                pass
            try:
                location_element = self.driver.find_element(By.CSS_SELECTOR, ".org-top-card-summary-info-list__info-item:nth-child(2)")
                company_data["company_location"] = location_element.text.strip()
            except NoSuchElementException:
                pass
            try:
                website_element = self.driver.find_element(By.CSS_SELECTOR, ".org-about-module__website a")
                company_data["website"] = website_element.get_attribute("href")
            except NoSuchElementException:
                pass
            try:
                about_element = self.driver.find_element(By.CSS_SELECTOR, ".org-about-module__description")
                company_data["about"] = about_element.text.strip()
            except NoSuchElementException:
                pass
        except Exception as e:
            logger.error(f"Error scraping LinkedIn company data: {str(e)}")
        return company_data

    def scrape_company_website(self, website_url: str) -> Dict:
        contact_data = {
            "email_addresses": [],
            "phone_numbers": [],
            "contact_page_url": "",
            "scraped_at": datetime.now().isoformat()
        }
        if not self.navigate_to_url(website_url):
            return contact_data
        try:
            contact_links = self.driver.find_elements(By.XPATH, 
                "//a[contains(translate(text(), 'CONTACT', 'contact'), 'contact') or contains(@href, 'contact')]")
            if contact_links:
                contact_url = contact_links[0].get_attribute("href")
                contact_data["contact_page_url"] = contact_url
                self.navigate_to_url(contact_url)
                page_source = self.driver.page_source
                import re
                emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}', page_source)
                contact_data["email_addresses"] = list(set(emails))
                phones = re.findall(r'\\+\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}', page_source)
                contact_data["phone_numbers"] = list(set(phones))
        except Exception as e:
            logger.error(f"Error scraping company website: {str(e)}")
        return contact_data

class ZoomInfoEnricher:
    """Class for enriching lead data using ZoomInfo API"""
    def __init__(self, api_key: str = None, api_username: str = None, api_password: str = None):
        self.api_key = api_key or os.environ.get("ZOOMINFO_API_KEY")
        self.api_username = api_username or os.environ.get("ZOOMINFO_USERNAME")
        self.api_password = api_password or os.environ.get("ZOOMINFO_PASSWORD")
        if not (self.api_key or (self.api_username and self.api_password)):
            raise ValueError("Either ZoomInfo API key or username/password credentials are required")
        self.base_url = "https://api.zoominfo.com/v1"
        self.auth_token = None
        self.token_expiry = None
        logger.info("ZoomInfo enricher initialized")

    def _authenticate(self) -> bool:
        if self.auth_token and self.token_expiry and datetime.now() < self.token_expiry:
            return True
        try:
            if self.api_key:
                self.headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                self.token_expiry = datetime.now() + pd.Timedelta(days=1)
                logger.info("Using API key authentication for ZoomInfo")
                return True
            else:
                auth_endpoint = f"{self.base_url}/auth"
                auth_payload = {
                    "username": self.api_username,
                    "password": self.api_password
                }
                response = requests.post(
                    auth_endpoint,
                    json=auth_payload,
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    auth_data = response.json()
                    self.auth_token = auth_data.get("token")
                    expires_in = auth_data.get("expiresIn", 86400)
                    self.token_expiry = datetime.now() + pd.Timedelta(seconds=expires_in)
                    self.headers = {
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.auth_token}"
                    }
                    logger.info("Successfully authenticated with ZoomInfo API")
                    return True
                else:
                    logger.error(f"ZoomInfo authentication error: {response.status_code} - {response.text}")
                    return False
        except Exception as e:
            logger.error(f"Error authenticating with ZoomInfo: {str(e)}")
            return False

    def enrich_contact_batch(self, contacts: List[Dict]) -> List[Dict]:
        """
        Enrich multiple contacts in a batch request.
        For each contact, match the returned payload data to the requested names from the CSV (case-insensitive, stripped).
        If name-based matching is ambiguous or fails, fallback to email-based matching (case-insensitive, stripped).
        """
        if not self._authenticate():
            logger.error("Failed to authenticate with ZoomInfo API")
            return contacts

        endpoint = f"{self.base_url}/person/bulk"
        batch_payload = {"persons": []}
        for contact in contacts:
            person_payload = {}
            if "first_name" in contact:
                person_payload["firstName"] = contact["first_name"]
            if "last_name" in contact:
                person_payload["lastName"] = contact["last_name"]
            if "email" in contact:
                person_payload["email"] = contact["email"]
            if "title" in contact:
                person_payload["jobTitle"] = contact["title"]
            if "company_name" in contact:
                person_payload["companyName"] = contact["company_name"]
            if "company_website" in contact:
                from urllib.parse import urlparse
                try:
                    parsed_url = urlparse(contact["company_website"])
                    domain = parsed_url.netloc
                    if domain.startswith("www."):
                        domain = domain[4:]
                    person_payload["companyDomain"] = domain
                except:
                    person_payload["companyDomain"] = contact["company_website"]
            if person_payload:
                batch_payload["persons"].append(person_payload)
        if not batch_payload["persons"]:
            logger.warning("No valid contacts to enrich")
            return contacts
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                json=batch_payload
            )
            if response.status_code == 200:
                enriched_data = response.json()
                logger.info(f"Successfully enriched {len(enriched_data.get('results', []))} contacts")
                enriched_contacts = []
                results = enriched_data.get("results", [])
                # Build lookups for name and email
                results_by_name = {}
                results_by_email = {}
                for r in results:
                    first = (r.get('firstName') or '').strip().lower()
                    last = (r.get('lastName') or '').strip().lower()
                    email = (r.get('email') or '').strip().lower()
                    # Name-based lookup (may be ambiguous)
                    name_key = (first, last)
                    if name_key not in results_by_name:
                        results_by_name[name_key] = []
                    results_by_name[name_key].append(r)
                    # Email-based lookup
                    if email:
                        results_by_email[email] = r
                for contact in contacts:
                    # Try name-based matching first
                    first = (contact.get('first_name') or '').strip().lower()
                    last = (contact.get('last_name') or '').strip().lower()
                    name_key = (first, last)
                    email = (contact.get('email') or '').strip().lower()
                    enriched = None
                    # If only one match for this name, use it
                    if name_key in results_by_name and len(results_by_name[name_key]) == 1:
                        enriched = results_by_name[name_key][0]
                    # If multiple matches for this name, try to match by email
                    elif name_key in results_by_name and len(results_by_name[name_key]) > 1 and email:
                        for r in results_by_name[name_key]:
                            r_email = (r.get('email') or '').strip().lower()
                            if r_email == email:
                                enriched = r
                                break
                    # If no name match, try email-based matching
                    if not enriched and email and email in results_by_email:
                        enriched = results_by_email[email]
                    if enriched:
                        # Merge and standardize data if found
                        company_info = {}
                        for key in ["companyName", "companyDomain", "companyId", "industry", "companyRevenue", "companyEmployees", "companyLocation"]:
                            if key in enriched:
                                snake_key = ''.join(['_'+c.lower() if c.isupper() else c for c in key]).lstrip('_')
                                company_info[snake_key.replace("company_", "")] = enriched.pop(key)
                        if company_info:
                            enriched["company"] = company_info
                        standardized = {}
                        for key, value in enriched.items():
                            if key not in ["company"]:
                                snake_key = ''.join(['_'+c.lower() if c.isupper() else c for c in key]).lstrip('_')
                                standardized[snake_key] = value
                            else:
                                standardized[key] = value
                        merged = {**contact, **standardized}
                        merged["zi_enriched"] = True
                        enriched_contacts.append(merged)
                    else:
                        # If no match found, keep original data
                        contact["zi_enriched"] = False
                        enriched_contacts.append(contact)
                return enriched_contacts
            else:
                logger.error(f"ZoomInfo API error: {response.status_code} - {response.text}")
                for contact in contacts:
                    contact["zi_enriched"] = False
                return contacts
        except Exception as e:
            logger.error(f"Error enriching contacts: {str(e)}")
            for contact in contacts:
                contact["zi_enriched"] = False
            return contacts

    def get_company_for_contacts(self, contacts: List[Dict]) -> List[Dict]:
        if not self._authenticate():
            logger.error("Failed to authenticate with ZoomInfo API")
            return contacts
        endpoint = f"{self.base_url}/company/bulk"
        companies_to_lookup = []
        contact_to_company_index = {}
        for i, contact in enumerate(contacts):
            company_name = contact.get("company_name")
            email_domain = None
            if "email" in contact and "@" in contact["email"]:
                email_domain = contact["email"].split("@")[1]
            website_domain = None
            if "company_website" in contact:
                from urllib.parse import urlparse
                try:
                    parsed_url = urlparse(contact["company_website"])
                    website_domain = parsed_url.netloc
                    if website_domain.startswith("www."):
                        website_domain = website_domain[4:]
                except:
                    pass
            company_identifier = None
            if website_domain:
                company_identifier = {"domain": website_domain}
            elif email_domain:
                company_identifier = {"domain": email_domain}
            elif company_name:
                company_identifier = {"companyName": company_name}
            if company_identifier:
                is_duplicate = False
                for existing in companies_to_lookup:
                    if existing.get("domain") and company_identifier.get("domain") and \
                       existing["domain"] == company_identifier["domain"]:
                        is_duplicate = True
                        break
                    elif existing.get("companyName") and company_identifier.get("companyName") and \
                         existing["companyName"].lower() == company_identifier["companyName"].lower():
                        is_duplicate = True
                        break
                if not is_duplicate:
                    companies_to_lookup.append(company_identifier)
                contact_to_company_index[i] = company_identifier
        if not companies_to_lookup:
            logger.warning("No valid companies to lookup")
            return contacts
        max_batch_size = 100
        company_batches = [companies_to_lookup[i:i + max_batch_size] 
                          for i in range(0, len(companies_to_lookup), max_batch_size)]
        all_company_results = []
        for batch in company_batches:
            batch_payload = {"companies": batch}
            try:
                response = requests.post(
                    endpoint,
                    headers=self.headers,
                    json=batch_payload
                )
                if response.status_code == 200:
                    batch_results = response.json().get("results", [])
                    all_company_results.extend(batch_results)
                else:
                    logger.error(f"ZoomInfo API error: {response.status_code} - {response.text}")
            except Exception as e:
                logger.error(f"Error looking up companies: {str(e)}")
        companies_by_domain = {c.get("domain", "").lower(): c for c in all_company_results if c.get("domain")}
        companies_by_name = {c.get("companyName", "").lower(): c for c in all_company_results if c.get("companyName")}
        for i, contact in enumerate(contacts):
            if i in contact_to_company_index:
                company_identifier = contact_to_company_index[i]
                company_data = None
                if "domain" in company_identifier:
                    domain = company_identifier["domain"].lower()
                    if domain in companies_by_domain:
                        company_data = companies_by_domain[domain]
                elif "companyName" in company_identifier:
                    name = company_identifier["companyName"].lower()
                    if name in companies_by_name:
                        company_data = companies_by_name[name]
                if company_data:
                    company_info = {}
                    for key, value in company_data.items():
                        snake_key = ''.join(['_'+c.lower() if c.isupper() else c for c in key]).lstrip('_')
                        company_info[snake_key] = value
                    contact["company"] = company_info
                    contact["company_enriched"] = True
                    if "company_name" not in contact and "company_name" in company_info:
                        contact["company_name"] = company_info["company_name"]
                    if "company_website" not in contact and "domain" in company_info:
                        contact["company_website"] = f"https://{company_info['domain']}"
                    if "company_industry" not in contact and "industry" in company_info:
                        contact["company_industry"] = company_info["industry"]
                    if "company_size" not in contact and "employees" in company_info:
                        contact["company_size"] = company_info["employees"]
                    if "company_location" not in contact and "location" in company_info:
                        contact["company_location"] = company_info["location"]
                else:
                    contact["company_enriched"] = False
            else:
                contact["company_enriched"] = False
        return contacts

class ContactQualifier:
    """Class for qualifying contacts based on criteria"""
    def __init__(self, 
                 title_keywords: List[str] = None,
                 department_keywords: List[str] = None, 
                 min_company_size: int = 50, 
                 max_company_size: int = 1000,
                 target_industries: List[str] = None,
                 target_locations: List[str] = None):
        self.title_keywords = [k.lower() for k in (title_keywords or [])]
        self.department_keywords = [k.lower() for k in (department_keywords or [])]
        self.min_company_size = min_company_size
        self.max_company_size = max_company_size
        self.target_industries = [i.lower() for i in (target_industries or [])]
        self.target_locations = [l.lower() for l in (target_locations or [])]
        logger.info("Contact qualifier initialized")

    def qualify_contact(self, contact_data: Dict) -> Dict:
        score = 0
        max_score = 0
        reasons = []
        if self.title_keywords and "title" in contact_data:
            max_score += 1
            title = contact_data["title"].lower()
            for keyword in self.title_keywords:
                if keyword in title:
                    score += 1
                    reasons.append(f"Title '{title}' contains keyword '{keyword}'")
                    break
            else:
                reasons.append(f"Title '{title}' does not match any target keywords")
        if self.department_keywords and "department" in contact_data:
            max_score += 1
            department = contact_data["department"].lower()
            for keyword in self.department_keywords:
                if keyword in department:
                    score += 1
                    reasons.append(f"Department '{department}' contains keyword '{keyword}'")
                    break
            else:
                reasons.append(f"Department '{department}' does not match any target keywords")
        company_size = None
        company_size_str = contact_data.get("company_size")
        if not company_size_str and "company" in contact_data and isinstance(contact_data["company"], dict):
            company_size_str = contact_data["company"].get("employees")
        if company_size_str:
            max_score += 1
            try:
                if "-" in company_size_str:
                    parts = company_size_str.split("-")
                    company_size = int(parts[0].replace(",", "").replace("+", "").strip())
                elif "+" in company_size_str:
                    company_size = int(company_size_str.replace(",", "").replace("+", "").strip())
                else:
                    company_size = int(company_size_str.replace(",", "").strip())
                if self.min_company_size <= company_size <= self.max_company_size:
                    score += 1
                    reasons.append(f"Company size ({company_size}) is within target range")
                else:
                    reasons.append(f"Company size ({company_size}) is outside target range")
            except (ValueError, AttributeError):
                reasons.append(f"Could not determine company size from '{company_size_str}'")
        industry = None
        industry_str = contact_data.get("company_industry")
        if not industry_str and "company" in contact_data and isinstance(contact_data["company"], dict):
            industry_str = contact_data["company"].get("industry")
        if industry_str and self.target_industries:
            max_score += 1
            industry = industry_str.lower()
            for target in self.target_industries:
                if target in industry:
                    score += 1
                    reasons.append(f"Industry '{industry}' matches target '{target}'")
                    break
            else:
                reasons.append(f"Industry '{industry}' does not match any target industries")
        location = None
        location_str = contact_data.get("company_location")
        if not location_str and "company" in contact_data and isinstance(contact_data["company"], dict):
            location_str = contact_data["company"].get("location")
        if not location_str:
            location_str = contact_data.get("location")
        if location_str and self.target_locations:
            max_score += 1
            location = location_str.lower()
            for target in self.target_locations:
                if target in location:
                    score += 1
                    reasons.append(f"Location '{location}' matches target '{target}'")
                    break
            else:
                reasons.append(f"Location '{location}' does not match any target locations")
        if "email" in contact_data and contact_data["email"]:
            max_score += 1
            if "@" in contact_data["email"]:
                score += 1
                reasons.append("Contact has a valid email address")
            else:
                reasons.append("Contact's email address appears invalid")
        percentage_score = (score / max_score * 100) if max_score > 0 else 0
        result = contact_data.copy()
        result["qualification"] = {
            "score": score,
            "max_score": max_score,
            "percentage_score": percentage_score,
            "reasons": reasons,
            "qualified": percentage_score >= 60
        }
        return result

class LeadGenerator:
    """Class for enriching people data using ZoomInfo API and exporting Name, Email, Contact Phone, with error reporting."""
    def __init__(self, enricher: Optional[ZoomInfoEnricher] = None, output_dir: str = "output"):
        self.enricher = enricher
        self.output_dir = output_dir
        self.csv_manager = CSVDataManager()
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        self.leads_df = pd.DataFrame()
        self.errors = []
        logger.info("Lead generator initialized for people enrichment.")

    def process_contact_list(self, contact_list: List[Dict], batch_size: int = 10) -> pd.DataFrame:
        """
        Enrich a list of contacts using ZoomInfo and return Name, Email, Contact Phone. Collect errors.
        Only contact-level fields are used for output; company fields are never used for Name, Email, or Contact Phone.
        """
        processed_contacts = []
        total_contacts = len(contact_list)
        for i in range(0, total_contacts, batch_size):
            batch = contact_list[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}, contacts {i+1}-{min(i+batch_size, total_contacts)}")
            try:
                if self.enricher:
                    logger.info(f"Enriching batch {i//batch_size + 1} with ZoomInfo")
                    enriched_batch = self.enricher.enrich_contact_batch(batch)
                else:
                    enriched_batch = batch
            except Exception as e:
                logger.error(f"Batch {i//batch_size + 1} failed: {str(e)}")
                for contact in batch:
                    self.errors.append({
                        'contact': contact,
                        'error': f'Batch enrichment error: {str(e)}'
                    })
                continue
            for orig, enriched in zip(batch, enriched_batch):
                try:
                    # Only extract contact-level fields for output, never from company fields
                    first = enriched.get('first_name', '') or enriched.get('First Name', '')
                    last = enriched.get('last_name', '') or enriched.get('Last Name', '')
                    name = f"{first} {last}".strip()
                    email = enriched.get('email', '') or enriched.get('Email', '')
                    phone = enriched.get('phone', '') or enriched.get('DirectPhone', '') or enriched.get('Contact Phone', '')
                    # Do NOT use any fields from enriched.get('company', {}) for output
                    # If all required fields are missing, log error
                    if not (name or email or phone):
                        raise ValueError("No name, email, or phone found after enrichment.")
                    processed_contacts.append({
                        'Name': name,
                        'Email': email,
                        'Contact Phone': phone
                    })
                except Exception as e:
                    logger.error(f"Contact enrichment failed: {str(e)} | Contact: {orig}")
                    self.errors.append({
                        'contact': orig,
                        'error': str(e)
                    })
            if i + batch_size < total_contacts:
                time.sleep(2)
        self.leads_df = pd.DataFrame(processed_contacts)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.output_dir, f"enriched_contacts_{timestamp}.csv")
        self.csv_manager.write_csv(self.leads_df, csv_path)
        logger.info(f"Saved {len(self.leads_df)} enriched contacts to {csv_path}")
        # Save errors if any
        error_csv_path = None
        if self.errors:
            error_csv_path = os.path.join(self.output_dir, f"enrichment_errors_{timestamp}.csv")
            error_rows = []
            for err in self.errors:
                row = err['contact'].copy()
                row['error'] = err['error']
                error_rows.append(row)
            error_df = pd.DataFrame(error_rows)
            self.csv_manager.write_csv(error_df, error_csv_path)
            logger.info(f"Saved {len(self.errors)} enrichment errors to {error_csv_path}")
        return self.leads_df, error_csv_path

    def process_csv_file(self, input_file: str, batch_size: int = 10):
        contacts = self.csv_manager.read_csv(input_file)
        if contacts.empty:
            logger.error(f"No valid contacts found in {input_file}")
            return pd.DataFrame(), None
        contact_list = contacts.to_dict('records')
        return self.process_contact_list(contact_list, batch_size=batch_size)

# Utility function remains for compatibility, but not used in this workflow

def save_leads_batch_to_csv(leads: List[Dict], output_path: str) -> None:
    try:
        simplified_leads = []
        for lead in leads:
            simplified_lead = {}
            for key, value in lead.items():
                if isinstance(value, (dict, list)):
                    simplified_lead[key] = json.dumps(value)
                else:
                    simplified_lead[key] = value
            simplified_leads.append(simplified_lead)
        df = pd.DataFrame(simplified_leads)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(simplified_leads)} leads to {output_path}")
    except Exception as e:
        logger.error(f"Error saving leads to CSV: {str(e)}")

# Main CLI

def main():
    import argparse
    import glob
    import concurrent.futures
    import pandas as pd
    from tabulate import tabulate
    from tqdm import tqdm
    import time

    parser = argparse.ArgumentParser(description='People Data Enrichment Tool (ZoomInfo)')
    parser.add_argument('--input', '-i', type=str, help='Path to input CSV file with people data')
    parser.add_argument('--input-dir', type=str, default=None, help='Directory containing CSV files to enrich (default: CSV_Data)')
    parser.add_argument('--output', '-o', type=str, default='output', help='Directory for output files (default: output)')
    parser.add_argument('--batch-size', type=int, default=10, help='Number of contacts to process in each batch (default: 10)')
    parser.add_argument('--max-workers', type=int, default=4, help='Number of parallel files to process (default: 4)')
    parser.add_argument('--retries', type=int, default=2, help='Number of retries on failure (default: 2)')
    args = parser.parse_args()

    if not os.path.exists(args.output):
        os.makedirs(args.output)

    enricher = ZoomInfoEnricher()  # API key from environment variable

    # Determine files to process
    files_to_process = []
    if args.input_dir:
        input_dir = args.input_dir
    elif not args.input and not args.input_dir:
        input_dir = 'CSV_Data'
    else:
        input_dir = None

    if input_dir:
        files_to_process = sorted(glob.glob(os.path.join(input_dir, '*.csv')))
        if not files_to_process:
            print(f"No CSV files found in directory: {input_dir}")
            return
    elif args.input:
        files_to_process = [args.input]
    else:
        print("Error: Please provide --input or --input-dir (or place files in CSV_Data).")
        return

    # Automation: skip files already processed
    def output_file_path(input_path):
        base = os.path.splitext(os.path.basename(input_path))[0]
        return os.path.join(args.output, f"enriched_contacts_{base}.csv")

    def error_file_path(input_path):
        base = os.path.splitext(os.path.basename(input_path))[0]
        return os.path.join(args.output, f"enrichment_errors_{base}.csv")

    files_to_run = []
    for f in files_to_process:
        out_path = output_file_path(f)
        if os.path.exists(out_path):
            print(f"Skipping already processed file: {f}")
            continue
        files_to_run.append(f)

    if not files_to_run:
        print("No new files to process.")
        return

    summary_rows = []

    def process_file(file_path):
        lead_gen = LeadGenerator(enricher=enricher, output_dir=args.output)
        # Read contacts for progress bar
        contacts_df = lead_gen.csv_manager.read_csv(file_path)
        total_contacts = len(contacts_df) if not contacts_df.empty else 0
        num_success = 0
        num_errors = 0
        out_path = output_file_path(file_path)
        err_path = None
        enriched_df = None
        error_csv_path = None
        for attempt in range(args.retries + 1):
            try:
                # Batch progress bar
                with tqdm(total=total_contacts, desc=f"{os.path.basename(file_path)}", unit="contact", leave=False) as pbar:
                    processed_contacts = []
                    errors = []
                    contact_list = contacts_df.to_dict('records')
                    for i in range(0, total_contacts, args.batch_size):
                        batch = contact_list[i:i + args.batch_size]
                        batch_success = False
                        for batch_attempt in range(args.retries + 1):
                            try:
                                if lead_gen.enricher:
                                    enriched_batch = lead_gen.enricher.enrich_contact_batch(batch)
                                else:
                                    enriched_batch = batch
                                for orig, enriched in zip(batch, enriched_batch):
                                    first = enriched.get('first_name', '') or enriched.get('First Name', '')
                                    last = enriched.get('last_name', '') or enriched.get('Last Name', '')
                                    name = f"{first} {last}".strip()
                                    email = enriched.get('email', '') or enriched.get('Email', '')
                                    phone = enriched.get('phone', '') or enriched.get('DirectPhone', '') or enriched.get('Contact Phone', '')
                                    if not (name or email or phone):
                                        raise ValueError("No name, email, or phone found after enrichment.")
                                    processed_contacts.append({
                                        'Name': name,
                                        'Email': email,
                                        'Contact Phone': phone
                                    })
                                batch_success = True
                                break
                            except Exception as e:
                                if batch_attempt < args.retries:
                                    time.sleep(2)
                                    continue
                                else:
                                    for contact in batch:
                                        errors.append({
                                            'contact': contact,
                                            'error': f'Batch enrichment error: {str(e)}'
                                        })
                        pbar.update(len(batch))
                        time.sleep(0.1)
                    enriched_df = pd.DataFrame(processed_contacts)
                    num_success = len(enriched_df)
                    if errors:
                        error_rows = []
                        for err in errors:
                            row = err['contact'].copy()
                            row['error'] = err['error']
                            error_rows.append(row)
                        error_df = pd.DataFrame(error_rows)
                        error_csv_path = error_file_path(file_path)
                        error_df.to_csv(error_csv_path, index=False)
                        num_errors = len(error_df)
                    break  # Success, break retry loop
            except Exception as e:
                if attempt < args.retries:
                    print(f"Retrying {file_path} (attempt {attempt+2}/{args.retries+1}) due to error: {e}")
                    time.sleep(2)
                    continue
                else:
                    print(f"Failed to process {file_path} after {args.retries+1} attempts. Error: {e}")
                    num_success = 0
                    num_errors = total_contacts
                    error_csv_path = error_file_path(file_path)
                    error_df = pd.DataFrame([{**row, 'error': str(e)} for row in contacts_df.to_dict('records')])
                    error_df.to_csv(error_csv_path, index=False)
            break
        if enriched_df is not None and not enriched_df.empty:
            enriched_df.to_csv(out_path, index=False)
        return {
            'File': os.path.basename(file_path),
            'Processed': num_success,
            'Errors': num_errors,
            'Output File': out_path,
            'Error File': error_csv_path if num_errors else ''
        }

    # File-level progress bar
    with tqdm(total=len(files_to_run), desc="Files", unit="file") as file_pbar:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
            future_to_file = {executor.submit(process_file, f): f for f in files_to_run}
            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                summary_rows.append(result)
                file_pbar.update(1)

    # Reporting
    if summary_rows:
        summary_rows = sorted(summary_rows, key=lambda x: x['File'])
        print("\nSummary:")
        print(tabulate(summary_rows, headers="keys", tablefmt="github"))
        summary_csv = os.path.join(args.output, 'enrichment_summary.csv')
        pd.DataFrame(summary_rows).to_csv(summary_csv, index=False)
        print(f"\nMaster summary saved to: {summary_csv}")
    else:
        print("No files processed.")

if __name__ == "__main__":
    main()