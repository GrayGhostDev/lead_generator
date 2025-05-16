import pandas as pd
import csv
import os
import json
from typing import List, Dict

class CSVDataManager:
    """Utility class for reading, writing, and managing CSV data"""

    def read_csv(self, file_path: str) -> pd.DataFrame:
        """
        Read a CSV file into a pandas DataFrame.
        Returns an empty DataFrame if file does not exist or is invalid.
        """
        try:
            return pd.read_csv(file_path)
        except Exception:
            return pd.DataFrame()

    def write_csv(self, df: pd.DataFrame, file_path: str) -> None:
        """
        Write a pandas DataFrame to a CSV file.
        """
        try:
            df.to_csv(file_path, index=False)
        except Exception as e:
            print(f"Error writing CSV: {e}")

    def flatten_nested_data(self, data: List[Dict]) -> List[Dict]:
        """
        Flatten nested dictionaries/lists in a list of dicts for CSV compatibility.
        """
        flat_data = []
        for item in data:
            flat_item = {}
            for k, v in item.items():
                if isinstance(v, dict) or isinstance(v, list):
                    flat_item[k] = json.dumps(v)
                else:
                    flat_item[k] = v
            flat_data.append(flat_item)
        return flat_data

    def create_sample_contacts_csv(self, file_path: str = "sample_contacts.csv") -> str:
        """
        Create a sample contacts CSV file for demonstration.
        """
        sample_data = [
            {
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice.smith@example.com",
                "phone": "+1-555-123-4567",
                "title": "CTO",
                "company_name": "Acme Corp",
                "company_website": "https://acme.com",
                "company_industry": "Software",
                "company_size": "200",
                "company_location": "New York, USA",
                "linkedin_url": "https://linkedin.com/in/alicesmith",
                "notes": "Sample contact"
            },
            {
                "first_name": "Bob",
                "last_name": "Johnson",
                "email": "bob.johnson@example.com",
                "phone": "+1-555-987-6543",
                "title": "VP Engineering",
                "company_name": "Beta Inc",
                "company_website": "https://beta.com",
                "company_industry": "Technology",
                "company_size": "500",
                "company_location": "San Francisco, USA",
                "linkedin_url": "https://linkedin.com/in/bobjohnson",
                "notes": "Sample contact"
            }
        ]
        df = pd.DataFrame(sample_data)
        df.to_csv(file_path, index=False)
        return file_path 