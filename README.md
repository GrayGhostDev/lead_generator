# B2B Lead Generation Tool

This project provides a comprehensive B2B lead generation pipeline, including contact enrichment, company data scraping, qualification, and CSV management.

## Features
- Enrich contacts and companies using the ZoomInfo API
- Scrape company websites for contact information using Selenium
- Qualify leads based on customizable criteria
- Predict missing email addresses
- Import/export and merge CSV files
- Command-line interface for flexible operation

## Setup

### 1. Clone the repository
```bash
git clone <your-repo-url>
cd <repo-directory>
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Set up environment variables
Copy `.env.example` to `.env` and fill in your ZoomInfo API credentials:
```bash
cp .env.example .env
```
Edit `.env` and set:
- `ZOOMINFO_API_KEY` (or `ZOOMINFO_USERNAME` and `ZOOMINFO_PASSWORD`)

### 4. Download ChromeDriver
Download the appropriate ChromeDriver for your Chrome version and ensure it is in your PATH or specify its location with the `--chrome-driver-path` argument.

## Usage

### Basic lead generation from contacts CSV
```bash
python lead_gen.py --input contacts.csv --output output_dir
```

### Create a sample contacts CSV
```bash
python lead_gen.py --create-sample
```

### Import and qualify ZoomInfo export
```bash
python lead_gen.py --import-zoominfo zoominfo_export.csv --predict-emails
```

### Merge multiple CSV files
```bash
python lead_gen.py --merge-csv file1.csv file2.csv --merge-output merged.csv
```

### Additional options
- `--skip-scraping` : Skip website scraping
- `--skip-enrichment` : Skip ZoomInfo enrichment
- `--batch-size N` : Set batch size for API calls
- `--title-keywords CEO CTO` : Set job title keywords for qualification
- `--target-industries Software Technology` : Set target industries

## Output
- Processed leads and qualified leads are saved in the specified output directory as CSV and JSON files.
- Logs are written to `lead_gen.log`.

## Notes
- Selenium requires Chrome and ChromeDriver installed.
- ZoomInfo API access is required for enrichment features.

## License
MIT 