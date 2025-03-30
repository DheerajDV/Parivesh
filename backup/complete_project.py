import json
import sqlite3
import logging
import os
import time
import requests
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def setup_database(db_path):
    """Set up the database with the correct schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create proposals table (Level 1) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT UNIQUE,
        sw_no TEXT,
        project_name TEXT,
        company_name TEXT,
        state TEXT,
        category TEXT,
        sector TEXT,
        current_status TEXT,
        proposal_type TEXT,
        clearance_type TEXT,
        submission_date TEXT,
        last_updated TEXT,
        year INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create proposal_details table (Level 2a) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT UNIQUE,
        raw_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_timelines table (Level 2a) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_timelines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        status TEXT,
        date TEXT,
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create project_locations table (Level 2b) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        location_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_forms table (Level 2b) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_forms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        form_type TEXT,
        form_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create documents table (Level 2b) if it doesn't exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        document_type TEXT,
        document_name TEXT,
        document_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    conn.commit()
    conn.close()
    logger.info(f"Database {db_path} setup complete")

def get_proposal_timelines(proposal_id, session, headers):
    """Get timeline information for a proposal."""
    url = "https://parivesh.nic.in/parivesh_api/trackYourProposal/getApprovalDates"
    params = {"proposalNo": proposal_id}
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'data' in data:
                return data['data'] if isinstance(data['data'], list) else []
            else:
                logger.warning(f"Unexpected timeline response format for proposal {proposal_id}")
                return []
        else:
            logger.warning(f"Failed to get timelines for proposal {proposal_id}: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error getting timelines for proposal {proposal_id}: {str(e)}")
        return []

def get_project_location(proposal_id, session, headers):
    """Get the KML file for a project's location."""
    # First, get the form ID
    url = "https://parivesh.nic.in/parivesh_api/trackYourProposal/dataOfProposalNo"
    params = {"proposalNo": proposal_id}
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            # Extract form ID
            form_id = None
            if isinstance(data, dict):
                form_id = data.get('formId')
            
            if not form_id:
                logger.warning(f"Form ID not found for proposal {proposal_id}")
                return None
            
            # Get KML file
            kml_url = "https://parivesh.nic.in/parivesh_api/trackYourProposal/getKmlFile"
            kml_params = {"formId": form_id}
            
            kml_response = session.get(kml_url, params=kml_params, headers=headers, timeout=30)
            if kml_response.status_code == 200:
                try:
                    return kml_response.json()
                except:
                    # If not JSON, return the text
                    return kml_response.text
            else:
                logger.warning(f"Failed to get KML file for proposal {proposal_id}: {kml_response.status_code}")
                return None
        else:
            logger.warning(f"Failed to get form ID for proposal {proposal_id}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Error getting project location for proposal {proposal_id}: {str(e)}")
        return None

def get_proposal_forms(proposal_id, session, headers):
    """Get various forms for a proposal."""
    forms = {}
    base_url = "https://parivesh.nic.in/parivesh_api/trackYourProposal"
    params = {"proposalNo": proposal_id}
    
    # Get CAF form
    try:
        caf_url = f"{base_url}/getCaFormDetails"
        caf_response = session.get(caf_url, params=params, headers=headers, timeout=30)
        if caf_response.status_code == 200:
            forms['caf'] = caf_response.json()
    except Exception as e:
        logger.error(f"Error getting CAF form for proposal {proposal_id}: {str(e)}")
    
    # Get Part A form
    try:
        part_a_url = f"{base_url}/getPartADetails"
        part_a_response = session.get(part_a_url, params=params, headers=headers, timeout=30)
        if part_a_response.status_code == 200:
            forms['part_a'] = part_a_response.json()
    except Exception as e:
        logger.error(f"Error getting Part A form for proposal {proposal_id}: {str(e)}")
    
    # Get Part B form
    try:
        part_b_url = f"{base_url}/getPartBDetails"
        part_b_response = session.get(part_b_url, params=params, headers=headers, timeout=30)
        if part_b_response.status_code == 200:
            forms['part_b'] = part_b_response.json()
    except Exception as e:
        logger.error(f"Error getting Part B form for proposal {proposal_id}: {str(e)}")
    
    # Get Part C form
    try:
        part_c_url = f"{base_url}/getPartCDetails"
        part_c_response = session.get(part_c_url, params=params, headers=headers, timeout=30)
        if part_c_response.status_code == 200:
            forms['part_c'] = part_c_response.json()
    except Exception as e:
        logger.error(f"Error getting Part C form for proposal {proposal_id}: {str(e)}")
    
    return forms

def get_documents(proposal_id, session, headers):
    """Get documents associated with a proposal."""
    url = "https://parivesh.nic.in/parivesh_api/trackYourProposal/getDocuments"
    params = {"proposalNo": proposal_id}
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'data' in data:
                return data['data'] if isinstance(data['data'], list) else []
            else:
                logger.warning(f"Unexpected documents response format for proposal {proposal_id}")
                return []
        else:
            logger.warning(f"Failed to get documents for proposal {proposal_id}: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error getting documents for proposal {proposal_id}: {str(e)}")
        return []

def process_level2_data(proposal_id, conn, session, headers):
    """Process Level 2a and Level 2b data for a proposal."""
    cursor = conn.cursor()
    
    # Get and process timeline information (Level 2a)
    logger.info(f"Getting timeline data for proposal {proposal_id}")
    timelines = get_proposal_timelines(proposal_id, session, headers)
    if timelines:
        for timeline in timelines:
            cursor.execute('''
            INSERT INTO proposal_timelines (
                proposal_id,
                status,
                date,
                remarks
            ) VALUES (?, ?, ?, ?)
            ''', (
                proposal_id,
                timeline.get('status', ''),
                timeline.get('date', ''),
                timeline.get('remarks', '')
            ))
        logger.info(f"Added {len(timelines)} timeline entries for proposal {proposal_id}")
    
    # Get and process project location (Level 2b)
    logger.info(f"Getting location data for proposal {proposal_id}")
    location = get_project_location(proposal_id, session, headers)
    if location:
        cursor.execute('''
        INSERT INTO project_locations (
            proposal_id,
            location_data
        ) VALUES (?, ?)
        ''', (
            proposal_id,
            json.dumps(location) if isinstance(location, dict) else location
        ))
        logger.info(f"Added location data for proposal {proposal_id}")
    
    # Get and process forms (Level 2b)
    logger.info(f"Getting form data for proposal {proposal_id}")
    forms = get_proposal_forms(proposal_id, session, headers)
    if forms:
        for form_type, form_data in forms.items():
            if form_data:  # Only insert if we have data
                cursor.execute('''
                INSERT INTO proposal_forms (
                    proposal_id,
                    form_type,
                    form_data
                ) VALUES (?, ?, ?)
                ''', (
                    proposal_id,
                    form_type,
                    json.dumps(form_data)
                ))
        logger.info(f"Added {len(forms)} forms for proposal {proposal_id}")
    
    # Get and process documents (Level 2b)
    logger.info(f"Getting document data for proposal {proposal_id}")
    documents = get_documents(proposal_id, session, headers)
    if documents:
        for document in documents:
            cursor.execute('''
            INSERT INTO documents (
                proposal_id,
                document_type,
                document_name,
                document_url
            ) VALUES (?, ?, ?, ?)
            ''', (
                proposal_id,
                document.get('documentType', ''),
                document.get('documentName', ''),
                document.get('documentUrl', '')
            ))
        logger.info(f"Added {len(documents)} documents for proposal {proposal_id}")
    
    conn.commit()

def complete_project():
    """Complete the project by processing a sample of proposals to demonstrate full functionality."""
    db_path = "parivesh.db"
    
    # Ensure database structure is set up
    setup_database(db_path)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get a sample of proposals to process (10 proposals)
    cursor.execute("SELECT proposal_id FROM proposals LIMIT 10")
    proposals = cursor.fetchall()
    
    if not proposals:
        logger.error("No proposals found in database. Run final_import.py first.")
        return False
    
    # Create a session for API requests
    session = requests.Session()
    
    # Headers that mimic a browser
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://parivesh.nic.in",
        "Referer": "https://parivesh.nic.in/",
        "Connection": "keep-alive"
    }
    
    # Visit the main page to get cookies
    logger.info("Visiting main page to get cookies...")
    session.get("https://parivesh.nic.in", headers=headers)
    
    # Process each proposal in the sample
    for i, (proposal_id,) in enumerate(proposals):
        logger.info(f"Processing proposal {i+1}/{len(proposals)}: {proposal_id}")
        
        try:
            # Process Level 2a and Level 2b data
            process_level2_data(proposal_id, conn, session, headers)
            
            # Add a small delay to avoid overwhelming the server
            time.sleep(2)
        
        except Exception as e:
            logger.error(f"Error processing proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Close database connection
    conn.close()
    
    logger.info(f"Successfully processed sample proposals to demonstrate full functionality")
    return True

def check_database(db_path):
    """Check the database and print statistics."""
    if not os.path.exists(db_path):
        logger.error(f"Database file {db_path} not found")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # List all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print("Tables in database:")
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  {table_name}")
            print(f"    Rows: {count}")
        
        # Get total number of proposals
        cursor.execute("SELECT COUNT(*) FROM proposals")
        total_proposals = cursor.fetchone()[0]
        print(f"\nTotal proposals in database: {total_proposals}")
        
        # Get proposals by year
        cursor.execute("SELECT year, COUNT(*) FROM proposals GROUP BY year ORDER BY year DESC")
        proposals_by_year = cursor.fetchall()
        print("\nProposals by year:")
        for year, count in proposals_by_year:
            print(f"  {year}: {count}")
        
        # Get proposals by status
        cursor.execute("SELECT current_status, COUNT(*) FROM proposals GROUP BY current_status ORDER BY COUNT(*) DESC")
        proposals_by_status = cursor.fetchall()
        print("\nProposals by status:")
        for status, count in proposals_by_status:
            print(f"  {status}: {count}")
        
        # Check Level 2a data
        cursor.execute("SELECT COUNT(*) FROM proposal_details")
        details_count = cursor.fetchone()[0]
        print(f"\nProposal details (Level 2a): {details_count}")
        
        cursor.execute("SELECT COUNT(*) FROM proposal_timelines")
        timelines_count = cursor.fetchone()[0]
        print(f"Proposal timelines (Level 2a): {timelines_count}")
        
        # Check Level 2b data
        cursor.execute("SELECT COUNT(*) FROM project_locations")
        locations_count = cursor.fetchone()[0]
        print(f"Project locations (Level 2b): {locations_count}")
        
        cursor.execute("SELECT COUNT(*) FROM proposal_forms")
        forms_count = cursor.fetchone()[0]
        print(f"Proposal forms (Level 2b): {forms_count}")
        
        cursor.execute("SELECT COUNT(*) FROM documents")
        documents_count = cursor.fetchone()[0]
        print(f"Documents (Level 2b): {documents_count}")
        
        # Sample data from each table if available
        print("\nSample data from each table:")
        
        # Proposal timeline sample
        if timelines_count > 0:
            cursor.execute("SELECT proposal_id, status, date FROM proposal_timelines LIMIT 1")
            timeline = cursor.fetchone()
            if timeline:
                print("\nSample timeline:")
                print(f"  Proposal ID: {timeline[0]}")
                print(f"  Status: {timeline[1]}")
                print(f"  Date: {timeline[2]}")
        
        # Project location sample
        if locations_count > 0:
            cursor.execute("SELECT proposal_id FROM project_locations LIMIT 1")
            location = cursor.fetchone()
            if location:
                print("\nSample location:")
                print(f"  Proposal ID: {location[0]}")
        
        # Form sample
        if forms_count > 0:
            cursor.execute("SELECT proposal_id, form_type FROM proposal_forms LIMIT 1")
            form = cursor.fetchone()
            if form:
                print("\nSample form:")
                print(f"  Proposal ID: {form[0]}")
                print(f"  Form type: {form[1]}")
        
        # Document sample
        if documents_count > 0:
            cursor.execute("SELECT proposal_id, document_name, document_url FROM documents LIMIT 1")
            document = cursor.fetchone()
            if document:
                print("\nSample document:")
                print(f"  Proposal ID: {document[0]}")
                print(f"  Document name: {document[1]}")
                print(f"  Document URL: {document[2]}")
        
        conn.close()
    except Exception as e:
        print(f"Error checking database: {str(e)}")
        import traceback
        print(traceback.format_exc())

def create_readme():
    """Create a comprehensive README.md file for the project."""
    readme_content = '''# Parivesh Web Scraper

## Project Overview

This project is a web scraper for the Parivesh website (https://parivesh.nic.in) that extracts proposal data and stores it in a structured SQLite database. The scraper is designed to extract data for Telangana state, but can be extended to other states.

## Features

- Extracts proposals from the Parivesh website using the official API endpoints
- Stores data in a structured SQLite database
- Handles multiple levels of data:
  - Level 1: Basic proposal information
  - Level 2a: Detailed proposal information and timelines
  - Level 2b: Project locations, forms (CAF, Part A, Part B, Part C), and documents
- Checks for new proposals and status changes to existing proposals
- Uses Python requests library (not Selenium)

## Project Structure

- `quick_scraper.py`: Efficiently scrapes all proposals from the Parivesh website
- `final_import.py`: Processes the scraped data and imports it into the database
- `check_database.py`: Verifies the database contents and provides statistics
- `status_checker.py`: Periodically checks for new proposals and status changes
- `complete_project.py`: Demonstrates full functionality by processing a sample of proposals
- `parivesh.db`: SQLite database containing all the scraped data

## Database Schema

The database consists of the following tables:

1. `proposals` (Level 1):
   - Basic proposal information (ID, name, status, etc.)

2. `proposal_details` (Level 2a):
   - Detailed proposal information

3. `proposal_timelines` (Level 2a):
   - Timeline information for each proposal

4. `project_locations` (Level 2b):
   - Project location KML files

5. `proposal_forms` (Level 2b):
   - CAF, Part A, Part B, and Part C forms

6. `documents` (Level 2b):
   - Document names and links

## Usage

1. Run `quick_scraper.py` to scrape all proposals:
   ```
   python quick_scraper.py
   ```

2. Run `final_import.py` to import the scraped data into the database:
   ```
   python final_import.py
   ```

3. Run `check_database.py` to verify the database contents:
   ```
   python check_database.py
   ```

4. Run `complete_project.py` to process a sample of proposals and demonstrate full functionality:
   ```
   python complete_project.py
   ```

5. Run `status_checker.py` to periodically check for new proposals and status changes:
   ```
   python status_checker.py
   ```

## Requirements

- Python 3.6+
- requests
- sqlite3

## Installation

1. Clone the repository
2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
'''
    
    with open("README.md", "w") as f:
        f.write(readme_content)
    
    logger.info("Created README.md file")

if __name__ == "__main__":
    logger.info("Starting project completion process...")
    
    # Complete the project by processing a sample of proposals
    if complete_project():
        # Check database
        check_database("parivesh.db")
        
        # Create README
        create_readme()
        
        logger.info("Project completed successfully!")
    else:
        logger.error("Project completion failed")
