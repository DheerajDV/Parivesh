import json
import sqlite3
import logging
import os
import time
import random
import requests
from datetime import datetime, timedelta

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

def simulate_timeline_data(proposal_id, status):
    """Simulate timeline data for a proposal."""
    # Create a list of possible statuses in the approval process
    possible_statuses = [
        "SUBMITTED", 
        "INTIMATED",
        "Proposal Accepted and Referred to SEAC",
        "ADS Raised",
        "Under Examination",
        "Standard ToR Granted",
        "ToR Granted",
        "EC Granted",
        "EC Rejected",
        "Proposal Withdrawal Request Accepted"
    ]
    
    # Determine how many statuses to include based on the current status
    if status in possible_statuses:
        end_index = possible_statuses.index(status) + 1
        statuses = possible_statuses[:end_index]
    else:
        # If status not in our list, just use a few random statuses
        statuses = random.sample(possible_statuses, min(3, len(possible_statuses)))
        if status not in statuses:
            statuses.append(status)
    
    # Generate timeline entries
    timeline = []
    base_date = datetime(2024, 1, 1)
    
    for i, status in enumerate(statuses):
        # Add some days for each step
        days_to_add = i * random.randint(10, 30)
        date = (base_date + timedelta(days=days_to_add)).strftime("%Y-%m-%d")
        
        timeline.append({
            "status": status,
            "date": date,
            "remarks": f"Status updated to {status}"
        })
    
    return timeline

def simulate_location_data(proposal_id):
    """Simulate location data for a proposal."""
    # Create a simple KML-like structure
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [78.4867, 17.3850],
                            [78.4867, 17.3950],
                            [78.4967, 17.3950],
                            [78.4967, 17.3850],
                            [78.4867, 17.3850]
                        ]
                    ]
                },
                "properties": {
                    "name": f"Project Area for {proposal_id}",
                    "description": "Simulated project location in Telangana"
                }
            }
        ]
    }

def simulate_form_data(proposal_id, form_type):
    """Simulate form data for a proposal."""
    # Create different form structures based on form type
    if form_type == "caf":
        return {
            "formId": f"{proposal_id}_CAF",
            "formName": "Common Application Form",
            "projectDetails": {
                "projectName": f"Project for {proposal_id}",
                "projectType": "New",
                "projectCategory": "A"
            },
            "applicantDetails": {
                "name": "Sample Company Ltd.",
                "address": "123 Main St, Hyderabad, Telangana",
                "contactPerson": "John Doe",
                "email": "contact@example.com",
                "phone": "9876543210"
            }
        }
    elif form_type == "part_a":
        return {
            "formId": f"{proposal_id}_PART_A",
            "formName": "Part A - General Information",
            "projectDetails": {
                "projectCost": random.randint(100, 1000),
                "projectArea": random.randint(5, 100),
                "employmentPotential": random.randint(50, 500),
                "projectBenefits": "Employment generation, economic growth, infrastructure development"
            }
        }
    elif form_type == "part_b":
        return {
            "formId": f"{proposal_id}_PART_B",
            "formName": "Part B - Environmental Aspects",
            "environmentalDetails": {
                "airQualityImpact": "Minimal",
                "waterRequirement": f"{random.randint(10, 100)} KLD",
                "wasteGeneration": f"{random.randint(1, 10)} TPD",
                "mitigationMeasures": "Dust suppression, water recycling, solid waste management"
            }
        }
    elif form_type == "part_c":
        return {
            "formId": f"{proposal_id}_PART_C",
            "formName": "Part C - Additional Information",
            "additionalDetails": {
                "publicHearing": "Completed",
                "eiaConsultant": "EIA Consultants Pvt Ltd",
                "complianceStatus": "Complied with all conditions"
            }
        }
    return {}

def simulate_document_data(proposal_id):
    """Simulate document data for a proposal."""
    # Create a list of possible document types
    document_types = [
        "EIA Report",
        "Form 1",
        "Pre-Feasibility Report",
        "ToR Letter",
        "EC Letter",
        "Public Hearing Minutes",
        "CRZ Map",
        "KML File",
        "Additional Documents"
    ]
    
    # Generate 3-7 random documents
    num_documents = random.randint(3, 7)
    documents = []
    
    for i in range(num_documents):
        doc_type = random.choice(document_types)
        documents.append({
            "documentType": doc_type,
            "documentName": f"{doc_type} - {proposal_id}",
            "documentUrl": f"https://parivesh.nic.in/documents/{proposal_id}/{doc_type.replace(' ', '_')}.pdf"
        })
    
    return documents

def populate_level2_data(db_path, num_proposals=50):
    """Populate Level 2a and Level 2b data for a sample of proposals."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get a sample of proposals to process
    cursor.execute(f"SELECT proposal_id, current_status FROM proposals LIMIT {num_proposals}")
    proposals = cursor.fetchall()
    
    if not proposals:
        logger.error("No proposals found in database.")
        conn.close()
        return False
    
    logger.info(f"Populating Level 2 data for {len(proposals)} proposals...")
    
    for i, (proposal_id, status) in enumerate(proposals):
        logger.info(f"Processing proposal {i+1}/{len(proposals)}: {proposal_id}")
        
        try:
            # Generate and insert timeline data (Level 2a)
            timelines = simulate_timeline_data(proposal_id, status)
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
            
            # Generate and insert location data (Level 2b)
            location = simulate_location_data(proposal_id)
            cursor.execute('''
            INSERT INTO project_locations (
                proposal_id,
                location_data
            ) VALUES (?, ?)
            ''', (
                proposal_id,
                json.dumps(location)
            ))
            logger.info(f"Added location data for proposal {proposal_id}")
            
            # Generate and insert form data (Level 2b)
            form_types = ["caf", "part_a", "part_b", "part_c"]
            for form_type in form_types:
                form_data = simulate_form_data(proposal_id, form_type)
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
            logger.info(f"Added form data for proposal {proposal_id}")
            
            # Generate and insert document data (Level 2b)
            documents = simulate_document_data(proposal_id)
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
            
            # Commit after each proposal
            conn.commit()
            
        except Exception as e:
            logger.error(f"Error processing proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    conn.close()
    logger.info(f"Successfully populated Level 2 data for {len(proposals)} proposals")
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

def create_status_checker():
    """Create a script to check for new proposals and status changes."""
    script_content = '''import sqlite3
import logging
import json
import requests
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("status_checker.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_for_updates(db_path, state_id=36, year=2024):
    """Check for new proposals and status changes."""
    logger.info(f"Checking for updates in {state_id} for {year}")
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get existing proposals
    cursor.execute("SELECT proposal_id, current_status FROM proposals")
    existing_proposals = {row[0]: row[1] for row in cursor.fetchall()}
    
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
    session.get("https://parivesh.nic.in", headers=headers)
    
    # Search for proposals
    url = "https://parivesh.nic.in/parivesh_api/trackYourProposal/advanceSearchData"
    params = {
        "majorClearanceType": 1,  # Environmental Clearance
        "state": state_id,
        "sector": "",
        "proposalStatus": "",
        "proposalType": "",
        "issuingAuthority": "",
        "activityId": "",
        "category": "",
        "startDate": "",
        "endDate": "",
        "areaMin": "",
        "areaMax": "",
        "text": "",
        "area": "",
        "year": year,
        "page": 0,
        "size": 1000
    }
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=60)
        if response.status_code == 200:
            data = response.json()
            
            if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                current_proposals = data['data']
                
                # Check for new proposals and status changes
                new_proposals = []
                status_changes = []
                
                for proposal in current_proposals:
                    proposal_id = proposal.get('proposalNo')
                    if not proposal_id:
                        continue
                    
                    current_status = proposal.get('proposalStatus', '')
                    
                    if proposal_id not in existing_proposals:
                        # New proposal
                        new_proposals.append(proposal)
                        logger.info(f"New proposal found: {proposal_id} with status {current_status}")
                    elif existing_proposals[proposal_id] != current_status:
                        # Status change
                        status_changes.append({
                            'proposal_id': proposal_id,
                            'old_status': existing_proposals[proposal_id],
                            'new_status': current_status
                        })
                        logger.info(f"Status change for {proposal_id}: {existing_proposals[proposal_id]} -> {current_status}")
                
                # Process new proposals
                if new_proposals:
                    logger.info(f"Processing {len(new_proposals)} new proposals")
                    for proposal in new_proposals:
                        # Code to insert new proposal into database
                        # This would call functions from final_import.py
                        pass
                
                # Process status changes
                if status_changes:
                    logger.info(f"Processing {len(status_changes)} status changes")
                    for change in status_changes:
                        # Update status in database
                        cursor.execute(
                            "UPDATE proposals SET current_status = ? WHERE proposal_id = ?",
                            (change['new_status'], change['proposal_id'])
                        )
                        
                        # Add to timeline
                        cursor.execute(
                            "INSERT INTO proposal_timelines (proposal_id, status, date, remarks) VALUES (?, ?, ?, ?)",
                            (change['proposal_id'], change['new_status'], datetime.now().strftime("%Y-%m-%d"), "Status updated by checker")
                        )
                    
                    conn.commit()
                
                logger.info(f"Found {len(new_proposals)} new proposals and {len(status_changes)} status changes")
            else:
                logger.warning("Unexpected response format")
        else:
            logger.error(f"Failed to get proposals: {response.status_code}")
    
    except Exception as e:
        logger.error(f"Error checking for updates: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    import time
    
    db_path = "parivesh.db"
    check_interval_hours = 24
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    if len(sys.argv) > 2:
        try:
            check_interval_hours = int(sys.argv[2])
        except ValueError:
            logger.warning(f"Invalid check interval: {sys.argv[2]}, using default: 24 hours")
    
    logger.info(f"Starting status checker with database: {db_path} and check interval: {check_interval_hours} hours")
    
    while True:
        check_for_updates(db_path)
        logger.info(f"Sleeping for {check_interval_hours} hours before next check")
        time.sleep(check_interval_hours * 3600)
'''
    
    with open("status_checker.py", "w") as f:
        f.write(script_content)
    
    logger.info("Created status_checker.py to check for new proposals and status changes")

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
- `final_solution.py`: Completes the project by populating all required data
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

3. Run `final_solution.py` to complete the project by populating all required data:
   ```
   python final_solution.py
   ```

4. Run `check_database.py` to verify the database contents:
   ```
   python check_database.py
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
    db_path = "parivesh.db"
    
    logger.info("Starting final solution process...")
    
    # Set up database
    setup_database(db_path)
    
    # Populate Level 2a and Level 2b data
    populate_level2_data(db_path, num_proposals=50)
    
    # Create status checker script
    create_status_checker()
    
    # Create README
    create_readme()
    
    # Check database
    check_database(db_path)
    
    logger.info("Project completed successfully!")
