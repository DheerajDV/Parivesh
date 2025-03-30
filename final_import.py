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
    
    # Drop existing tables if they exist
    tables = [
        "proposals",
        "proposal_details",
        "proposal_timelines",
        "project_locations",
        "proposal_forms",
        "documents"
    ]
    
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Create proposals table (Level 1)
    cursor.execute('''
    CREATE TABLE proposals (
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
    
    # Create proposal_details table (Level 2a)
    cursor.execute('''
    CREATE TABLE proposal_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT UNIQUE,
        raw_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_timelines table (Level 2a)
    cursor.execute('''
    CREATE TABLE proposal_timelines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        status TEXT,
        date TEXT,
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create project_locations table (Level 2b)
    cursor.execute('''
    CREATE TABLE project_locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        location_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_forms table (Level 2b)
    cursor.execute('''
    CREATE TABLE proposal_forms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        form_type TEXT,
        form_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create documents table (Level 2b)
    cursor.execute('''
    CREATE TABLE documents (
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
    logger.info(f"Database {db_path} setup complete with fresh schema")

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
    forms = get_proposal_forms(proposal_id, session, headers)
    if forms:
        for form_type, form_data in forms.items():
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

def import_proposals(json_file, db_path):
    """Import proposals from JSON file to database."""
    if not os.path.exists(json_file):
        logger.error(f"JSON file {json_file} not found")
        return False
    
    try:
        # Load proposals from JSON file
        with open(json_file, 'r') as f:
            proposals = json.load(f)
        
        logger.info(f"Loaded {len(proposals)} proposals from {json_file}")
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
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
        
        # Process each proposal
        for i, proposal in enumerate(proposals):
            try:
                proposal_id = proposal.get('proposalNo')
                if not proposal_id:
                    logger.warning(f"Proposal at index {i} has no proposalNo, skipping")
                    continue
                
                logger.info(f"Processing proposal {i+1}/{len(proposals)}: {proposal_id}")
                
                # Extract year from proposal ID
                year = None
                if proposal_id and len(proposal_id) >= 4:
                    try:
                        year = int(proposal_id[-4:])
                    except ValueError:
                        year = datetime.now().year
                
                # Insert into proposals table (Level 1)
                cursor.execute('''
                INSERT INTO proposals (
                    proposal_id,
                    sw_no,
                    project_name,
                    company_name,
                    state,
                    category,
                    sector,
                    current_status,
                    proposal_type,
                    clearance_type,
                    submission_date,
                    last_updated,
                    year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    proposal_id,
                    proposal.get('swNo', ''),
                    proposal.get('projectName', ''),
                    proposal.get('companyName', ''),
                    proposal.get('stateName', ''),
                    proposal.get('categoryName', ''),
                    proposal.get('sectorName', ''),
                    proposal.get('proposalStatus', ''),
                    proposal.get('proposalType', ''),
                    proposal.get('clearanceTypeName', ''),
                    proposal.get('lastSubmissionDate', ''),
                    proposal.get('lastStatusDate', ''),
                    year
                ))
                
                # Insert into proposal_details table (Level 2a)
                cursor.execute('''
                INSERT INTO proposal_details (
                    proposal_id,
                    raw_json
                ) VALUES (?, ?)
                ''', (
                    proposal_id,
                    json.dumps(proposal)
                ))
                
                # Process Level 2a and Level 2b data
                process_level2_data(proposal_id, conn, session, headers)
                
                # Add a small delay to avoid overwhelming the server
                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i+1} proposals, taking a short break...")
                    time.sleep(2)
            
            except Exception as e:
                logger.error(f"Error processing proposal {proposal_id}: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
        
        # Close database connection
        conn.close()
        
        logger.info(f"Successfully imported {len(proposals)} proposals to database")
        return True
    
    except Exception as e:
        logger.error(f"Error importing proposals: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

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
        
        # Sample proposal data
        cursor.execute("SELECT proposal_id, current_status FROM proposals LIMIT 1")
        sample = cursor.fetchone()
        if sample:
            print("\nSample proposal data:")
            print(f"  ID: {sample[0]}")
            print(f"  Status: {sample[1]}")
        
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

if __name__ == "__main__":
    json_file = "telangana_2024_all_proposals.json"
    db_path = "parivesh.db"
    
    logger.info("Starting final import process...")
    
    # Set up fresh database
    setup_database(db_path)
    
    # Import proposals with all levels of data
    if import_proposals(json_file, db_path):
        logger.info("Import completed successfully")
        
        # Create status checker script
        create_status_checker()
        
        # Check database
        check_database(db_path)
    else:
        logger.error("Import failed")
