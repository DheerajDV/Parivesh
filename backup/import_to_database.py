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

def create_database_schema(db_path):
    """Create the database schema if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create proposals table (Level 1)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT UNIQUE,
        proposal_no TEXT,
        project_name TEXT,
        company_name TEXT,
        state TEXT,
        category TEXT,
        sector TEXT,
        proposal_status TEXT,
        proposal_type TEXT,
        clearance_type TEXT,
        sw_no TEXT,
        last_submission_date TEXT,
        last_status_date TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create proposal_details table (Level 2a)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT UNIQUE,
        proposal_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_timelines table (Level 2a)
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
    
    # Create project_locations table (Level 2b)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposal_id TEXT,
        location_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
    )
    ''')
    
    # Create proposal_forms table (Level 2b)
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
    
    # Create documents table (Level 2b)
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
    logger.info("Database schema created successfully")

def process_proposal(proposal, cursor, session, headers):
    """Process a single proposal and insert into database."""
    proposal_id = proposal.get('proposalNo')
    if not proposal_id:
        logger.warning("Proposal ID not found in proposal data")
        return False
    
    # Check if proposal already exists
    cursor.execute("SELECT id FROM proposals WHERE proposal_id = ?", (proposal_id,))
    result = cursor.fetchone()
    if result:
        logger.info(f"Proposal {proposal_id} already exists in database, updating...")
        # Update existing proposal
        cursor.execute('''
        UPDATE proposals SET
            proposal_no = ?,
            project_name = ?,
            company_name = ?,
            state = ?,
            category = ?,
            sector = ?,
            proposal_status = ?,
            proposal_type = ?,
            clearance_type = ?,
            sw_no = ?,
            last_submission_date = ?,
            last_status_date = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE proposal_id = ?
        ''', (
            proposal.get('proposalNo', ''),
            proposal.get('projectName', ''),
            proposal.get('companyName', ''),
            proposal.get('stateName', ''),
            proposal.get('categoryName', ''),
            proposal.get('sectorName', ''),
            proposal.get('proposalStatus', ''),
            proposal.get('proposalType', ''),
            proposal.get('clearanceTypeName', ''),
            proposal.get('swNo', ''),
            proposal.get('lastSubmissionDate', ''),
            proposal.get('lastStatusDate', ''),
            proposal_id
        ))
    else:
        # Insert new proposal
        cursor.execute('''
        INSERT INTO proposals (
            proposal_id,
            proposal_no,
            project_name,
            company_name,
            state,
            category,
            sector,
            proposal_status,
            proposal_type,
            clearance_type,
            sw_no,
            last_submission_date,
            last_status_date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            proposal_id,
            proposal.get('proposalNo', ''),
            proposal.get('projectName', ''),
            proposal.get('companyName', ''),
            proposal.get('stateName', ''),
            proposal.get('categoryName', ''),
            proposal.get('sectorName', ''),
            proposal.get('proposalStatus', ''),
            proposal.get('proposalType', ''),
            proposal.get('clearanceTypeName', ''),
            proposal.get('swNo', ''),
            proposal.get('lastSubmissionDate', ''),
            proposal.get('lastStatusDate', '')
        ))
    
    # Store proposal details (Level 2a)
    cursor.execute("SELECT id FROM proposal_details WHERE proposal_id = ?", (proposal_id,))
    result = cursor.fetchone()
    if result:
        # Update existing details
        cursor.execute('''
        UPDATE proposal_details SET
            proposal_json = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE proposal_id = ?
        ''', (json.dumps(proposal), proposal_id))
    else:
        # Insert new details
        cursor.execute('''
        INSERT INTO proposal_details (
            proposal_id,
            proposal_json
        ) VALUES (?, ?)
        ''', (proposal_id, json.dumps(proposal)))
    
    # Get and process additional data (Level 2a and 2b)
    get_and_process_additional_data(cursor, proposal_id, session, headers)
    
    return True

def get_and_process_additional_data(cursor, proposal_id, session, headers):
    """Get and process additional data for a proposal."""
    # API base URL
    base_url = "https://parivesh.nic.in/parivesh_api/trackYourProposal"
    
    # Get proposal timelines (Level 2a)
    try:
        timelines = get_proposal_timelines(proposal_id, session, headers, base_url)
        if timelines:
            # Delete existing timelines
            cursor.execute("DELETE FROM proposal_timelines WHERE proposal_id = ?", (proposal_id,))
            
            # Insert new timelines
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
            logger.info(f"Inserted {len(timelines)} timelines for proposal {proposal_id}")
    except Exception as e:
        logger.error(f"Error getting timelines for proposal {proposal_id}: {str(e)}")
    
    # Get project location (Level 2b)
    try:
        location = get_project_location(proposal_id, session, headers, base_url)
        if location:
            # Delete existing location
            cursor.execute("DELETE FROM project_locations WHERE proposal_id = ?", (proposal_id,))
            
            # Insert new location
            cursor.execute('''
            INSERT INTO project_locations (
                proposal_id,
                location_data
            ) VALUES (?, ?)
            ''', (proposal_id, json.dumps(location) if isinstance(location, dict) else location))
            logger.info(f"Inserted location data for proposal {proposal_id}")
    except Exception as e:
        logger.error(f"Error getting location for proposal {proposal_id}: {str(e)}")
    
    # Get proposal forms (Level 2b)
    try:
        forms = get_proposal_forms(proposal_id, session, headers, base_url)
        if forms:
            # Delete existing forms
            cursor.execute("DELETE FROM proposal_forms WHERE proposal_id = ?", (proposal_id,))
            
            # Insert new forms
            for form_type, form_data in forms.items():
                cursor.execute('''
                INSERT INTO proposal_forms (
                    proposal_id,
                    form_type,
                    form_data
                ) VALUES (?, ?, ?)
                ''', (proposal_id, form_type, json.dumps(form_data)))
            logger.info(f"Inserted {len(forms)} forms for proposal {proposal_id}")
    except Exception as e:
        logger.error(f"Error getting forms for proposal {proposal_id}: {str(e)}")
    
    # Get documents (Level 2b)
    try:
        documents = get_documents(proposal_id, session, headers, base_url)
        if documents:
            # Delete existing documents
            cursor.execute("DELETE FROM documents WHERE proposal_id = ?", (proposal_id,))
            
            # Insert new documents
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
            logger.info(f"Inserted {len(documents)} documents for proposal {proposal_id}")
    except Exception as e:
        logger.error(f"Error getting documents for proposal {proposal_id}: {str(e)}")

def get_proposal_timelines(proposal_id, session, headers, base_url):
    """Get timeline information for a proposal."""
    url = f"{base_url}/getApprovalDates"
    params = {"proposalNo": proposal_id}
    
    try:
        response = session.get(url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            # Log the structure of the response for debugging
            logger.debug(f"Timeline response structure: {type(data)}")
            
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

def get_project_location(proposal_id, session, headers, base_url):
    """Get the KML file for a project's location."""
    # First, get the form ID
    url = f"{base_url}/dataOfProposalNo"
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
            kml_url = f"{base_url}/getKmlFile"
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

def get_proposal_forms(proposal_id, session, headers, base_url):
    """Get various forms for a proposal."""
    forms = {}
    
    # Get CAF form
    caf_url = f"{base_url}/getCaFormDetails"
    params = {"proposalNo": proposal_id}
    
    try:
        caf_response = session.get(caf_url, params=params, headers=headers, timeout=30)
        if caf_response.status_code == 200:
            forms['caf'] = caf_response.json()
    except Exception as e:
        logger.error(f"Error getting CAF form for proposal {proposal_id}: {str(e)}")
    
    # Get Part A form
    part_a_url = f"{base_url}/getPartADetails"
    try:
        part_a_response = session.get(part_a_url, params=params, headers=headers, timeout=30)
        if part_a_response.status_code == 200:
            forms['part_a'] = part_a_response.json()
    except Exception as e:
        logger.error(f"Error getting Part A form for proposal {proposal_id}: {str(e)}")
    
    # Get Part B form
    part_b_url = f"{base_url}/getPartBDetails"
    try:
        part_b_response = session.get(part_b_url, params=params, headers=headers, timeout=30)
        if part_b_response.status_code == 200:
            forms['part_b'] = part_b_response.json()
    except Exception as e:
        logger.error(f"Error getting Part B form for proposal {proposal_id}: {str(e)}")
    
    # Get Part C form
    part_c_url = f"{base_url}/getPartCDetails"
    try:
        part_c_response = session.get(part_c_url, params=params, headers=headers, timeout=30)
        if part_c_response.status_code == 200:
            forms['part_c'] = part_c_response.json()
    except Exception as e:
        logger.error(f"Error getting Part C form for proposal {proposal_id}: {str(e)}")
    
    return forms

def get_documents(proposal_id, session, headers, base_url):
    """Get documents associated with a proposal."""
    url = f"{base_url}/getDocuments"
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

def import_to_database(json_file, db_path):
    """Import proposals from JSON file to SQLite database."""
    logger.info(f"Importing data from {json_file} to {db_path}")
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        logger.error(f"JSON file {json_file} not found")
        return False
    
    # Load proposals from JSON file
    try:
        with open(json_file, 'r') as f:
            proposals = json.load(f)
        
        logger.info(f"Loaded {len(proposals)} proposals from {json_file}")
    except Exception as e:
        logger.error(f"Error loading JSON file: {str(e)}")
        return False
    
    # Create database schema
    create_database_schema(db_path)
    
    # Create a database connection
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
    total_processed = 0
    for i, proposal in enumerate(proposals):
        logger.info(f"Processing proposal {i+1}/{len(proposals)}: {proposal.get('proposalNo', 'Unknown')}")
        
        try:
            result = process_proposal(proposal, cursor, session, headers)
            if result:
                total_processed += 1
                conn.commit()  # Commit after each successful proposal
            
            # Add a small delay to avoid overwhelming the server
            if i % 10 == 0 and i > 0:
                logger.info(f"Processed {i} proposals, taking a short break...")
                time.sleep(2)
        except Exception as e:
            logger.error(f"Error processing proposal {proposal.get('proposalNo', 'Unknown')}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    # Close database connection
    conn.close()
    
    logger.info(f"Import completed. Processed {total_processed} out of {len(proposals)} proposals")
    
    # Run database check
    try:
        import check_database
        check_database.check_database(db_path)
    except ImportError:
        logger.warning("check_database.py not found, skipping database verification")
    
    return True

if __name__ == "__main__":
    json_file = "telangana_2024_all_proposals.json"
    db_path = "parivesh.db"
    
    logger.info("Starting import process...")
    import_to_database(json_file, db_path)
    logger.info("Import process completed")
