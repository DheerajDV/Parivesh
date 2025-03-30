import requests
from bs4 import BeautifulSoup
import json
import time
import logging
from datetime import datetime
import re
import os
import sqlite3

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='parivesh_scraper.log',
    filemode='a'
)
logger = logging.getLogger('parivesh_scraper')

class PariveshScraper:
    def __init__(self, database_path, base_url="https://parivesh.nic.in"):
        """Initialize the scraper with the database path."""
        self.db_path = database_path
        self.base_url = base_url
        self.parivesh_api = f"{base_url}/parivesh_api"
        self.session = requests.Session()
        
        # Set up headers to mimic a browser
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': base_url,
            'Referer': base_url,
            'Connection': 'keep-alive',
            'Content-Type': 'application/json'
        }
        
        # Create a directory for downloaded files
        os.makedirs("downloads", exist_ok=True)
        
        # Initialize database
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        
        # Create proposals table (Level 1 data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS proposals (
            proposal_id TEXT PRIMARY KEY,
            s_w_no TEXT,
            state TEXT,
            year INTEGER,
            proposal_title TEXT,
            current_status TEXT,
            last_updated TIMESTAMP
        )
        ''')
        
        # Create proposal_details table (Level 2a data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS proposal_details (
            proposal_id TEXT PRIMARY KEY,
            clearance_type TEXT,
            project_name TEXT,
            project_location TEXT,
            proponent TEXT,
            category TEXT,
            sector TEXT,
            issuing_authority TEXT,
            submission_date TEXT,
            caf_no TEXT,
            single_window_no TEXT,
            FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
        )
        ''')
        
        # Create proposal_timelines table (Level 2a data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS proposal_timelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id TEXT,
            date TEXT,
            status TEXT,
            remarks TEXT,
            FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
        )
        ''')
        
        # Create project_locations table (Level 2b data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS project_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id TEXT,
            kml_data TEXT,
            FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
        )
        ''')
        
        # Create proposal_forms table (Level 2b data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS proposal_forms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id TEXT,
            form_type TEXT,
            form_data TEXT,
            FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
        )
        ''')
        
        # Create documents table (Level 2b data)
        c.execute('''
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proposal_id TEXT,
            document_name TEXT,
            document_link TEXT,
            FOREIGN KEY (proposal_id) REFERENCES proposals (proposal_id)
        )
        ''')
        
        conn.commit()
        conn.close()
    
    def random_delay(self, min_seconds=1, max_seconds=3):
        """Add a random delay to avoid overloading the server."""
        delay = min_seconds + (max_seconds - min_seconds) * (time.time() % 1)
        time.sleep(delay)
    
    def get_csrf_token(self):
        """Get CSRF token from the main page."""
        try:
            logger.info("Getting CSRF token")
            response = self.session.get(
                f"{self.base_url}/newupgrade/#/trackYourProposal",
                headers=self.headers,
                timeout=30
            )
            response.raise_for_status()
            
            # Extract CSRF token from the page
            soup = BeautifulSoup(response.text, 'lxml')
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            
            if csrf_meta and csrf_meta.get('content'):
                token = csrf_meta.get('content')
                logger.debug(f"Found CSRF token: {token}")
                return token
            
            # If meta tag not found, look for it in scripts
            scripts = soup.find_all('script')
            for script in scripts:
                if script.string and 'csrf' in script.string.lower():
                    match = re.search(r'csrf[_-]token["\']?\s*:\s*["\']([^"\']+)["\']', script.string, re.IGNORECASE)
                    if match:
                        token = match.group(1)
                        logger.debug(f"Found CSRF token in script: {token}")
                        return token
            
            logger.warning("CSRF token not found, proceeding without it")
            return None
        except Exception as e:
            logger.error(f"Error getting CSRF token: {str(e)}")
            return None
    
    def simulate_search(self, state="TELANGANA", year=2024):
        """Simulate search results for testing purposes."""
        logger.info(f"Simulating search results for {state} in {year}")
        
        # Create sample proposal data
        proposals = []
        for i in range(1, 4):
            proposal_id = f"SIA/TG/INFRA2/51332/{year}"
            proposal = {
                "proposalNo": proposal_id,
                "state": state,
                "year": year,
                "projectName": f"Sample Project {i} for {state} in {year}",
                "proponentName": f"Sample Proponent {i}",
                "currentStatus": "ACCEPTED" if i % 2 == 0 else "PENDING",
                "clearanceType": "Environmental Clearance",
                "category": "B1",
                "sector": "Infrastructure",
                "issuingAuthority": "SEIAA",
                "submissionDate": f"{year}-01-{i:02d}"
            }
            proposals.append(proposal)
        
        return proposals
    
    def search_proposals(self, state_id, year, major_clearance_type=1, proposal_status=None, proposal_type=None):
        """Search for proposals using the advanced search feature with real API endpoint."""
        try:
            logger.info(f"Searching proposals for state_id: {state_id}, year: {year}")
            
            all_proposals = []
            page = 0  # The API appears to be 0-indexed for pages
            page_size = 30  # Default page size used by the website
            
            # Based on the API request you shared, the correct URL includes 'parivesh_api' not just 'parivesh_api'
            url = f"{self.parivesh_api}/trackYourProposal/advanceSearchData"
            
            # Log the base URL to verify it's correct
            logger.info(f"Base API URL: {self.parivesh_api}")
            
            while True:
                # Build query parameters
                params = {
                    "majorClearanceType": major_clearance_type,
                    "state": state_id,
                    "sector": "",
                    "proposalStatus": proposal_status or "",
                    "proposalType": proposal_type or "",
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
                    "page": page,
                    "size": page_size
                }
                
                logger.info(f"Making API request to page {page} with params: {params}")
                response = self.session.get(url, params=params, headers=self.headers, timeout=30)
                
                # Log the actual URL that was requested
                logger.info(f"Actual request URL: {response.url}")
                logger.info(f"Response status code: {response.status_code}")
                
                response.raise_for_status()
                
                try:
                    data = response.json()
                    
                    # Log the structure of the response to understand what we're getting
                    if isinstance(data, dict):
                        logger.info(f"Response keys: {list(data.keys())}")
                        if 'data' in data:
                            logger.info(f"Data type: {type(data['data'])}, length: {len(data['data']) if isinstance(data['data'], list) else 'not a list'}")
                    
                    if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                        proposals = data['data']
                        all_proposals.extend(proposals)
                        logger.info(f"Retrieved {len(proposals)} proposals from page {page}")
                        
                        # Log the first proposal to understand its structure
                        if proposals and page == 0:
                            logger.info(f"First proposal structure: {list(proposals[0].keys()) if isinstance(proposals[0], dict) else 'not a dict'}")
                        
                        # Check if we've reached the end of the results
                        if len(proposals) < page_size:
                            logger.info(f"Received fewer proposals ({len(proposals)}) than page size ({page_size}), ending pagination")
                            break
                        
                        # Move to the next page
                        page += 1
                        
                        # Add a small delay to avoid overwhelming the server
                        time.sleep(0.5)
                    else:
                        # No more data available
                        if isinstance(data, dict):
                            logger.warning(f"No data found in API response for page {page}")
                        else:
                            logger.warning(f"Unexpected response format for page {page}")
                        break
                except Exception as json_error:
                    logger.error(f"Error parsing JSON response: {str(json_error)}")
                    logger.error(f"Response content: {response.text[:500]}...")
                    break
            
            logger.info(f"Found a total of {len(all_proposals)} proposals for state_id {state_id} in {year}")
            return all_proposals
            
        except Exception as e:
            logger.error(f"Error searching proposals for state_id {state_id} in {year}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_proposal_details(self, proposal_no):
        """Get detailed information for a specific proposal using real API endpoint."""
        try:
            logger.info(f"Getting details for proposal: {proposal_no}")
            
            url = f"{self.parivesh_api}/trackYourProposal/dataOfProposalNo?proposalNo={proposal_no}"
            
            logger.info(f"Making API request to: {url}")
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get('status') == 200 and 'data' in data:
                proposal_details = data['data']
                logger.info(f"Successfully retrieved details for proposal {proposal_no}")
                return proposal_details
            else:
                logger.warning(f"No data found in API response for proposal {proposal_no}")
                return None
            
        except Exception as e:
            logger.error(f"Error getting details for proposal {proposal_no}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def get_proposal_timelines(self, proposal_id):
        """Get timeline information for a proposal."""
        try:
            logger.info(f"Getting timeline information for proposal: {proposal_id}")
            url = f"{self.parivesh_api}/trackYourProposal/getApprovalDates?proposalNo={proposal_id}"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                # Check if the response contains timeline data
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"Retrieved {len(data)} timeline entries for proposal {proposal_id}")
                    
                    # Format the timeline data
                    timelines = []
                    for entry in data:
                        timeline = {
                            'date': entry.get('approvalDate', ''),
                            'status': entry.get('status', ''),
                            'remarks': entry.get('remarks', '')
                        }
                        timelines.append(timeline)
                    
                    return timelines
                else:
                    logger.warning(f"No timeline data found for proposal {proposal_id}")
                    return []
            else:
                logger.error(f"Failed to get timeline information for proposal {proposal_id}: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting timeline information for proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_project_location(self, proposal_no):
        """Get KML file for project location using real API endpoint."""
        try:
            logger.info(f"Getting project location for proposal: {proposal_no}")
            
            # First, get the proposal details to extract the form_id
            proposal_details = self.get_proposal_details(proposal_no)
            if not proposal_details:
                logger.warning(f"Could not get proposal details for {proposal_no}")
                return None
            
            form_id = proposal_details.get('form_id')
            if not form_id:
                logger.warning(f"No form_id found in proposal details for {proposal_no}")
                return None
            
            # Now get the KML data
            url = f"{self.parivesh_api}/trackYourProposal/getKmlFile?formId={form_id}"
            
            logger.info(f"Making API request to: {url}")
            response = self.session.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('status') == 200 and 'data' in data:
                        kml_data = data['data']
                        logger.info(f"Successfully retrieved KML data for proposal {proposal_no}")
                        return kml_data
                    else:
                        logger.warning(f"No KML data found in API response for proposal {proposal_no}")
                        return None
                except ValueError:
                    # If the response is not JSON, it might be the KML file directly
                    logger.info(f"Retrieved KML file directly for proposal {proposal_no}")
                    return response.text
            else:
                logger.warning(f"Failed to get KML data for proposal {proposal_no}: {response.status_code}")
                return None
            
        except Exception as e:
            logger.error(f"Error getting project location for proposal {proposal_no}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def get_proposal_forms(self, proposal_id):
        """Get forms for a proposal."""
        try:
            logger.info(f"Getting forms for proposal: {proposal_id}")
            forms = {}
            
            # Get CAF form
            caf_url = f"{self.parivesh_api}/trackYourProposal/getCaFormDetails?proposalNo={proposal_id}"
            caf_response = self.session.get(caf_url, headers=self.headers, timeout=30)
            if caf_response.status_code == 200:
                caf_data = caf_response.json()
                if caf_data:
                    forms['CAF'] = caf_data
                    logger.info(f"Retrieved CAF form for proposal {proposal_id}")
            
            # Get Part A form
            part_a_url = f"{self.parivesh_api}/trackYourProposal/getPartADetails?proposalNo={proposal_id}"
            part_a_response = self.session.get(part_a_url, headers=self.headers, timeout=30)
            if part_a_response.status_code == 200:
                part_a_data = part_a_response.json()
                if part_a_data:
                    forms['Part_A'] = part_a_data
                    logger.info(f"Retrieved Part A form for proposal {proposal_id}")
            
            # Get Part B form
            part_b_url = f"{self.parivesh_api}/trackYourProposal/getPartBDetails?proposalNo={proposal_id}"
            part_b_response = self.session.get(part_b_url, headers=self.headers, timeout=30)
            if part_b_response.status_code == 200:
                part_b_data = part_b_response.json()
                if part_b_data:
                    forms['Part_B'] = part_b_data
                    logger.info(f"Retrieved Part B form for proposal {proposal_id}")
            
            # Get Part C form
            part_c_url = f"{self.parivesh_api}/trackYourProposal/getPartCDetails?proposalNo={proposal_id}"
            part_c_response = self.session.get(part_c_url, headers=self.headers, timeout=30)
            if part_c_response.status_code == 200:
                part_c_data = part_c_response.json()
                if part_c_data:
                    forms['Part_C'] = part_c_data
                    logger.info(f"Retrieved Part C form for proposal {proposal_id}")
            
            if forms:
                logger.info(f"Retrieved {len(forms)} forms for proposal {proposal_id}")
                return forms
            else:
                logger.warning(f"No forms found for proposal {proposal_id}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting forms for proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {}
    
    def get_documents(self, proposal_id):
        """Get documents for a proposal."""
        try:
            logger.info(f"Getting documents for proposal: {proposal_id}")
            url = f"{self.parivesh_api}/trackYourProposal/getDocuments?proposalNo={proposal_id}"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                
                # Check if the response contains document data
                if isinstance(data, list) and len(data) > 0:
                    logger.info(f"Retrieved {len(data)} documents for proposal {proposal_id}")
                    
                    # Format the document data
                    documents = []
                    for doc in data:
                        document = {
                            'name': doc.get('documentName', ''),
                            'link': doc.get('documentPath', '')
                        }
                        documents.append(document)
                    
                    return documents
                else:
                    logger.warning(f"No document data found for proposal {proposal_id}")
                    return []
            else:
                logger.error(f"Failed to get documents for proposal {proposal_id}: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting documents for proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def extract_sw_no(self, proposal_no):
        """Extract Single Window Number from proposal number."""
        match = re.search(r'SW/(\d+)/\d+', proposal_no)
        if match:
            return f"SW/{match.group(1)}"
        return ""
    
    def process_proposal(self, proposal_data):
        """Process a proposal by inserting or updating in the database."""
        try:
            logger.info(f"Processing proposal: {proposal_data.get('proposalNo')}")
            
            # Extract data from the proposal
            proposal_id = proposal_data.get('proposalNo')
            if not proposal_id:
                logger.warning("Proposal ID is missing, skipping")
                return False
            
            # Extract other fields
            sw_no = proposal_data.get('singleWindowNumber')
            state = proposal_data.get('state')
            year = proposal_id.split('/')[-1] if '/' in proposal_id else None
            project_name = proposal_data.get('projectName')
            current_status = proposal_data.get('proposalStatus')
            last_updated = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Connect to the database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if the proposal already exists
            cursor.execute("SELECT proposal_id, current_status FROM proposals WHERE proposal_id = ?", (proposal_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Proposal exists, check if status has changed
                if existing[1] != current_status:
                    logger.info(f"Status changed for proposal {proposal_id}: {existing[1]} -> {current_status}")
                    
                    # Update the proposal
                    cursor.execute('''
                    UPDATE proposals 
                    SET current_status = ?, last_updated = ? 
                    WHERE proposal_id = ?''', 
                    (current_status, last_updated, proposal_id))
                    
                    # Process additional data for the updated proposal
                    self.process_proposal_details(cursor, proposal_id, proposal_data)
                    self.get_and_process_additional_data(cursor, proposal_id)
                else:
                    logger.info(f"No status change for proposal {proposal_id}, skipping")
            else:
                # New proposal, insert it
                logger.info(f"New proposal found: {proposal_id}")
                
                cursor.execute('''
                INSERT INTO proposals (proposal_id, s_w_no, state, year, proposal_title, current_status, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)''',
                (proposal_id, sw_no, state, year, project_name, current_status, last_updated))
                
                # Process additional data for the new proposal
                self.process_proposal_details(cursor, proposal_id, proposal_data)
                self.get_and_process_additional_data(cursor, proposal_id)
            
            conn.commit()
            conn.close()
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing proposal {proposal_data.get('proposalNo')}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def process_proposal_details(self, cursor, proposal_id, proposal_data):
        """Process and store detailed proposal information."""
        try:
            logger.info(f"Processing details for proposal: {proposal_id}")
            
            # Extract data from the proposal
            clearance_type = proposal_data.get('clearanceType', '')
            project_name = proposal_data.get('projectName', '')
            proponent = proposal_data.get('nameOfUserAgency', '')
            category = proposal_data.get('category', '')
            sector = proposal_data.get('sector', '')
            issuing_authority = proposal_data.get('issuing_authority', '')
            submission_date = proposal_data.get('dateOfSubmission', '')
            caf_no = proposal_data.get('cafnumber', '')
            single_window_no = proposal_data.get('singleWindowNumber', '')
            
            # For project location, we'll use the state for now
            project_location = proposal_data.get('state', '')
            
            # Check if details already exist
            cursor.execute("SELECT proposal_id FROM proposal_details WHERE proposal_id = ?", (proposal_id,))
            existing = cursor.fetchone()
            
            if existing:
                # Update existing details
                cursor.execute('''
                UPDATE proposal_details 
                SET clearance_type = ?, project_name = ?, project_location = ?, 
                    proponent = ?, category = ?, sector = ?, issuing_authority = ?, 
                    submission_date = ?, caf_no = ?, single_window_no = ?
                WHERE proposal_id = ?''', 
                (clearance_type, project_name, project_location, proponent, 
                 category, sector, issuing_authority, submission_date, 
                 caf_no, single_window_no, proposal_id))
            else:
                # Insert new details
                cursor.execute('''
                INSERT INTO proposal_details 
                (proposal_id, clearance_type, project_name, project_location, 
                 proponent, category, sector, issuing_authority, 
                 submission_date, caf_no, single_window_no)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (proposal_id, clearance_type, project_name, project_location, 
                 proponent, category, sector, issuing_authority, 
                 submission_date, caf_no, single_window_no))
            
            logger.info(f"Successfully processed details for proposal {proposal_id}")
            
        except Exception as e:
            logger.error(f"Error processing details for proposal {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def get_and_process_additional_data(self, cursor, proposal_id):
        """Get and process additional data for a proposal."""
        try:
            logger.info(f"Getting additional data for proposal: {proposal_id}")
            
            # Get timeline information
            timelines = self.get_proposal_timelines(proposal_id)
            if timelines:
                logger.info(f"Processing {len(timelines)} timeline entries for {proposal_id}")
                for timeline in timelines:
                    cursor.execute('''
                    INSERT INTO proposal_timelines (proposal_id, date, status, remarks)
                    VALUES (?, ?, ?, ?)''',
                    (proposal_id,
                     timeline.get('date', ''),
                     timeline.get('status', ''),
                     timeline.get('remarks', '')))
            
            # Get project location (KML data)
            kml_data = self.get_project_location(proposal_id)
            if kml_data:
                logger.info(f"Processing project location for {proposal_id}")
                cursor.execute('''
                INSERT INTO project_locations (proposal_id, kml_data)
                VALUES (?, ?)''',
                (proposal_id, json.dumps(kml_data) if isinstance(kml_data, dict) else kml_data))
            
            # Get forms data
            forms = self.get_proposal_forms(proposal_id)
            if forms:
                logger.info(f"Processing {len(forms)} forms for {proposal_id}")
                for form_type, form_data in forms.items():
                    cursor.execute('''
                    INSERT INTO proposal_forms (proposal_id, form_type, form_data)
                    VALUES (?, ?, ?)''',
                    (proposal_id, form_type, json.dumps(form_data)))
            
            # Get documents
            documents = self.get_documents(proposal_id)
            if documents:
                logger.info(f"Processing {len(documents)} documents for {proposal_id}")
                for doc in documents:
                    cursor.execute('''
                    INSERT INTO documents (proposal_id, document_name, document_link)
                    VALUES (?, ?, ?)''',
                    (proposal_id, doc.get('name', ''), doc.get('link', '')))
            
            logger.info(f"Successfully processed additional data for proposal {proposal_id}")
            
        except Exception as e:
            logger.error(f"Error processing additional data for {proposal_id}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
    
    def get_status_list(self, workgroup_id=1):
        """Get the list of possible status values from the API."""
        try:
            logger.info("Getting list of status values")
            url = f"{self.parivesh_api}/trackYourProposal/getListOfStatus?workgroupId={workgroup_id}"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 200 and 'data' in data:
                statuses = [status.get('name') for status in data.get('data', [])]
                logger.info(f"Found {len(statuses)} status values: {', '.join(statuses)}")
                return statuses
            else:
                logger.warning(f"Unexpected response format for status list: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting status list: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_proposal_types(self, clearance_type_id=1):
        """Get the list of proposal types based on clearance type."""
        try:
            logger.info(f"Getting proposal types for clearance type ID: {clearance_type_id}")
            url = f"{self.parivesh_api}/trackYourProposal/getProposalTypeOnBasesOfClearanceType?id={clearance_type_id}"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 200 and 'data' in data:
                proposal_types = []
                for pt in data.get('data', []):
                    proposal_types.append({
                        'id': pt.get('id'),
                        'name': pt.get('name')
                    })
                logger.info(f"Found {len(proposal_types)} proposal types")
                return proposal_types
            else:
                logger.warning(f"Unexpected response format for proposal types: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting proposal types: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_states(self):
        """Get the list of all states from the API."""
        try:
            logger.info("Getting list of all states")
            url = f"{self.parivesh_api}/trackYourProposal/getListOfAllState"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 200 and 'data' in data:
                states = []
                for state in data.get('data', []):
                    if state.get('is_active') and not state.get('is_deleted'):
                        states.append({
                            'id': state.get('id'),
                            'name': state.get('name'),
                            'code': state.get('code'),
                            'abbr': state.get('state_abbr')
                        })
                logger.info(f"Found {len(states)} active states")
                return states
            else:
                logger.warning(f"Unexpected response format for states list: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting states list: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_clearance_types(self):
        """Get the list of all clearance types."""
        try:
            logger.info("Getting list of all clearance types")
            url = f"{self.parivesh_api}/trackYourProposal/getUGCStatus"
            
            response = self.session.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Filter for active clearance types
            clearance_types = [ct for ct in data if ct.get('isActive', True)]
            
            logger.info(f"Found {len(clearance_types)} clearance types")
            return clearance_types
            
        except Exception as e:
            logger.error(f"Error getting clearance types: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def scrape_proposals(self, state_id, years=None):
        """Scrape proposals for a specific state and years."""
        if years is None:
            current_year = datetime.now().year
            years = list(range(current_year - 5, current_year + 1))
        
        logger.info(f"Starting scraping for state_id: {state_id}, years: {years}")
        
        total_proposals = 0
        
        for year in years:
            logger.info(f"Scraping proposals for year: {year}")
            
            # Search for proposals
            proposals = self.search_proposals(state_id, year)
            
            if proposals:
                logger.info(f"Found {len(proposals)} proposals for state_id {state_id} in {year}")
                
                # Process each proposal
                for proposal in proposals:
                    self.process_proposal(proposal)
                    self.random_delay()  # Add delay to avoid overloading the server
                    total_proposals += 1
            else:
                logger.warning(f"No proposals found for state_id {state_id} in {year}")
        
        logger.info(f"Scraping completed. Processed {total_proposals} proposals.")
        return total_proposals
