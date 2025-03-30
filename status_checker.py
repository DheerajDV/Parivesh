import sqlite3
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
