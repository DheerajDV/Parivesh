import json
import sqlite3
import logging
import os
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def create_tables(conn):
    """Create the necessary tables if they don't exist."""
    cursor = conn.cursor()
    
    # Create proposals table (Level 1)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT UNIQUE,
        projectName TEXT,
        companyName TEXT,
        stateName TEXT,
        categoryName TEXT,
        sectorName TEXT,
        proposalStatus TEXT,
        proposalType TEXT,
        clearanceTypeName TEXT,
        swNo TEXT,
        lastSubmissionDate TEXT,
        lastStatusDate TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create proposal_details table (Level 2a)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_details (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT UNIQUE,
        proposal_json TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposalNo) REFERENCES proposals (proposalNo)
    )
    ''')
    
    # Create proposal_timelines table (Level 2a)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_timelines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT,
        status TEXT,
        date TEXT,
        remarks TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposalNo) REFERENCES proposals (proposalNo)
    )
    ''')
    
    # Create project_locations table (Level 2b)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS project_locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT,
        location_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposalNo) REFERENCES proposals (proposalNo)
    )
    ''')
    
    # Create proposal_forms table (Level 2b)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS proposal_forms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT,
        form_type TEXT,
        form_data TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposalNo) REFERENCES proposals (proposalNo)
    )
    ''')
    
    # Create documents table (Level 2b)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        proposalNo TEXT,
        document_type TEXT,
        document_name TEXT,
        document_url TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (proposalNo) REFERENCES proposals (proposalNo)
    )
    ''')
    
    conn.commit()
    logger.info("Database tables created successfully")

def process_proposals(json_file, db_path):
    """Process proposals from JSON file and store in database."""
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
        
        # Create tables if they don't exist
        create_tables(conn)
        
        # Process each proposal
        cursor = conn.cursor()
        processed_count = 0
        
        for i, proposal in enumerate(proposals):
            try:
                proposal_no = proposal.get('proposalNo')
                if not proposal_no:
                    logger.warning(f"Proposal at index {i} has no proposalNo, skipping")
                    continue
                
                logger.info(f"Processing proposal {i+1}/{len(proposals)}: {proposal_no}")
                
                # Check if proposal already exists
                cursor.execute("SELECT id FROM proposals WHERE proposalNo = ?", (proposal_no,))
                result = cursor.fetchone()
                
                if result:
                    # Update existing proposal
                    cursor.execute('''
                    UPDATE proposals SET
                        projectName = ?,
                        companyName = ?,
                        stateName = ?,
                        categoryName = ?,
                        sectorName = ?,
                        proposalStatus = ?,
                        proposalType = ?,
                        clearanceTypeName = ?,
                        swNo = ?,
                        lastSubmissionDate = ?,
                        lastStatusDate = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE proposalNo = ?
                    ''', (
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
                        proposal_no
                    ))
                else:
                    # Insert new proposal
                    cursor.execute('''
                    INSERT INTO proposals (
                        proposalNo,
                        projectName,
                        companyName,
                        stateName,
                        categoryName,
                        sectorName,
                        proposalStatus,
                        proposalType,
                        clearanceTypeName,
                        swNo,
                        lastSubmissionDate,
                        lastStatusDate
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        proposal_no,
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
                cursor.execute("SELECT id FROM proposal_details WHERE proposalNo = ?", (proposal_no,))
                result = cursor.fetchone()
                
                if result:
                    # Update existing details
                    cursor.execute('''
                    UPDATE proposal_details SET
                        proposal_json = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE proposalNo = ?
                    ''', (json.dumps(proposal), proposal_no))
                else:
                    # Insert new details
                    cursor.execute('''
                    INSERT INTO proposal_details (
                        proposalNo,
                        proposal_json
                    ) VALUES (?, ?)
                    ''', (proposal_no, json.dumps(proposal)))
                
                processed_count += 1
                
                # Commit every 10 proposals
                if processed_count % 10 == 0:
                    conn.commit()
                    logger.info(f"Committed {processed_count} proposals to database")
            
            except Exception as e:
                logger.error(f"Error processing proposal {proposal.get('proposalNo', 'Unknown')}: {str(e)}")
        
        # Final commit
        conn.commit()
        
        # Close database connection
        conn.close()
        
        logger.info(f"Processed {processed_count} out of {len(proposals)} proposals")
        return True
    
    except Exception as e:
        logger.error(f"Error processing proposals: {str(e)}")
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
        
        # Get table counts
        tables = [
            "proposals",
            "proposal_details",
            "proposal_timelines",
            "project_locations",
            "proposal_forms",
            "documents"
        ]
        
        print("Database statistics:")
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"  {table}: {count} rows")
            except sqlite3.OperationalError:
                print(f"  {table}: Table not found")
        
        # Get column names from proposals table
        cursor.execute("PRAGMA table_info(proposals)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Get proposal status counts if the column exists
        status_column = None
        for possible_column in ['proposalStatus', 'proposal_status', 'status']:
            if possible_column in columns:
                status_column = possible_column
                break
        
        if status_column:
            cursor.execute(f"SELECT {status_column}, COUNT(*) FROM proposals GROUP BY {status_column} ORDER BY COUNT(*) DESC")
            status_counts = cursor.fetchall()
            
            print("\nProposal status counts:")
            for status, count in status_counts:
                print(f"  {status}: {count}")
        else:
            print("\nCould not find status column in proposals table")
            print("Available columns:", columns)
        
        conn.close()
    
    except Exception as e:
        logger.error(f"Error checking database: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    json_file = "telangana_2024_all_proposals.json"
    db_path = "parivesh.db"
    
    logger.info("Starting data processing...")
    
    if process_proposals(json_file, db_path):
        logger.info("Data processing completed successfully")
        check_database(db_path)
    else:
        logger.error("Data processing failed")
