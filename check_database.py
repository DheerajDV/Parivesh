import sqlite3
import sys
import os
import json
from datetime import datetime

def check_database(db_path):
    """Check the database and print statistics about the proposals."""
    if not os.path.exists(db_path):
        print(f"Database file {db_path} does not exist.")
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
        
        # Get column names from proposals table
        cursor.execute("PRAGMA table_info(proposals)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Get proposals by year
        if 'proposal_id' in columns:
            cursor.execute("SELECT SUBSTR(proposal_id, -4) as year, COUNT(*) FROM proposals GROUP BY year ORDER BY year DESC")
            proposals_by_year = cursor.fetchall()
            print("\nProposals by year:")
            for year, count in proposals_by_year:
                print(f"  {year}: {count}")
        
        # Get proposals by status
        if 'current_status' in columns:
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
        id_column = 'proposal_id' if 'proposal_id' in columns else 'proposalNo'
        cursor.execute(f"SELECT {id_column}, current_status FROM proposals LIMIT 1")
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

if __name__ == "__main__":
    db_path = "parivesh.db"
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    check_database(db_path)
