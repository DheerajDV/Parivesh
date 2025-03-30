import sqlite3
import os
from tabulate import tabulate

def show_proposals(db_path):
    """Show the proposals table in a nicely formatted way."""
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get column names for proposals table
    cursor.execute("PRAGMA table_info(proposals)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]
    
    # Get proposal data
    cursor.execute("SELECT * FROM proposals")
    rows = cursor.fetchall()
    
    # Print using tabulate for better formatting
    if rows:
        print(tabulate(rows, headers=column_names, tablefmt="grid"))
        print(f"\nTotal proposals: {len(rows)}")
    else:
        print("No proposals found in the database.")
    
    conn.close()

if __name__ == "__main__":
    db_path = "parivesh_data.db"
    show_proposals(db_path)
