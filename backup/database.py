import sqlite3
from datetime import datetime
import os

class Database:
    def __init__(self, db_path='parivesh_data.db'):
        """Initialize the database connection."""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.init_db()

    def connect(self):
        """Connect to the database."""
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        return self.conn, self.cursor

    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

    def init_db(self):
        """Initialize the database schema."""
        conn, c = self.connect()
        
        # Level 1 Table: proposals (Basic proposal info)
        c.execute('''CREATE TABLE IF NOT EXISTS proposals (
                    proposal_id TEXT PRIMARY KEY,
                    s_w_no TEXT,
                    state TEXT,
                    year INTEGER,
                    proposal_title TEXT,
                    current_status TEXT,
                    last_updated DATETIME
                )''')
        
        # Level 2a Table: proposal_details (Detailed proposal info)
        c.execute('''CREATE TABLE IF NOT EXISTS proposal_details (
                    proposal_id TEXT PRIMARY KEY,
                    title TEXT,
                    proponent TEXT,
                    category TEXT,
                    sector TEXT,
                    proposal_for TEXT,
                    date_submitted DATETIME,
                    FOREIGN KEY(proposal_id) REFERENCES proposals(proposal_id)
                )''')
        
        # Level 2a Table: proposal_timelines (Timeline and status updates)
        c.execute('''CREATE TABLE IF NOT EXISTS proposal_timelines (
                    timeline_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id TEXT,
                    date DATETIME,
                    status TEXT,
                    remarks TEXT,
                    FOREIGN KEY(proposal_id) REFERENCES proposals(proposal_id)
                )''')
        
        # Level 2b Table: project_locations (KML file data)
        c.execute('''CREATE TABLE IF NOT EXISTS project_locations (
                    location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id TEXT,
                    kml_file_url TEXT,
                    kml_file_content TEXT,
                    FOREIGN KEY(proposal_id) REFERENCES proposals(proposal_id)
                )''')
        
        # Level 2b Table: proposal_forms (CAF, Part A, Part B, Part C)
        c.execute('''CREATE TABLE IF NOT EXISTS proposal_forms (
                    form_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id TEXT,
                    form_type TEXT,
                    form_content TEXT,
                    FOREIGN KEY(proposal_id) REFERENCES proposals(proposal_id)
                )''')
        
        # Level 2b Table: documents (Uploaded documents)
        c.execute('''CREATE TABLE IF NOT EXISTS documents (
                    doc_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    proposal_id TEXT,
                    doc_name TEXT,
                    doc_url TEXT,
                    FOREIGN KEY(proposal_id) REFERENCES proposals(proposal_id)
                )''')
        
        conn.commit()
        self.close()

    def insert_proposal(self, proposal_data):
        """Insert or update a proposal in the proposals table."""
        conn, c = self.connect()
        
        # Check if proposal exists
        c.execute("SELECT proposal_id, current_status FROM proposals WHERE proposal_id = ?", 
                 (proposal_data['proposal_id'],))
        existing = c.fetchone()
        
        status_changed = False
        if not existing:
            # New proposal
            c.execute('''INSERT INTO proposals 
                        (proposal_id, s_w_no, state, year, proposal_title, current_status, last_updated) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)''',
                     (proposal_data['proposal_id'], 
                      proposal_data.get('s_w_no', ''),
                      proposal_data.get('state', ''),
                      proposal_data.get('year', 0),
                      proposal_data.get('proposal_title', ''),
                      proposal_data.get('current_status', ''),
                      datetime.now()))
        elif existing[1] != proposal_data.get('current_status', ''):
            # Status changed
            status_changed = True
            c.execute('''UPDATE proposals 
                        SET current_status = ?, last_updated = ? 
                        WHERE proposal_id = ?''',
                     (proposal_data.get('current_status', ''),
                      datetime.now(),
                      proposal_data['proposal_id']))
            
            # Add to timeline
            if proposal_data.get('current_status'):
                c.execute('''INSERT INTO proposal_timelines 
                            (proposal_id, date, status) 
                            VALUES (?, ?, ?)''',
                         (proposal_data['proposal_id'],
                          datetime.now(),
                          proposal_data.get('current_status', '')))
        
        conn.commit()
        self.close()
        return status_changed

    def insert_proposal_details(self, details_data):
        """Insert or update proposal details."""
        conn, c = self.connect()
        
        c.execute('''INSERT OR REPLACE INTO proposal_details 
                    (proposal_id, title, proponent, category, sector, proposal_for, date_submitted) 
                    VALUES (?, ?, ?, ?, ?, ?, ?)''',
                 (details_data['proposal_id'],
                  details_data.get('title', ''),
                  details_data.get('proponent', ''),
                  details_data.get('category', ''),
                  details_data.get('sector', ''),
                  details_data.get('proposal_for', ''),
                  details_data.get('date_submitted', None)))
        
        conn.commit()
        self.close()

    def insert_timeline(self, timeline_data):
        """Insert a timeline entry."""
        conn, c = self.connect()
        
        c.execute('''INSERT INTO proposal_timelines 
                    (proposal_id, date, status, remarks) 
                    VALUES (?, ?, ?, ?)''',
                 (timeline_data['proposal_id'],
                  timeline_data.get('date', datetime.now()),
                  timeline_data.get('status', ''),
                  timeline_data.get('remarks', '')))
        
        conn.commit()
        self.close()

    def insert_project_location(self, location_data):
        """Insert project location data."""
        conn, c = self.connect()
        
        c.execute('''INSERT INTO project_locations 
                    (proposal_id, kml_file_url, kml_file_content) 
                    VALUES (?, ?, ?)''',
                 (location_data['proposal_id'],
                  location_data.get('kml_file_url', ''),
                  location_data.get('kml_file_content', '')))
        
        conn.commit()
        self.close()

    def insert_form(self, form_data):
        """Insert form data (CAF, Part A, B, C)."""
        conn, c = self.connect()
        
        c.execute('''INSERT INTO proposal_forms 
                    (proposal_id, form_type, form_content) 
                    VALUES (?, ?, ?)''',
                 (form_data['proposal_id'],
                  form_data.get('form_type', ''),
                  form_data.get('form_content', '')))
        
        conn.commit()
        self.close()

    def insert_document(self, doc_data):
        """Insert document data."""
        conn, c = self.connect()
        
        c.execute('''INSERT INTO documents 
                    (proposal_id, doc_name, doc_url) 
                    VALUES (?, ?, ?)''',
                 (doc_data['proposal_id'],
                  doc_data.get('doc_name', ''),
                  doc_data.get('doc_url', '')))
        
        conn.commit()
        self.close()

    def get_proposal_ids(self):
        """Get all proposal IDs from the database."""
        conn, c = self.connect()
        
        c.execute("SELECT proposal_id FROM proposals")
        proposal_ids = [row[0] for row in c.fetchall()]
        
        self.close()
        return proposal_ids
