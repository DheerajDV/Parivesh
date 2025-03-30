import sqlite3
import os
import sys
import datetime

def generate_html_table(cursor, table_name):
    """Generate HTML table for the given table name."""
    # Get column names
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    
    # Get data
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 100")
    rows = cursor.fetchall()
    
    # Generate HTML
    html = f"""
    <h2>{table_name} ({len(rows)} rows shown, limit 100)</h2>
    <div class="table-responsive">
        <table class="table table-striped table-bordered">
            <thead>
                <tr>
                    {''.join(f'<th>{col}</th>' for col in columns)}
                </tr>
            </thead>
            <tbody>
    """
    
    for row in rows:
        html += "<tr>"
        for value in row:
            # Handle different data types
            if isinstance(value, (dict, list)):
                cell_value = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
            elif value is None:
                cell_value = "<em>NULL</em>"
            else:
                cell_value = str(value)
                if len(cell_value) > 100:
                    cell_value = cell_value[:100] + "..."
            
            html += f"<td>{cell_value}</td>"
        html += "</tr>"
    
    html += """
            </tbody>
        </table>
    </div>
    """
    
    return html

def generate_database_stats(cursor):
    """Generate database statistics."""
    # Get table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    stats = "<h2>Database Statistics</h2><ul>"
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        stats += f"<li><strong>{table}</strong>: {count} rows</li>"
    
    stats += "</ul>"
    return stats

def export_database_to_html(db_path, output_path=None):
    """Export SQLite database to HTML file."""
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.")
        return False
    
    if output_path is None:
        output_path = f"database_view_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Generate HTML
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Parivesh Database Viewer</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                body { padding: 20px; }
                h1 { margin-bottom: 30px; }
                h2 { margin-top: 30px; color: #0d6efd; }
                .table-responsive { margin-bottom: 40px; }
                .navbar { margin-bottom: 20px; }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Parivesh Database Viewer</h1>
                <p>Generated on: """ + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + """</p>
                
                <nav class="navbar navbar-expand-lg navbar-light bg-light">
                    <div class="container-fluid">
                        <span class="navbar-brand">Navigation</span>
                        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
                            <span class="navbar-toggler-icon"></span>
                        </button>
                        <div class="collapse navbar-collapse" id="navbarNav">
                            <ul class="navbar-nav">
                                <li class="nav-item">
                                    <a class="nav-link" href="#stats">Statistics</a>
                                </li>
        """
        
        for table in tables:
            html += f"""
                                <li class="nav-item">
                                    <a class="nav-link" href="#{table}">{table}</a>
                                </li>
            """
        
        html += """
                            </ul>
                        </div>
                    </div>
                </nav>
                
                <div id="stats">
        """
        
        # Add database statistics
        html += generate_database_stats(cursor)
        
        # Add tables
        for table in tables:
            html += f'<div id="{table}">'
            html += generate_html_table(cursor, table)
            html += '</div>'
        
        html += """
                </div>
            </div>
            
            <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
        </body>
        </html>
        """
        
        # Write HTML to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        conn.close()
        
        print(f"Database exported to '{output_path}' successfully.")
        return True
    
    except Exception as e:
        print(f"Error exporting database: {str(e)}")
        return False

if __name__ == "__main__":
    db_path = "parivesh.db"
    output_path = "parivesh_database_view.html"
    
    if len(sys.argv) > 1:
        db_path = sys.argv[1]
    
    if len(sys.argv) > 2:
        output_path = sys.argv[2]
    
    export_database_to_html(db_path, output_path)
