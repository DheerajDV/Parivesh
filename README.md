# Parivesh Web Scraper

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
