import argparse
import logging
from datetime import datetime
import os

from scraper import PariveshScraper

def main():
    """Main function to run the scraper."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Parivesh Proposal Scraper')
    parser.add_argument('--state', default='TELANGANA', 
                        help='State to scrape (default: TELANGANA)')
    parser.add_argument('--years', nargs='+', type=int, 
                        help='Years to scrape (default: last 5 years)')
    parser.add_argument('--db-path', default='parivesh_data.db', 
                        help='Path to SQLite database file (default: parivesh_data.db)')
    parser.add_argument('--log-level', default='INFO', 
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Logging level (default: INFO)')
    parser.add_argument('--check-interval', type=int, default=24,
                        help='Interval in hours to check for new proposals (default: 24)')
    parser.add_argument('--all-clearance-types', action='store_true',
                        help='Search across all clearance types (default: False)')
    
    args = parser.parse_args()
    
    # Set up years if not provided
    if not args.years:
        current_year = datetime.now().year
        args.years = list(range(current_year - 5, current_year + 1))
    
    # Set up logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        filename='parivesh_scraper.log',
        filemode='a'
    )
    logger = logging.getLogger('parivesh_scraper')
    
    # Add console handler to see logs in console
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, args.log_level))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console.setFormatter(formatter)
    logger.addHandler(console)
    
    logger.info("Starting Parivesh Proposal Scraper")
    logger.info(f"State: {args.state}")
    logger.info(f"Years: {args.years}")
    logger.info(f"Database path: {args.db_path}")
    logger.info(f"All clearance types: {args.all_clearance_types}")
    
    # Initialize scraper
    scraper = PariveshScraper(args.db_path)
    
    # Get the state ID for the specified state
    states = scraper.get_states()
    state_id = None
    for state in states:
        if state['name'] == args.state:
            state_id = state['id']
            break
    
    if not state_id:
        logger.error(f"State '{args.state}' not found. Available states: {[s['name'] for s in states]}")
        return
    
    # Get clearance types
    clearance_types = [1]  # Default to Environmental Clearance (EC)
    if args.all_clearance_types:
        # Get all clearance types from the API
        clearance_types_data = scraper.get_clearance_types()
        if clearance_types_data:
            clearance_types = [ct['id'] for ct in clearance_types_data]
            logger.info(f"Using all clearance types: {clearance_types}")
    
    # Run scraper for each year and clearance type
    total_proposals = 0
    for year in args.years:
        for clearance_type in clearance_types:
            logger.info(f"Scraping proposals for {args.state} in {year} with clearance type {clearance_type}")
            proposals = scraper.search_proposals(state_id, year, major_clearance_type=clearance_type)
            
            if proposals:
                logger.info(f"Found {len(proposals)} proposals for {args.state} in {year} with clearance type {clearance_type}")
                
                # Process each proposal
                for proposal in proposals:
                    result = scraper.process_proposal(proposal)
                    if result:
                        total_proposals += 1
            else:
                logger.info(f"No proposals found for {args.state} in {year} with clearance type {clearance_type}")
    
    logger.info(f"Scraping completed successfully. Processed {total_proposals} proposals.")

if __name__ == "__main__":
    main()
