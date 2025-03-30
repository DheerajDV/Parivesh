import requests
import json
import logging
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def scrape_telangana_2024():
    """Scrape all proposals for Telangana in 2024."""
    try:
        # API base URL
        base_url = "https://parivesh.nic.in"
        api_url = f"{base_url}/parivesh_api"
        
        # Headers that mimic a browser
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": "https://parivesh.nic.in",
            "Referer": "https://parivesh.nic.in/",
            "Connection": "keep-alive"
        }
        
        # Session to maintain cookies
        session = requests.Session()
        
        # Visit the main page to get cookies
        logger.info("Visiting main page to get cookies...")
        session.get(base_url, headers=headers)
        
        # Parameters for Telangana in 2024
        state_id = 36  # Telangana state ID
        year = 2024
        
        # Search for proposals
        all_proposals = []
        page = 0
        page_size = 30
        total_expected_proposals = 1071
        
        while len(all_proposals) < total_expected_proposals:
            logger.info(f"Retrieving page {page}...")
            
            # Construct URL with query parameters
            url = f"{api_url}/trackYourProposal/advanceSearchData"
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
                "page": page,
                "size": page_size
            }
            
            # Implement retry mechanism
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    response = session.get(url, params=params, headers=headers, timeout=60)
                    response.raise_for_status()
                    success = True
                except requests.exceptions.Timeout:
                    retry_count += 1
                    logger.warning(f"Request timed out. Retry {retry_count}/{max_retries}")
                    time.sleep(2)
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    logger.warning(f"Request failed: {str(e)}. Retry {retry_count}/{max_retries}")
                    time.sleep(2)
            
            if not success:
                logger.error(f"Failed to retrieve data after {max_retries} retries")
                break
            
            # Log the actual URL that was requested
            logger.info(f"Requested URL: {response.url}")
            
            try:
                data = response.json()
                
                # Save the raw response for debugging
                with open(f"telangana_2024_page_{page}.json", "w") as f:
                    json.dump(data, f, indent=2)
                
                if isinstance(data, dict) and 'data' in data and isinstance(data['data'], list):
                    proposals = data['data']
                    logger.info(f"Retrieved {len(proposals)} proposals from page {page}")
                    all_proposals.extend(proposals)
                    
                    # If we got fewer proposals than the page size, we've reached the end
                    if len(proposals) < page_size:
                        logger.info(f"Reached end of results on page {page}")
                        break
                    
                    # Move to the next page
                    page += 1
                    
                    # Add a delay to avoid overwhelming the server
                    time.sleep(1)
                else:
                    logger.warning(f"Unexpected response format for page {page}")
                    break
            except Exception as e:
                logger.error(f"Error processing page {page}: {str(e)}")
                break
        
        # Save all proposals to a JSON file
        with open("telangana_2024_all_proposals.json", "w") as f:
            json.dump(all_proposals, f, indent=2)
        
        logger.info(f"Scraped a total of {len(all_proposals)} proposals for Telangana in 2024")
        
        # Print some basic statistics
        if all_proposals:
            status_counts = {}
            for proposal in all_proposals:
                status = proposal.get('proposalStatus', 'Unknown')
                status_counts[status] = status_counts.get(status, 0) + 1
            
            logger.info("Proposal status counts:")
            for status, count in status_counts.items():
                logger.info(f"  {status}: {count}")
        
        return all_proposals
    
    except Exception as e:
        logger.error(f"Error scraping proposals: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

if __name__ == "__main__":
    logger.info("Starting quick scraper for Telangana 2024...")
    proposals = scrape_telangana_2024()
    logger.info("Quick scraper completed.")
