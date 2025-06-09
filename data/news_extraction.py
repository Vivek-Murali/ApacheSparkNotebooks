"""
News data extraction using Event Registry API with daily dynamic updates
"""
import os
import sys
from pathlib import Path
import requests
import json
import datetime
import logging

# Add project root to Python path
project_root = str(Path(__file__).parent.parent)
sys.path.append(project_root)

from eventregistry import *
from src.config.databricks_config import EVENT_REGISTRY_API_KEY, EVENT_TOKEN

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('news_extraction.log'),
        logging.StreamHandler()
    ]
)

# Define expected schema for Tinybird
REQUIRED_FIELDS = {
    'uri': str,
    'title': str,
    'body': str,
    'date': str,
    'source': dict,
    'extraction_timestamp': str
}

def validate_article_schema(article):
    """
    Validate article against required schema
    """
    try:
        # Check required fields
        for field, field_type in REQUIRED_FIELDS.items():
            if field not in article:
                return False, f"Missing required field: {field}"
            if not isinstance(article[field], field_type):
                return False, f"Invalid type for field {field}: expected {field_type}, got {type(article[field])}"
        
        # Additional validations
        if not article['uri'] or not article['title'] or not article['body']:
            return False, "Empty required fields"
            
        # Validate date format
        try:
            datetime.datetime.fromisoformat(article['date'].replace('Z', '+00:00'))
        except ValueError:
            return False, "Invalid date format"
            
        return True, "Valid"
    except Exception as e:
        return False, f"Validation error: {str(e)}"

def clean_article_data(article):
    """
    Clean and format article data before sending to Tinybird
    """
    try:
        # Ensure all required fields exist
        cleaned_article = {
            'uri': article.get('uri', ''),
            'title': article.get('title', ''),
            'body': article.get('body', ''),
            'date': article.get('date', ''),
            'source': article.get('source', {}),
            'extraction_timestamp': datetime.datetime.now().isoformat()
        }
        
        # Clean text fields
        cleaned_article['title'] = cleaned_article['title'].strip()
        cleaned_article['body'] = cleaned_article['body'].strip()
        
        # Ensure source is a dictionary with required fields
        if not isinstance(cleaned_article['source'], dict):
            cleaned_article['source'] = {'title': str(cleaned_article['source'])}
        
        # Add additional fields if available
        if 'concepts' in article:
            cleaned_article['concepts'] = article['concepts']
        if 'categories' in article:
            cleaned_article['categories'] = article['categories']
        if 'location' in article:
            cleaned_article['location'] = article['location']
            
        return cleaned_article
    except Exception as e:
        logging.error(f"Error cleaning article data: {str(e)}")
        return None

def get_date_range():
    """
    Get today's date and yesterday's date for the query range
    """
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    
    return yesterday.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')

def send_to_tinybird(article):
    """
    Send individual article to Tinybird
    """
    try:
        # Clean and validate article data
        cleaned_article = clean_article_data(article)
        if not cleaned_article:
            return False, "Failed to clean article data"
            
        # Validate schema
        is_valid, validation_message = validate_article_schema(cleaned_article)
        if not is_valid:
            return False, f"Schema validation failed: {validation_message}"
        
        data = json.dumps(cleaned_article)
        r = requests.post(
            'https://api.europe-west2.gcp.tinybird.co/v0/events',
            params={
                'name': 'news_data',
                'token': EVENT_TOKEN
            },
            data=data
        )
        
        # Tinybird returns 202 for successful ingestion
        if r.status_code in [200, 202]:
            response = r.json()
            if response.get('successful_rows', 0) > 0:
                logging.info(f"Successfully sent article {cleaned_article.get('uri', 'unknown')} to Tinybird")
                return True, "Success"
            else:
                error_msg = f"Article {cleaned_article.get('uri', 'unknown')} was quarantined by Tinybird"
                if 'quarantined_rows' in response:
                    error_msg += f" - {response.get('quarantined_rows', 0)} rows quarantined"
                logging.warning(error_msg)
                return False, error_msg
        else:
            error_msg = f"Failed to send article {cleaned_article.get('uri', 'unknown')} to Tinybird. Status code: {r.status_code}"
            logging.error(error_msg)
            logging.error(f"Response: {r.text}")
            return False, error_msg
    except Exception as e:
        error_msg = f"Error sending article to Tinybird: {str(e)}"
        logging.error(error_msg)
        return False, error_msg

def extract_news_data():
    """
    Extract news data using Event Registry API for the last 24 hours
    """
    try:
        # Get date range
        date_start, date_end = get_date_range()
        logging.info(f"Extracting news from {date_start} to {date_end}")
        
        er = EventRegistry(apiKey=EVENT_REGISTRY_API_KEY, allowUseOfArchive=False)
        
        # Create query for travel-related news
        q = QueryArticlesIter(
            keywords="travel tourism",
            dateStart=date_start,
            dateEnd=date_end,
            lang="eng",
            dataType=["blog"]  # Focus on blog content
        )
        
        # Configure return info to get additional article details
        returnInfo = ReturnInfo(
            articleInfo=ArticleInfoFlags(
                concepts=True,
                categories=True,
                location=True,
                image=True
            )
        )
        
        # Process articles one by one
        success_count = 0
        error_count = 0
        quarantined_count = 0
        validation_errors = []
        
        for article in q.execQuery(er, maxItems=100, sortByAsc=False, returnInfo=returnInfo):
            try:
                # Send individual article to Tinybird
                success, message = send_to_tinybird(article)
                if success:
                    success_count += 1
                else:
                    if "quarantined" in message.lower():
                        quarantined_count += 1
                        validation_errors.append({
                            'uri': article.get('uri', 'unknown'),
                            'error': message
                        })
                    else:
                        error_count += 1
                    
            except Exception as e:
                logging.error(f"Error processing article: {str(e)}")
                error_count += 1
        
        # Log summary with validation errors
        logging.info(f"Processing complete. Successfully sent: {success_count}, Quarantined: {quarantined_count}, Errors: {error_count}")
        if validation_errors:
            logging.info("Validation errors summary:")
            for error in validation_errors:
                logging.info(f"URI: {error['uri']} - Error: {error['error']}")
        
        return success_count, quarantined_count, error_count, validation_errors
        
    except Exception as e:
        logging.error(f"Error in news extraction: {str(e)}")
        return 0, 0, 0, []

def main():
    try:
        success_count, quarantined_count, error_count, validation_errors = extract_news_data()
        logging.info(f"Final results - Successfully processed: {success_count}, Quarantined: {quarantined_count}, Errors: {error_count}")
        if validation_errors:
            logging.info(f"Found {len(validation_errors)} validation errors. Check the log for details.")
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")

if __name__ == "__main__":
    main()