import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import logging
import time
import regex as re
import json
import boto3



def get_html(url):
    """Retrieve the HTML content of a webpage."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def get_city_urls():
    """Retrieve URLs for each city from the main page."""
    url = 'https://appointmenttrader.com/'
    html_content = get_html(url)
    if html_content:
        soup = BeautifulSoup(html_content, 'html.parser')
        cities_container = soup.find(class_="horizontalScrollContainerInner")
        if cities_container:
            city_elements = cities_container.find_all('a')
            # Skipping over the first element due to the geographical recommendation feature
            return {city_element['href']: city_element.get_text(strip=True) for city_element in city_elements[1:]}

def create_additional_urls(cities_urls):
    """Create additional URLs for each city."""
    additional_urls = {}
    for city_url, city in cities_urls.items():
        for i in range(26, 276, 50):
            additional_urls[f"{city_url}_rank-{i}-to-{i+49}"] = city
    return additional_urls

def combine_urls(cities_urls, additional_urls):
    """Combine city and additional URLs."""
    return {**cities_urls, **additional_urls}

def scrape_all_urls(url_list):
    """Iterating through urls to find each restaurant link on that page."""
    all_links = []
    for url in url_list:
        city_name = urls[url]
        while city_name[0].isalpha() == False:
            city_name = city_name[1:]
        html_content = get_html(url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            elements = soup.find_all(class_='top10-center-content')
            for element in elements:
                link = element.find('a')['href']
                name = element.find(class_='lead-text ToplistTitle').find('a').text.strip()
                
                p_element = element.find_all('p', class_='small-text mb-1 gray-txt ToplistDescription')                
                match = re.search(r"Rank (\d+)", p_element[0].text)
                rank = match.group(1) if match else ''
                
                business_type_match =re.search(r"(Prepaid Restaurant|Bar|Restaurant|Night Club|Hotel|Health and Beauty Service)", p_element[1].text, re.IGNORECASE)
                business_type = business_type_match.group(1).strip() if business_type_match else ''
                
                all_links.append([f'https://appointmenttrader.com{link}', name, city_name, rank, business_type])
                
        time.sleep(1.5)
        
    return all_links

def wrapped_function():
    try:
        cities_urls = get_city_urls()
        additional_urls = create_additional_urls(cities_urls)
        urls = combine_urls(cities_urls, additional_urls)
        url_list = [url for url in urls.keys()]

        # Run all_links function to create comprehensive restaurant link list
        all_links = scrape_all_urls(url_list)

    except Exception as e:
        logging.error(f'Error occurred: {e}', exc_info=True)


    df = pd.DataFrame(all_links)
    df.columns = ['Link', 'Name', 'City', 'Rank', 'Type']

    return df



def lambda_handler(event, context):
    sqs = boto3.client('sqs')

    ### NEEDS TO BE CHANGED#######
    queue_url = 'https://sqs.us-east-2.amazonaws.com/380604359918/RestaurantLinks'

    df = wrapped_function()

    for index, row in df.iterrows():
        message_body = row.to_json()

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body
        )

        print(response)

    return {
        'statusCode': 200,
        'body': json.dumps('Messages sent to SQS')
    }