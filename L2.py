import numpy as np
import pandas as pd
import time
import regex as re
import json
import boto3
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException



def initialize_driver():
    """Initialize the Selenium WebDriver."""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")

    path_to_chromedriver = r"C:\Users\jeffr\Downloads\chromedriver-win64\chromedriver-win64\chromedriver.exe" 
    return webdriver.Chrome(executable_path=path_to_chromedriver, options=options)

driver = initialize_driver()

column_names = [
        "Name", "Address", "City", "Rank", "Ticker Sign", "Page Views Last 24 Hours",
        "Page Views Last 30 Days", "Number of buyers watching", "Number of sellers watching",
        "Currently Active Bids", "Currently Active Listings", "90 Day Historical Bid Low",
        "90 Day Historical Bid Average", "90 Day Historical Bid High", "90 Day Historical Ask Low",
        "90 Day Historical Ask Average", "90 Day Historical Ask High", "Page Views",
        "Average 3-Day Notice Price", "Average 30-Day+ Notice Price", "Transaction Volume",
        "Number Bids", "Number Listings (Asks)", "Number Transactions"
    ]


def scrape_data(driver, url, column_names):
    data = {column: None for column in column_names}
    
    for k,v in df_dict.items():
        data[k]=v[0]

    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'body')))

        try:
            modal = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'confirmBox')))
            if modal.is_displayed():
                cancel_button = driver.find_element(By.ID, 'cancelButton')
                cancel_button.click()
        except (TimeoutException, NoSuchElementException):
            pass

        try:
            address_element = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located(
                    (By.XPATH, '//a[contains(@href, "https://maps.google.com")]'))
            )
            address = address_element.text
            data["Address"] = address
        except (NoSuchElementException, TimeoutException):
            print('address error', url)
            data["Address"] = "n/a"
        
        try:
            more_location_data = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//*[contains(text(), 'More on')]")))
            driver.execute_script("arguments[0].click();", more_location_data)
            time.sleep(2)
        except (TimeoutException, NoSuchElementException):
            print(f"Failed clicking 'more info' dropdown")
            

        try:
            WebDriverWait(driver, 15).until(EC.element_to_be_clickable((By.ID, 'LocationMetrics')))
            location_metrics = driver.find_element(By.ID, 'LocationMetrics')
            info_elements = location_metrics.find_elements(By.XPATH, ".//div[starts-with(@id, 'LocationMetrics-dyndata-')]")

            historical_data = {
                "Bid Low": [],
                "Bid Average": [],
                "Bid High": [],
                "Ask Low": [],
                "Ask Average": [],
                "Ask High": []
            }

            for info_element in info_elements:
                label_element = info_element.find_element(By.XPATH, ".//span[starts-with(@id, 'Label-LocationMetrics-dyndata-')]")
                value_element = info_element.find_element(By.XPATH, ".//span[starts-with(@id, 'Value-LocationMetrics-dyndata-')]")
                label = label_element.get_attribute('innerHTML')
                value = value_element.get_attribute('innerHTML')

                if label == "Ticker Sign:":
                    data["Ticker Sign"] = value
                elif label == "Page Views Last 24 Hours:":
                    data["Page Views Last 24 Hours"] = value
                elif label == "Page Views Last 30 Days:":
                    data["Page Views Last 30 Days"] = value
                elif label == "Number of buyers watching:":
                    data["Number of buyers watching"] = value
                elif label == "Number of sellers watching:":
                    data["Number of sellers watching"] = value
                elif label == "Currently Active Bids:":
                    data["Currently Active Bids"] = value
                elif label == "Currently Active Listings:":
                    data["Currently Active Listings"] = value
                elif label == 'Average 3-Day Notice Price:':
                    data["Average 3-Day Notice Price"] = value
                elif label == 'Average 30-Day+ Notice Price:':
                    data["Average 30-Day+ Notice Price"] = value
                elif label == "Page Views:":
                    data["Page Views"] = value
                elif label == "Transaction Volume:":
                    data["Transaction Volume"] = value
                elif label == "Average 3-Day Notice Price:":
                    data["Average 3-Day Notice Price"] = value
                elif label == "Average 30-Day+ Notice Price:":
                    data["Average 30-Day+ Notice Price"] = value
                elif label == "Number Bids:":
                    data["Number Bids"] = value
                elif label == "Number Listings (Asks):":
                    data["Number Listings (Asks)"] = value
                elif label == "Number Transactions:":
                    data["Number Transactions"] = value
                elif label == "<hr><b>90 Day Historical:</b>":

                    # Extracting low, average, and high bid and ask prices
                    index = int(info_element.get_attribute('id').split('-')[-1])

                    bid_element = None
                    ask_element = None

                    try:
                        bid_element = location_metrics.find_element(By.ID, f"LocationMetrics-dyndata-{index + 1}")
                    except NoSuchElementException:
                        pass

                    try:
                        ask_element = location_metrics.find_element(By.ID, f"LocationMetrics-dyndata-{index + 2}")
                    except NoSuchElementException:
                        pass

                    try:
                        bid_low = None
                        bid_average = None
                        bid_high = None
                        ask_low = None
                        ask_average = None
                        ask_high = None

                        if bid_element:
                            bid_low = bid_element.find_element(By.XPATH, ".//span[contains(@class, 'gray-txt-light') and contains(@style, 'color:var(--ColorDefaultBuy)')]")
                            bid_low_inner_html = bid_low.get_attribute('innerHTML')
                            bid_low = re.search(r"Low: \$([\d,]+)", bid_low_inner_html).group(1) if bid_low_inner_html else None

                            bid_average = bid_element.find_element(By.XPATH, ".//span[contains(@class, 'SpanTag medianprice')]")
                            bid_average_inner_html = bid_average.get_attribute('innerHTML')
                            bid_average = re.search(r"Average Bid: \$([\d,]+)", bid_average_inner_html).group(1) if bid_average_inner_html else None

                            bid_high = bid_element.find_element(By.XPATH, ".//span[contains(@class, 'gray-txt-light') and contains(@style, 'color:var(--ColorDefaultBuy)')][last()]")
                            bid_high_inner_html = bid_high.get_attribute('innerHTML')
                            bid_high = re.search(r"High: \$([\d,]+)", bid_high_inner_html).group(1) if bid_high_inner_html else None

                        if ask_element:
                            ask_low = ask_element.find_element(By.XPATH, ".//span[contains(@class, 'gray-txt-light') and contains(@style, 'color:var(--ColorDefaultSell)')]")
                            ask_low_inner_html = ask_low.get_attribute('innerHTML')
                            ask_low = re.search(r"Low: \$([\d,]+)", ask_low_inner_html).group(1) if ask_low_inner_html else None

                            ask_average = ask_element.find_element(By.XPATH, ".//span[contains(@class, 'SpanTag medianprice')]")
                            ask_average_inner_html = ask_average.get_attribute('innerHTML')
                            ask_average = re.search(r"Average Ask: \$([\d,]+)", ask_average_inner_html).group(1) if ask_average_inner_html else None

                            ask_high = ask_element.find_element(By.XPATH, ".//span[contains(@class, 'gray-txt-light') and contains(@style, 'color:var(--ColorDefaultSell)')][last()]")
                            ask_high_inner_html = ask_high.get_attribute('innerHTML')
                            ask_high = re.search(r"High: \$([\d,]+)", ask_high_inner_html).group(1) if ask_high_inner_html else None

                        historical_data["Bid Low"].append(bid_low)
                        historical_data["Bid Average"].append(bid_average)
                        historical_data["Bid High"].append(bid_high)
                        historical_data["Ask Low"].append(ask_low)
                        historical_data["Ask Average"].append(ask_average)
                        historical_data["Ask High"].append(ask_high)
                    except (NoSuchElementException, AttributeError):
                        pass

            data.update({
                f"90 Day Historical {key}": values[0]
                for key, values in historical_data.items()
                if values
            })

        except (TimeoutException, NoSuchElementException):
            print(f"Failed to find location metrics for URL: {url}")
            

    except (TimeoutException, NoSuchElementException):
        print(f"Failed to load URL: {url}")
        print(driver.page_source)  # Print smaller HTML element for diagnosis
        


    return data


def open_s3():
    s3_client = boto3.client('s3')
    bucket_name = 'your-bucket-name'
    file_key = 'your-file.csv'
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    existing_df = pd.read_csv(response['Body'])
    return existing_df



def lambda_handler(event, context):
    for record in event['Records']:
        message_body = record['body']
        data = json.loads(message_body)


    new_data = scrape_data(driver, url = data['Link'], column_names = column_names)

    new_data = {key: [value] for key, value in new_data.items()}
    df_out = pd.DataFrame(new_data, columns = column_names)

    existing_df = open_s3()
    
    updated_df = existing_df.append(new_df, ignore_index=True)
    
    csv_buffer = StringIO()
    updated_df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())

    sqs = boto3.client('sqs')
    for record in event['Records']:
        receipt_handle = record['receiptHandle']
        sqs.delete_message(QueueUrl='YOUR_SQS_QUEUE_URL', ReceiptHandle=receipt_handle)

    return {
        'statusCode': 200,
    }