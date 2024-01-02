
# Web Scraping and Data Processing Tool for our newsletter.

This Python script is designed for web scraping and data processing, particularly for gathering news articles related to specific keywords. It uses libraries such as `requests`, `BeautifulSoup`, `openpyxl`, `smtplib`, `boto3`, and others to perform various tasks like downloading files from AWS S3, sending emails, and manipulating Excel files.

## Features

1. **Web Scraping**: Extracts news content from various websites using predefined extraction rules.
2. **S3 Integration**: Downloads and uploads files to/from AWS S3.
3. **Email Notifications**: Sends emails with gathered information.
4. **Excel Operations**: Appends data to an Excel file.
5. **Data Summarization**: Utilizes an API to summarize content.
6. **Custom Date Processing**: Transforms date strings into different formats.

## Usage

1. **Setting Extraction Rules**: Define the extraction rules for each target website. These rules are dictionaries that specify how to extract the date, title, and content from web pages.
2. **File Operations with AWS S3**: Use `download_file_from_s3` and `upload_file_to_s3` to handle files in an S3 bucket.
3. **Sending Emails**: The `email_sender` function takes a list of URLs and sends an email with their contents. Email settings are configurable within the function.
4. **Excel File Manipulation**: Append data to an Excel file using `append_to_excel`.

## Functions

- `download_file_from_s3(bucket_name, object_key, local_path)`: Downloads a file from S3.
- `upload_file_to_s3(file_path, bucket_name, object_key)`: Uploads a file to S3.
- `email_sender(matching_urls, selected_operator)`: Sends an email with the URLs that match the given criteria.
- `find_matching_urls_with_keywords(keywords, selected_operator)`: Finds URLs that match the specified keywords.
- `lambda_handler(event, context)`: AWS Lambda handler function for processing events.

## Setup

1. Ensure all required libraries are installed.
2. Configure AWS credentials for S3 access.
3. Set up the email account and password for sending emails.
4. Define extraction rules for each target website.

## Note

This script requires external APIs and services like AWS S3 and a summarization API. Make sure you have access to these services and their credentials are properly set up.

---

