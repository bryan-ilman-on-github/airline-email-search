import pandas as pd
from googlesearch import search
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import sys
import warnings

# Suppress only the single warning from urllib3 needed.
from requests.packages.urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

# Function to decode Cloudflare's email protection.
def decode_cfemail(cfemail):
    r = int(cfemail[:2], 16)
    email = ''.join([chr(int(cfemail[i:i+2], 16) ^ r) for i in range(2, len(cfemail), 2)])
    return email

# Function to extract emails from specific HTML tags.
def extract_emails_from_soup(soup):
    emails = set()

    # Extract from mailto links.
    for mailto in soup.select('a[href^=mailto]'):
        email = mailto.get('href').replace('mailto:', '').strip()
        # Remove any trailing punctuation.
        email = email.rstrip('.,;:')
        # Validate email format.
        if re.match(r'^[\w.+-]+@[\w-]+\.[\w.-]+$', email):
            emails.add(email)

    # Extract from Cloudflare-protected emails.
    for cfemail in soup.find_all('a', class_='__cf_email__'):
        encoded = cfemail.get('data-cfemail')
        if encoded:
            try:
                decoded_email = decode_cfemail(encoded)
                # Validate email format.
                if re.match(r'^[\w.+-]+@[\w-]+\.[\w.-]+$', decoded_email):
                    emails.add(decoded_email)
            except Exception as e:
                # Handle any decoding errors.
                continue

    # Extract from text within specific tags.
    for tag in soup.find_all(['p', 'span', 'div']):
        text = tag.get_text(separator=' ').strip()
        # Find all email-like patterns.
        found_emails = re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
        for email in found_emails:
            # Remove any trailing punctuation.
            email = email.rstrip('.,;:')
            # Validate email format.
            if re.match(r'^[\w.+-]+@[\w-]+\.[\w.-]+$', email):
                emails.add(email)

    return list(emails)

# Function to extract emails from a webpage, handling SSL errors by disabling verification.
def extract_emails_from_url(url, log):
    parsed_url = urlparse(url)
    scheme = parsed_url.scheme

    try_urls = []

    # Prioritize HTTP first.
    if scheme == 'https':
        http_url = url.replace('https://', 'http://', 1)
        try_urls.append(http_url)
        try_urls.append(url)  # Retry HTTPS if HTTP fails.
    elif scheme == 'http':
        try_urls.append(url)
        https_url = url.replace('http://', 'https://', 1)
        try_urls.append(https_url)  # Retry HTTPS if HTTP fails.
    else:
        # If scheme is missing, assume HTTP first.
        http_url = 'http://' + url
        https_url = 'https://' + url
        try_urls.extend([http_url, https_url])

    # Set a User-Agent header to mimic a real browser.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/58.0.3029.110 Safari/537.3'
    }

    for attempt_url in try_urls:
        try:
            # Disable SSL verification by setting verify=False.
            response = requests.get(attempt_url, timeout=10, verify=False, headers=headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            emails = extract_emails_from_soup(soup)
            if emails:
                return emails
        except requests.exceptions.SSLError as ssl_error:
            log.write(f"SSL ERROR for {attempt_url}: {ssl_error}\n")
            continue  # Try the next URL (HTTP or HTTPS).
        except requests.exceptions.RequestException as req_error:
            log.write(f"REQUEST ERROR for {attempt_url}: {req_error}\n")
            continue  # Try the next URL.
        except Exception as e:
            log.write(f"UNKNOWN ERROR for or parsing {attempt_url}: {e}\n")
            continue  # Try the next URL.

    return []

# Function to remove trailing punctuation from text.
def remove_trailing_punctuation(text):
    if not pd.isna(text) and text:
        return text.rstrip('.,;:')
    return text

# Function to read an Excel tab, search airline names, and update the file.
def search_and_update_airline_emails(file_path, tab_name, num_names=None):
    # Read the specified tab of the Excel file.
    try:
        df = pd.read_excel(file_path, sheet_name=tab_name)
    except Exception as e:
        print(f"FILE ERROR: {e}")
        sys.exit(1)

    # Ensure the file contains the necessary columns.
    if len(df.columns) < 2:
        raise ValueError("The selected tab must have at least two columns: 'Airline Code' and 'Airline Name'.")

    # Add necessary columns if not present.
    if 'Emails' not in df.columns:
        df["Emails"] = ""

    if 'Source URL' not in df.columns:
        df["Source URL"] = ""

    # Prepare for logging.
    log_file = "airline_email_search.log"
    with open(log_file, "w", encoding='utf-8') as log:
        total_rows = num_names if num_names else len(df)
        success_count = 0
        fail_count = 0

        # Process each airline.
        for index, row in df.iterrows():
            if num_names and index >= num_names:
                break

            airline = row.iloc[1]  # Accessing by position explicitly.
            print(f"Processing '{airline}' ({index + 1}/{total_rows})...")
            log.write(f"\nProcessing '{airline}':\n")
            emails_found = []
            source_url = ""

            try:
                # Perform Google search with "contact email" appended.
                query = f"{airline} contact email"
                for result_url in search(query, num_results=4):
                    emails = extract_emails_from_url(result_url, log)
                    print(f"{result_url} {emails}")
                    if emails:
                        log.write(f"Email(s) found at {result_url}.\n")
                        # Remove trailing punctuation from each email.
                        cleaned_emails = [email.rstrip('.,;:') for email in emails]
                        emails_found.extend(cleaned_emails)
                        source_url = result_url  # Capture the source URL of the first valid email.
                        break  # Stop after finding emails.
            except Exception as e:
                log.write(f"SEARCH ERROR for {airline}: {e}\n")

            # Update the DataFrame with found emails and source URL.
            if emails_found:
                # Remove any duplicates and join with commas.
                unique_emails = ', '.join(sorted(set(emails_found)))
                df.at[index, "Emails"] = unique_emails
                df.at[index, "Source URL"] = remove_trailing_punctuation(source_url)
                success_count += 1
            else:
                df.at[index, "Emails"] = "No email found."
                df.at[index, "Source URL"] = "No valid URL found."
                fail_count += 1

        # Write summary to log.
        log.write(f"\nTotal Rows Processed: {total_rows}\n")
        log.write(f"Successes: {success_count}\n")
        log.write(f"Failures: {fail_count}\n")

    # Save the updated DataFrame back to the file.
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=tab_name, index=False)
    except Exception as e:
        print(f"WRITING ERROR: {e}")
        sys.exit(1)

    print(f"\nFile updated successfully. Log saved to {log_file}.")

# Example usage.
if __name__ == "__main__":
    # File path to the Excel file.
    file_path = "airline_database.xlsx"  # Replace with your file path.

    # Tab name to read.
    tab_name = "Airlines"  # Replace with your tab name.

    # Number of airline names to process (set None to process all rows).
    num_names = 10  # Replace with the desired number of names to iterate or None for all.

    # Call the function.
    search_and_update_airline_emails(file_path, tab_name, num_names)
