import csv
import requests
import concurrent.futures
import time
from urllib.parse import quote, urlparse
import os
from typing import List, Tuple, Optional


def process_kismode(kismode: str) -> str:
    """Convert KISMODE code to human-readable format."""
    if kismode == '01':
        return 'Full-time'
    elif kismode == '02':
        return 'Part-time'
    elif kismode == '03':
        return 'Both'
    else:
        return 'Unknown'


def generate_url(pubukprn: str, kiscourseid: str, kismode: str) -> str:
    """Generate the URL from the provided parameters."""
    mode_str = process_kismode(kismode)
    encoded_pubukprn = quote(str(pubukprn).strip())
    encoded_kiscourseid = quote(str(kiscourseid).strip())
    encoded_mode = quote(mode_str.strip())

    return f"https://discoveruni.gov.uk/course-details/{encoded_pubukprn}/{encoded_kiscourseid}/{encoded_mode}/"


def get_final_url(url: str, timeout: int = 10) -> Tuple[str, str, float, bool]:
    """
    Follow redirects and return the final URL, response time, and whether it's the home page.
    Returns: (original_url, final_url, response_time, is_home_page)
    """
    start_time = time.time()
    try:
        # Don't allow redirects initially so we can track them manually
        response = requests.get(
            url,
            timeout=timeout,
            allow_redirects=False,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
        )

        final_url = url
        is_home_page = False

        # If there's a redirect, follow it
        if response.status_code in (301, 302, 303, 307, 308) and 'Location' in response.headers:
            redirect_url = response.headers['Location']
            # Handle relative redirects
            if redirect_url.startswith('/'):
                parsed_original = urlparse(url)
                redirect_url = f"{parsed_original.scheme}://{parsed_original.netloc}{redirect_url}"

            # Follow the redirect
            redirect_response = requests.get(
                redirect_url,
                timeout=timeout,
                allow_redirects=True,  # Allow further redirects
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                }
            )
            final_url = redirect_response.url
        else:
            # No redirect, use original URL
            final_url = response.url

        response_time = time.time() - start_time

        # Check if the final URL is the home page
        parsed_final = urlparse(final_url)
        is_home_page = (
                parsed_final.path == '/' or
                parsed_final.path == '/home' or
                parsed_final.path == '/index.html' or
                'home' in parsed_final.path.lower() or
                len(parsed_final.path.strip('/')) == 0  # Just domain with trailing slash
        )

        return url, final_url, response_time, is_home_page

    except requests.exceptions.RequestException as e:
        response_time = time.time() - start_time
        return url, f"ERROR: {str(e)}", response_time, False


def process_urls_concurrently(urls: List[str], max_workers: int = None) -> List[Tuple[str, str, float, bool]]:
    """Process URLs concurrently and return final URLs after redirects."""
    if max_workers is None:
        max_workers = min(32, (os.cpu_count() or 8) * 2)

    print(f"Using {max_workers} concurrent workers")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(get_final_url, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
                if i % 100 == 0:
                    print(f"Processed {i}/{len(urls)} URLs")
            except Exception as e:
                results.append((url, f"EXCEPTION: {str(e)}", 0.0, False))
                print(f"Error processing {url}: {e}")

    return results


def optimized_url_processor(urls: List[str], max_workers: int = 16, timeout: int = 8):
    """Optimized URL processor with session reuse for better performance."""
    results = []
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max_workers,
        pool_maxsize=max_workers,
        max_retries=2
    )
    session.mount('https://', adapter)

    def process_single_url(url):
        start_time = time.time()
        try:
            # Follow redirects automatically but track final URL
            response = session.get(
                url,
                timeout=timeout,
                allow_redirects=True,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                }
            )

            response_time = time.time() - start_time
            final_url = response.url

            # Check if redirected to home page (404 handling)
            parsed_final = urlparse(final_url)
            is_home_page = (
                    parsed_final.path == '/' or
                    parsed_final.path == '/home' or
                    parsed_final.path == '/index.html' or
                    parsed_final.path == '/?load_error=true&error_type=0' or
                    'home' in parsed_final.path.lower() or
                    len(parsed_final.path.strip('/')) == 0
            )

            return url, final_url, response_time, is_home_page

        except requests.exceptions.RequestException as e:
            response_time = time.time() - start_time
            return url, f"ERROR: {str(e)}", response_time, False
        except Exception as e:
            response_time = time.time() - start_time
            return url, f"EXCEPTION: {str(e)}", response_time, False

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(process_single_url, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
            try:
                result = future.result()
                results.append(result)
                if i % 250 == 0:
                    print(f"Processed {i}/{len(urls)} URLs")
            except Exception as e:
                url = future_to_url[future]
                results.append((url, f"FUTURE_EXCEPTION: {str(e)}", 0.0, False))

    session.close()
    return results


def main(input_csv: str, output_csv: str = None, max_workers: int = None, use_optimized: bool = True):
    """Main function to process the CSV and get final URLs after redirects."""
    if output_csv is None:
        base_name = os.path.splitext(input_csv)[0]
        output_csv = f"{base_name}_with_final_urls.csv"

    # Read the input CSV
    rows = []
    urls = []
    url_to_row_index = {}

    print("Reading CSV and generating URLs...")
    with open(input_csv, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        headers = next(reader)  # Read header row

        for row_index, row in enumerate(reader):
            if len(row) >= 3:
                pubukprn, kiscourseid, kismode = row[0], row[1], row[2]
                url = generate_url(pubukprn, kiscourseid, kismode)
                urls.append(url)
                url_to_row_index[url] = row_index
                # Store original row with placeholders
                rows.append(row + [url, '', '', ''])  # URL, Final_URL, Response_Time, Is_Home_Page
            else:
                rows.append(row + ['', 'ERROR: Invalid row', '', ''])

    print(f"Generated {len(urls)} URLs from {len(rows)} rows")

    # Process URLs concurrently
    print("Starting concurrent URL processing with redirect following...")
    start_time = time.time()

    if use_optimized:
        url_results = optimized_url_processor(urls, max_workers)
    else:
        url_results = process_urls_concurrently(urls, max_workers)

    total_time = time.time() - start_time

    print(f"URL processing completed in {total_time:.2f} seconds")
    print(f"Average time per URL: {total_time / len(urls):.3f} seconds")

    # Update rows with results
    home_page_redirects = 0
    for original_url, final_url, response_time, is_home_page in url_results:
        if original_url in url_to_row_index:
            row_index = url_to_row_index[original_url]
            if len(rows[row_index]) >= 7:  # Ensure we have enough columns
                rows[row_index][4] = final_url  # Final URL
                rows[row_index][5] = f"{response_time:.3f}"  # Response time
                rows[row_index][6] = "Yes" if is_home_page else "No"  # Is home page

                if is_home_page:
                    home_page_redirects += 1

    # Write the updated CSV
    print(f"Writing results to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        # Write headers with new columns
        writer.writerow(headers + ['Generated_URL', 'Final_URL', 'Response_Time_Seconds', 'Redirected_To_Home_Page'])
        writer.writerows(rows)

    # Print summary statistics
    successful_requests = sum(1 for row in rows if len(row) > 4 and not row[4].startswith(('ERROR:', 'EXCEPTION:')))
    error_requests = sum(1 for row in rows if len(row) > 4 and row[4].startswith(('ERROR:', 'EXCEPTION:')))

    print(f"\nSummary:")
    print(f"Total URLs processed: {len(urls)}")
    print(f"Successful requests: {successful_requests}")
    print(f"Error requests: {error_requests}")
    print(f"Redirects to home page (likely 404s): {home_page_redirects}")
    print(f"Output file: {output_csv}")


if __name__ == "__main__":
    input_file = "output.csv"  # Your input CSV file
    output_file = "output_with_final_urls.csv"

    # For M3 MacBook Pro - adjust based on your network
    max_workers = 20  # Conservative number to avoid overwhelming the server

    print(f"Starting URL processing on M3 MacBook Pro with {max_workers} workers...")
    print("Note: This will follow redirects and capture final URLs")
    print("404 pages that redirect to home will be marked as such\n")

    main(input_file, output_file, max_workers, use_optimized=True)