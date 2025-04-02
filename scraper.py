from playwright.sync_api import sync_playwright
from utils import require_internet
from processing import download_and_process_excel
import os
from dotenv import load_dotenv
import time

load_dotenv()

# Get URLs from .env file
urls = os.getenv("URLS").split(",")


# scraper.py - Updated select_dropdown_options function
@require_internet
def select_dropdown_options(page):
    print("Selecting years...")

    # waiting
    page.wait_for_selector("input[type='submit']", state="attached", timeout=60000)
    page.wait_for_timeout(10000)  # Base wait time

    selectors = [
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox",
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_ValuesListBox",
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_ValuesListBox",
    ]

    for selector in selectors:
        page.wait_for_selector(selector, state="attached", timeout=30000)
        page.eval_on_selector(
            selector,
            "el => { Array.from(el.options).forEach(option => option.selected = true); el.dispatchEvent(new Event('change')); }",
        )
        page.wait_for_timeout(2000)  # Short pause between selections

    # Year selection with error handling
    year_selector = "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_ValuesListBox"
    page.wait_for_selector(year_selector, state="attached", timeout=30000)

    total_years = page.eval_on_selector(year_selector, "el => el.options.length")
    print(f"Total available years: {total_years}")

    slice_count = min(2, total_years)  # Start with 2 or total available if less

    while slice_count <= total_years:
        print(f"Selecting {slice_count} years...")
        try:
            page.eval_on_selector(
                year_selector,
                f"el => {{ Array.from(el.options).slice(0, {slice_count}).forEach(option => option.selected = true); el.dispatchEvent(new Event('change')); }}",
            )
            page.wait_for_timeout(3000)  # Longer wait for validation

            if page.is_visible("#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_SelectionErrorlabel",
                               timeout=5000):
                print("Selection limit reached, adjusting to be fit <= 100,000 rows...")
                page.eval_on_selector(
                    year_selector,
                    "el => { let options = Array.from(el.options).filter(option => option.selected); if (options.length > 0) { options[options.length - 1].selected = false; el.dispatchEvent(new Event('change')); } }",
                )
                break
            slice_count += 1
        except Exception as e:
            print(f"Error during year selection: {e}")
            break

    # Format selection with retries
    format_selector = "select#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_OutputFormats_OutputFormats_OutputFormatDropDownList"
    for attempt in range(3):
        try:
            page.select_option(format_selector, value="FileTypeExcelX", timeout=30000)
            break
        except Exception as e:
            if attempt == 2:
                raise
            print(f"Format selection failed (attempt {attempt + 1}), retrying...")
            page.wait_for_timeout(3000)

    # More robust submit button handling
    submit_button = page.wait_for_selector("input[type='submit']", state="attached", timeout=120000)
    if submit_button:
        print("Submit button found, clicking with retries...")
        for attempt in range(3):
            try:
                submit_button.click(timeout=60000)
                page.wait_for_load_state("networkidle", timeout=180000)
                print("Page loaded successfully")
                break
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"Submit failed (attempt {attempt + 1}), retrying...")
                page.wait_for_timeout(10000)
    else:
        raise Exception("Submit button NOT found after extended waiting!")

def navigate_with_retries(page, url, max_retries=3, timeout=300000):
    """Navigate to a URL with retries and flexible timeout."""
    retries = 0
    while retries < max_retries:
        try:
            print(f"Trying navigate to URL {retries + 1}/{max_retries}: {url}")
            page.goto(url, timeout=timeout)  # Increased timeout
            page.wait_for_load_state("networkidle", timeout=60000)
            print(f"Successfully loaded: {url}")
            return
        except Exception as e:
            print(f"Error navigating to URL: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying... Attempt {retries + 1}/{max_retries}")
                time.sleep(5)  # Wait before retrying
            else:
                print(f"Failed to load URL after {max_retries} attempts. Skipping URL.")
                return

@require_internet
def scrape_all():
    print("Internet available, starting...")
    from database import store_data_in_mysql
    from datetime import datetime
    import pandas as pd

    timestamp = datetime.now().strftime("%B%d,%Y_%H-%M-%S")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        all_cleaned_sheets = {}

        for index, url in enumerate(urls):
            print(f"\nNavigating to URL {index + 1}/{len(urls)}: {url}")
            navigate_with_retries(page, url, max_retries=5, timeout=300000)

            select_dropdown_options(page)

            cleaned_data = download_and_process_excel(page, index, timestamp, urls)
            all_cleaned_sheets.update(cleaned_data)

        browser.close()

    if all_cleaned_sheets:
        filename = f"Scraped_Agri-Prices_{timestamp}.xlsx"
        with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
            for sheet, data in all_cleaned_sheets.items():
                data.to_excel(writer, sheet_name=sheet, index=False)

        print(f"✅ Final Excel file saved: {filename}")
        store_data_in_mysql(all_cleaned_sheets)
    else:
        print("❌ No data was scraped!")