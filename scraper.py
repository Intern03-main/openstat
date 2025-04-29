from playwright.sync_api import sync_playwright
from utils import require_internet, wait_for_selector_with_retry, wait_for_internet
from processing import download_and_process_excel
from database import store_data_in_mysql
from dotenv import load_dotenv
from rich.console import Console
from datetime import datetime
import os
import pandas as pd
import time

# Initialize
console = Console()
load_dotenv()
urls = os.getenv("URLS").split(",")

timestamp = datetime.now().strftime("%B%d,%Y_%H-%M-%S")

@require_internet
def select_dropdown_options(page, year_indexes, finalize=False):
    """Select all fixed dropdowns and optionally the year + Excel option"""
    selectors = [
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl01_VariableValueSelect_VariableValueSelect_ValuesListBox",
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl02_VariableValueSelect_VariableValueSelect_ValuesListBox",
        "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl04_VariableValueSelect_VariableValueSelect_ValuesListBox",
    ]
    year_selector = "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_ValuesListBox"

    for selector in selectors:
        wait_for_selector_with_retry(page, selector)
        page.eval_on_selector(selector, """
            el => {
                Array.from(el.options).forEach(opt => opt.selected = true);
                el.dispatchEvent(new Event('change'));
            }
        """)
        page.wait_for_timeout(1500)

    # Handle year selection
    wait_for_selector_with_retry(page, year_selector)
    page.eval_on_selector(year_selector, """
        el => {
            Array.from(el.options).forEach(opt => opt.selected = false);
            el.dispatchEvent(new Event('change'));
        }
    """)
    for idx in year_indexes:
        page.eval_on_selector(year_selector, f"""
            el => {{
                el.options[{idx}].selected = true;
                el.dispatchEvent(new Event('change'));
            }}
        """)
        page.wait_for_timeout(500)

    if finalize:
        format_selector = "select#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_OutputFormats_OutputFormats_OutputFormatDropDownList"
        for attempt in range(3):
            try:
                page.select_option(format_selector, value="FileTypeExcelX", timeout=30000)
                break
            except Exception:
                if attempt == 2:
                    raise
                console.print(f"[yellow]Retrying Excel format selection (Attempt {attempt+2}/3)...[/yellow]")
                page.wait_for_timeout(3000)


@require_internet
def scrape_all():
    all_data_frames = []
    console.rule(f"[bold cyan]Agri-Price Scraper Started at {timestamp}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()

        for url_index, url in enumerate(urls):
            console.rule(f"[bold green]Processing URL {url_index + 1}/{len(urls)}")
            navigate_with_retries(page, url)

            year_selector = "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_VariableSelectorValueSelectRepeater_ctl03_VariableValueSelect_VariableValueSelect_ValuesListBox"
            wait_for_selector_with_retry(page, year_selector, timeout=30000)
            total_years = page.eval_on_selector(year_selector, "el => el.options.length")
            console.print(f"[bold blue]Years available:[/bold blue] {total_years}")

            remaining_years = list(range(total_years))

            while remaining_years:
                current_batch = []
                for year_idx in remaining_years:
                    test_batch = current_batch + [year_idx]
                    console.print(f"[dim]Testing batch:[/dim] {test_batch}")

                    navigate_with_retries(page, url)
                    select_dropdown_options(page, test_batch)

                    try:
                        is_error = page.is_visible(
                            "#ctl00_ContentPlaceHolderMain_VariableSelector1_VariableSelector1_SelectionErrorlabel",
                            timeout=5000
                        )
                        if is_error:
                            console.print("[yellow]Selection limit hit, finalizing batch...[/yellow]")
                            break
                        else:
                            current_batch = test_batch
                    except Exception as e:
                        console.print(f"[red]Error checking selection: {e}[/red]")
                        break

                if not current_batch:
                    skipped = remaining_years.pop(0)
                    console.print(f"[red]Skipped year index:[/red] {skipped}")
                    continue

                # Finalize dropdowns and download
                navigate_with_retries(page, url)
                select_dropdown_options(page, current_batch, finalize=True)

                try:
                    submit_button = page.wait_for_selector("input[type='submit']", timeout=120000)
                    submit_button.click()
                    page.wait_for_load_state("networkidle", timeout=180000)

                    cleaned_data = download_and_process_excel(page, url_index, timestamp, urls)
                    if not cleaned_data.empty:
                        all_data_frames.append(cleaned_data)
                        console.print(f"[green]✔ Processed batch:[/green] {current_batch}")
                    else:
                        console.print("[yellow]No data returned[/yellow]")

                except Exception as e:
                    console.print(f"[red] Submission error:[/red] {e}")

                remaining_years = [y for y in remaining_years if y not in current_batch]

        browser.close()

    if all_data_frames:
        final_df = pd.concat(all_data_frames, ignore_index=True)
        output_dir = os.path.join(os.path.expanduser("~"), "Desktop", "Agri-Price-Data_Files")
        os.makedirs(output_dir, exist_ok=True)

        # Save per commodity instead of a single file
        commodities = final_df['Commodity'].unique()
        for commodity in commodities:
            commodity_df = final_df[final_df['Commodity'] == commodity].copy()
            # commodity_df.drop(columns=['Commodity'], inplace=True)  # optional
            safe_name = commodity.replace(" ", "_").replace("/", "-")
            commodity_file = os.path.join(output_dir, f"{safe_name}_{timestamp}.csv")
            commodity_df.to_csv(commodity_file, index=False)
            console.print(f"[bold green]✔ Saved:[/bold green] {commodity_file}")

        store_data_in_mysql(final_df)
    else:
        console.print("[bold red]No data was scraped![/bold red]")

    console.rule("[bold cyan]Scraper Finished")


def navigate_with_retries(page, url, timeout=30000):
    """Navigate to a URL, retrying on errors or disconnection. After reconnection, reloads and continues."""
    attempt = 1
    while True:
        try:
            print(f"→ Navigating to URL (Attempt {attempt})...")
            page.goto(url, timeout=timeout)
            page.wait_for_load_state("networkidle", timeout=60000)
            return
        except Exception as e:
            print(f"Navigation error: {e}")

            # If network error detected, wait for reconnection and reload the page
            if "ERR_NAME_NOT_RESOLVED" in str(e) or "ERR_NETWORK_CHANGED" in str(e) or "net::ERR_INTERNET_DISCONNECTED" in str(e):
                print("⚠ Internet lost. Waiting to reconnect...")
                wait_for_internet()
                print("Internet reconnected. Reloading the page and retrying...")
                try:
                    page.reload(timeout=timeout)
                    page.wait_for_load_state("networkidle", timeout=60000)
                    return
                except Exception as reload_error:
                    print(f"Reload failed: {reload_error}. Retrying from scratch in 5 seconds...")
            else:
                print(f"Unexpected error. Retrying in 5 seconds... ({e})")

            time.sleep(5)
            attempt += 1