import io
import os
import re
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from utils import require_internet
from sklearn.impute import SimpleImputer

load_dotenv()


@require_internet
def download_and_process_excel(page, url_index, timestamp, urls):
    print(f"Downloading Excel file for URL {url_index + 1}/{len(urls)}...")

    # Enhanced timeout and retry configuration
    MAX_SUBMIT_RETRIES = 3
    BASE_TIMEOUT = 300000  # 5 minutes
    INCREMENTAL_TIMEOUT = url_index * 30000  # 30 seconds more per URL
    TOTAL_TIMEOUT = min(BASE_TIMEOUT + INCREMENTAL_TIMEOUT, 600000)  # Max 10 minutes

    # Ensure error directories exist
    os.makedirs("screenshots", exist_ok=True)
    os.makedirs("error_logs", exist_ok=True)

    try:
        for attempt in range(MAX_SUBMIT_RETRIES):
            try:
                print(f"⚡️ Attempt {attempt + 1}/{MAX_SUBMIT_RETRIES} to submit and download...")

                with page.expect_download(timeout=TOTAL_TIMEOUT) as download_info:
                    # Enhanced click with timeout and verification
                    page.click("input[type='submit']", timeout=30000)

                download = download_info.value

                # Verify download completion
                download_path = download.path()
                wait_time = 0
                while not os.path.exists(download_path) and wait_time < 120:  # Max 2 minutes
                    page.wait_for_timeout(1000)
                    wait_time += 1

                if not os.path.exists(download_path):
                    raise Exception("Download file not found after waiting")

                print("✅ File downloaded successfully!")
                break  # Exit retry loop if successful

            except Exception as e:
                print(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
                if attempt == MAX_SUBMIT_RETRIES - 1:
                    # Final attempt failed, capture diagnostics
                    print("❌ Failed to submit after multiple attempts. Capturing diagnostics...")
                    page.screenshot(path=f"screenshots/failure_url_{url_index + 1}.png")
                    with open(f"error_logs/url_{url_index + 1}.html", "w", encoding="utf-8") as f:
                        f.write(page.content())
                    return {}  # Return empty dict to skip this URL

                # Wait before retrying with increasing delay
                retry_delay = (attempt + 1) * 5000  # 5, 10, 15 seconds
                print(f"🔄 Retrying in {retry_delay / 1000} seconds...")
                page.wait_for_timeout(retry_delay)

        # Proceed with reading and processing Excel content
        file_stream = io.BytesIO()
        with open(download.path(), "rb") as f:
            file_stream.write(f.read())
        file_stream.seek(0)

        print("📄 Reading and cleaning Excel content...")
        df = pd.read_excel(file_stream, sheet_name=None, header=None)
        cleaned_sheets = {}

        unwanted_texts = os.getenv("UNWANTED_TEXTS").split(",")

        for sheet, data in df.items():
            if data is None or data.empty:
                continue

            # SHEET-NAME with enhanced error handling
            def get_short_name_from_page(page):
                try:
                    title_text = page.text_content(
                        "span.hierarchical_tableinformation_title",
                        timeout=10000  # Added timeout
                    ).strip()
                    short_name = title_text.split(":")[0]  # Extract first word before colon
                    return short_name[:10]  # Limit to 10 characters
                except Exception as e:
                    print(f"Could not extract short name: {e}")
                    return f"Unknown_{url_index}"  # More descriptive fallback

            short_name = get_short_name_from_page(page)
            new_sheet_name = f"{short_name}_{url_index + 1}"

            # COMMODITY-NAME with enhanced error handling
            def get_commodity_type_from_page(page):
                try:
                    title_text = page.text_content(
                        "span.hierarchical_tableinformation_title",
                        timeout=10000  # Added timeout
                    ).strip()
                    commodity_type = title_text.split(":")[0]
                    return commodity_type[:18]  # Limit to 18 characters
                except Exception as e:
                    print(f"⚠️ Could not extract commodity name: {e}")
                    return f"Commodity_{url_index}"  # More descriptive fallback

            commodity_type = get_commodity_type_from_page(page)

            # Data processing with additional validation
            data.dropna(how="all", inplace=True)
            if data.empty:
                print(f"Sheet {sheet} is empty after dropping NA rows")
                continue

            data.reset_index(drop=True, inplace=True)

            # Enhanced year detection with fallback
            possible_year_row = data.iloc[:5].apply(
                lambda row: row.astype(str).str.contains(r"\d{4}").sum(), axis=1
            )
            year_row_index = (
                possible_year_row.idxmax() if possible_year_row.max() > 0 else None
            )

            if year_row_index is None:
                print(f"No year row detected in sheet {sheet}")
                continue

            year_headers = (
                data.iloc[year_row_index, 2:]
                .astype(str)
                .str.extract(r"(\d{4})")[0]
                .ffill()
                .dropna()
                .astype(int)
                .values
            )

            month_row_index = year_row_index + 1
            months = (
                data.iloc[month_row_index, 2:].dropna().astype(str).values
                if month_row_index is not None and month_row_index < len(data)
                else []
            )

            if not len(year_headers) or not len(months):
                print(f"Insufficient temporal data in sheet {sheet}")
                continue

            year_col_map = []
            for i, month in enumerate(months):
                if i < len(year_headers):  # Prevent index errors
                    year_col_map.append(year_headers[i])

            data = data.iloc[month_row_index + 1:].reset_index(drop=True)
            data.rename(columns={0: "Geolocation", 1: "Commodity"}, inplace=True)

            # Melt with additional validation
            try:
                data_long = pd.melt(
                    data,
                    id_vars=["Geolocation", "Commodity"],
                    var_name="column_index",
                    value_name="Price",
                )
            except Exception as e:
                print(f"Failed to melt data for sheet {sheet}: {e}")
                continue

            # Add temporal information with bounds checking
            data_long["Commodity Type"] = commodity_type
            data_long["Year"] = data_long["column_index"].apply(
                lambda x: year_col_map[x - 2] if x - 2 < len(year_col_map) else None
            )
            data_long["Period"] = data_long["column_index"].apply(
                lambda x: months[x - 2] if x - 2 < len(months) else None
            )
            data_long.drop(columns=["column_index"], inplace=True)

            # Enhanced data cleaning
            data_long["Price"] = data_long["Price"].replace(["..", "-", "NA"], "")
            pattern = "|".join(map(re.escape, unwanted_texts))
            data_long = data_long[
                ~data_long.astype(str).apply(
                    lambda row: row.str.contains(pattern, na=False, regex=True).any(),
                    axis=1,
                )
            ]

            data_long["Geolocation"] = (
                data_long["Geolocation"]
                .str.replace(r"^\.+", "", regex=True)
                .replace("", np.nan)
                .ffill()
            )

            if data_long[["Geolocation", "Commodity"]].isna().all().all():
                print(f"No valid geolocation/commodity data in sheet {sheet}")
                continue

            data_long.dropna(subset=["Geolocation", "Commodity"], inplace=True)

            data_long["Price"] = pd.to_numeric(data_long["Price"], errors="coerce")
            data_long.columns = data_long.columns.astype(str)
            data_long[["Geolocation", "Commodity"]] = data_long[
                ["Geolocation", "Commodity"]
            ].fillna("")

            # Enhanced imputation with validation
            def impute_prices(group):
                if group["Price"].notna().sum() == 0:
                    print(
                        f"No historical data available for {group['Commodity'].iloc[0]}. Skipping imputation."
                    )
                    return group
                try:
                    imputer = SimpleImputer(strategy="mean")
                    group.loc[:, "Price"] = imputer.fit_transform(group[["Price"]])
                    group.loc[:, "Price"] = group["Price"].round(2)
                except Exception as e:
                    print(f"⚠️ Imputation failed for {group['Commodity'].iloc[0]}: {e}")
                return group

            data_long = (
                data_long.groupby(
                    "Commodity", group_keys=False, observed=True, as_index=False
                )
                .apply(impute_prices)
                .reset_index(drop=True)
            )

            cleaned_sheets[new_sheet_name] = data_long
            print(f"Processed data from URL {url_index + 1}, sheet {sheet}")

        return cleaned_sheets

    except Exception as e:
        print(f"Critical error processing URL {url_index + 1}: {str(e)}")
        return {}