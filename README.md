## Playwright web-scraper that scrapes OpenSTAT of PSA's records on Wholesale Selling Prices of Agricultural Commodities.
# Agri-Price Scraper

## Project Description:
This scraper collects agricultural price data from multiple sources for various commodities. It processes the data and stores it in a clean, structured format suitable for analysis. The scraper extracts data by navigating dropdowns for different years and selecting the relevant commodity and price information.

## Columns in Scraped Data:

1. **Geolocation**: 
   - Description: The geographical location (e.g., city or region) where the commodity price is recorded.
   - Example: "Laguna", "Cebu"

2. **Commodity**: 
   - Description: The type of commodity (e.g., rice, corn).
   - Example: "Rice", "Corn"

3. **Commodity Type**: 
   - Description: The classification or category of the commodity (e.g., grains, vegetables).
   - Example: "Grains", "Fruits"

4. **Year**:
   - Description: The year the price data is recorded.
   - Example: 2023, 2024

5. **Period**: 
   - Description: The time period within the year (e.g., month or quarter) when the data is recorded.
   - Example: "January", "Q1", "August"

6. **Price**:
   - Description: The price of the commodity for the corresponding period and geolocation.
   - Example: 45.00, 55.75 (in local currency)

## How to Run the Scraper:
1. Install required dependencies:
   ```bash
   pip install -r requirements.txt
