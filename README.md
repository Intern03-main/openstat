#  Agricultural Commodities' Price Scraper
---

**Dataset Description:**  
This dataset contains scraped and processed agricultural commodity price data from PSA's website (OpenSTAT). The data is gathered per region, commodity type, and period (month or annual), and saved in a cleaned, structured CSV file.

---

##  Columns:

| Column Name       | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `Geolocation`     | The region or province where the data was collected.                       |
| `Commodity`       | The specific agricultural product (e.g., "Tomato", "Rice").                |
| `Commodity Type`  | The category of the commodity (e.g., "Vegetables", "Cereals").             |
| `Year`            | The year corresponding to the price value.                                 |
| `Period`          | The month or time period within the year (e.g., "January", "Annual").      |
| `Price`           | The average price of the commodity for the given region and time period.   |

---

## Setup & Usage

1. Install dependencies:
```py
pip install -r requirements.txt
playwright install
```
2. Make sure that the .env file contents match the MySQL database credentials you're using.
   
3. Run the scraper:
```py
python main.py
```

---

##  Cleaning Process:

- Dropped all rows and columns that are entirely empty (`NaN`).
- Removed rows containing user-defined unwanted patterns from the `.env` file (see `UNWANTED_TEXTS`).

---

##  Imputation Process:

- For commodities with missing price data:
  - We **group by each commodity**.
  - If the group contains at least one valid price, we use **mean imputation** to fill in the missing values (`NaN`) in the `Price` column.
  - This is done using `SimpleImputer(strategy='mean')` from `sklearn.impute`.
  - Prices are rounded to 2 decimal places after imputation.

>  If a commodity group contains **no historical data (i.e., all values missing)**, we skip imputation for that group.

---

##  Example Data Preview:

| Geolocation | Commodity | Price | Commodity Type   | Year    | Period   |
|-------------|-----------|-------|------------------|---------|----------|
| Abra        | Avocado   | 300   | Fruits           | 2023    | January  |
| Apayao      | Beans     | 50.75 | Beans and Legumes| 2024    | February |
| Benguet     | Corn      | 150   | Cereals          | 2025    | Annual   |

---

##  File Output:

- The cleaned data is saved as:  
  **`Scraped_Agri-Prices_<timestamp>.csv`**  
  Located in:  
  **Desktop/Agri-Price-Data_Files/**
