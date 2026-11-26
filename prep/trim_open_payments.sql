-- trim_open_payments.sql
-- Local prep: CMS Open Payments General Payments PY2025 (~8 GB CSV) -> one ZSTD Parquet file.
--
-- REBUILT AFTER THE FACT. I ran this step by hand in the DuckDB CLI and didn't save it.
-- This version is rebuilt from the 16 columns and types in the bronze schema and the
-- scope in my notes (U.S. physicians + non-physician practitioners, teaching hospitals out).
-- If you run it, the check at the bottom should print 16,095,794 rows.
--
-- Run from the folder holding the CSV:
--   duckdb -c ".read prep/trim_open_payments.sql"
-- Replace the file name below with the General Payments CSV you downloaded from
-- https://openpaymentsdata.cms.gov (its name changes with each CMS refresh).

COPY (
    SELECT
        CAST(Record_ID AS BIGINT)                                         AS Record_ID,
        Covered_Recipient_Type,
        CAST(Covered_Recipient_Profile_ID AS BIGINT)                      AS Covered_Recipient_Profile_ID,
        CAST(Covered_Recipient_NPI AS BIGINT)                             AS Covered_Recipient_NPI,
        Covered_Recipient_Specialty_1,
        Recipient_State,
        CAST(Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID AS BIGINT)
                                                                          AS Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID,
        Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,
        CAST(Total_Amount_of_Payment_USDollars AS DOUBLE)                 AS Total_Amount_of_Payment_USDollars,
        CAST(strptime(Date_of_Payment, '%m/%d/%Y') AS DATE)               AS Date_of_Payment,
        CAST(Number_of_Payments_Included_in_Total_Amount AS BIGINT)       AS Number_of_Payments_Included_in_Total_Amount,
        Form_of_Payment_or_Transfer_of_Value,
        Nature_of_Payment_or_Transfer_of_Value,
        Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1,
        Product_Category_or_Therapeutic_Area_1,
        CAST(Program_Year AS BIGINT)                                      AS Program_Year
    FROM read_csv('OP_DTL_GNRL_PGYR2025_<your-file>.csv', header = true, all_varchar = true)
    WHERE Covered_Recipient_Type IN ('Covered Recipient Physician',
                                     'Covered Recipient Non-Physician Practitioner')
      AND Recipient_Country = 'United States'
) TO 'clinicians_pgyr2025.parquet' (FORMAT parquet, COMPRESSION zstd);

-- Check: should match the bronze count in the notebook (16,095,794 rows, 16 columns)
SELECT COUNT(*) AS rows FROM 'clinicians_pgyr2025.parquet';
