import pandas as pd
import sqlite3

"""
This is a data transformation script that calculates the client fees for each transaction.
input required: 
                1. 2023_all table from the database
                2. Commission_BO_ChecksUpdate table from the database
flow:
        1. Read 2023_all table from the database
        2. Join 2023_all table with Commission_BO_ChecksUpdate table
        3. Calculate client fees for each transaction
        4. Group by client fees
        5. Export the final table to csv

"""


def all_txn_commission(path_to_db):
    """
    Join 2023_all table with Commission_BO_ChecksUpdate table
    output: 2023_all_client_commission table
    """
    con = sqlite3.connect(path_to_db)
    df = pd.read_sql_query(
        f"""SELECT a."Transfer ID",
        a.[Country],
        a.Company,
        a."Payment System",
        a."Transfer Type",
        a."PS Crrcy" ,
        a."Crrcy",
        a."Transfer Date",
        CAST(REPLACE(a."Amount$", ',', '')AS REAL) "Amount in $",
        c."Deposit Commission %",
        c."Withdrawal Commission %",
        c."Deposit Fixed Commission",
        c."Withdrawal Fixed Commission",
        c."Withdrawal Fixed Commission $",
        c."Withdrawal Min Commission",
        c."Withdrawal Max Commission"
        FROM [2023_all] a
        LEFT JOIN Commission_BO_ChecksUpdate c ON a."Payment System" = c."Payment System - QV Names"
        AND c.Company = a.Company
        AND c."PS Crrcy" = a."PS Crrcy"
        WHERE
        a."is KPI" = 'KPI'
        AND a."Payment System" NOT IN ('Charity Withdrawal', 'Closing account', 'Broker to Broker', 'Discretionary deposit', 'FXTMPartners', 'Партнёрский счет', 'Alpari Partners', 'IB Transfer')""",
        con,
    )
    df.to_sql("2023_all_client_commission", con, if_exists="replace", index=False)
    con.close()
    print(df.head())
    print(df.shape)
    return df


def client_fees_no_limit(path_to_db):
    """
    Calculate client fees for each transaction
    output: 2023_all_client_fees_no_limit table
    """
    con = sqlite3.connect(path_to_db)
    df = pd.read_sql_query(
        f"""SELECT *,
        CASE WHEN "Transfer type" = 'Deposit' THEN ROUND("Amount in $" / (1 - "Deposit Commission %"/100) - "Amount in $", 2)
            WHEN "Transfer type" = 'Withdrawal' THEN ROUND("Amount in $" * "Withdrawal Commission %"/100 + CAST("Withdrawal Fixed Commission $" AS REAL) , 2)
            ELSE 0 END "Client Fees"
        FROM "2023_all_client_commission" acc""",
        con,
    )
    df.to_sql("2023_all_client_fees_no_limit", con, if_exists="replace", index=False)


def read_sqlite_table(table_name, path_to_db):
    """
    Read sqlite table into a DataFrame
    """
    con = sqlite3.connect(path_to_db)
    df = pd.read_sql_query(f"""SELECT * from '{table_name}'""", con)
    con.close()
    print(df.head())
    print(df.shape)
    return df


def groupby_client_fees(path_to_db):
    """
    Group by client fees and export the final table to csv
    """

    con = sqlite3.connect(path_to_db)
    df = pd.read_sql_query(
        f"""SELECT Company,
        SUBSTR(af."Transfer Date", 4, 8) AS "Month",
        "Payment System",
        "PS Crrcy",
        ROUND(SUM(CAST(REPLACE("Amount in $", ',', '')AS REAL)),2) "Volume $",
        COUNT(*) "Count",
        AVG("Deposit Commission %") "Deposit Commission %",
        AVG("Deposit Fixed Commission") "Deposit Fixed Commission",
        AVG("Withdrawal Commission %") "Withdrawal Commission %",
        AVG("Withdrawal Fixed Commission") "Withdrawal Fixed Commission",
        AVG("Withdrawal Fixed Commission $") "Withdrawal Fixed Commission $",
        AVG("Withdrawal Min Commission") "Withdrawal Min Commission", 
        AVG("Withdrawal Max Commission") "Withdrawal Max Commission",
        ROUND(SUM(af."Final Client Fees"),2) "Client Fees $"
        FROM "2023_all_client_fees" af
        WHERE "Payment System" NOT IN ('Charity Withdrawal', 'Closing account', 'Broker to Broker', 'Discretionary deposit', 'FXTMPartners', 'Партнёрский счет', 'Alpari Partners', 'IB Transfer')
        GROUP BY "Payment System", af."PS Crrcy", SUBSTR("Transfer Date", 4, 2), Company, "Withdrawal Commission %"
        ORDER BY "Client Fees $" DESC""",
        con,
    )
    df.to_csv("2023_all_client_fees.csv", index=False)
    df.to_sql("2023_all_client_fees_groupby", con, if_exists="replace", index=False)


def add_limit_to_client_fees(path_to_db):
    """
    Add withdrawal limit to client fees
    """
    df = read_sqlite_table("2023_all_client_fees_no_limit", "Withdrawal_fees.sqlite")
    df["Withdrawal Min Commission"] = pd.to_numeric(
        df["Withdrawal Min Commission"], errors="coerce"
    )
    df["Withdrawal Max Commission"] = pd.to_numeric(
        df["Withdrawal Max Commission"], errors="coerce"
    )
    df["Final Client Fees"] = df[
        "Client Fees"
    ]  # Initialize "Final Client Fees" column with "Client Fees" values

    # Apply conditional statements to update "Final Client Fees" column
    try:
        df.loc[
            (df["Transfer type"] == "Withdrawal")
            & (df["Withdrawal Min Commission"].notnull())
            & (df["Client Fees"] < df["Withdrawal Min Commission"]),
            "Final Client Fees",
        ] = df["Withdrawal Min Commission"]
        df.loc[
            (df["Transfer type"] == "Withdrawal")
            & (df["Withdrawal Max Commission"].notnull())
            & (df["Client Fees"] > df["Withdrawal Max Commission"]),
            "Final Client Fees",
        ] = df["Withdrawal Max Commission"]
    except ValueError:
        # Handle the ValueError as per your requirement
        pass
    con = sqlite3.connect(path_to_db)
    df.to_sql("2023_all_client_fees", con, if_exists="replace", index=False)


if __name__ == "__main__":
    all_txn_commission("Withdrawal_fees.sqlite")
    client_fees_no_limit("Withdrawal_fees.sqlite")
    add_limit_to_client_fees("Withdrawal_fees.sqlite")
    groupby_client_fees("Withdrawal_fees.sqlite")
