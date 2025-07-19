import pandas as pd
# load transaction_data_finalpart2.csv
df = pd.read_csv("data/data_generation/transaction_data/transaction_data_finalpart3_updated.csv")

# Convert Txn Date to datetime with specific format YYYY/MM/DD, if some rows are not in this format remove it
df["Txn Date"] = pd.to_datetime(df["Txn Date"], format="%Y/%m/%d", errors="coerce")

# Drop rows with invalid Txn Date, and print count of deleted rows
deleted_rows = df["Txn Date"].isna().sum()
df = df.dropna(subset=["Txn Date"])
print(f"Deleted {deleted_rows} rows with invalid Txn Date")

# Convert Txn Date back to string in %Y/%m/%d format
df["Txn Date"] = df["Txn Date"].dt.strftime("%Y/%m/%d")

# get user IDs
user_ids = df["User Id"].unique()

for userid in user_ids:
    # filter dataframe for each user and store original indices
    mask = df["User Id"] == userid
    user_df = df[mask].copy()
    
    # Sort by date within each user's transactions
    user_df = user_df.sort_values(by="Txn Date").reset_index(drop=True)
    
    # Create transaction numbers for each month separately
    user_df["month_num"] = pd.to_datetime(user_df["Txn Date"], format="%Y/%m/%d").dt.month
    user_df["txn_num"] = user_df.groupby("month_num").cumcount() + 1

    # Txn ID format: T + user_id + month + sequential number (001, 002, etc.)
    user_df["Txn ID"] = "T" + user_df["User Id"].astype(str) + user_df["month_num"].astype(str).str.zfill(2) + user_df["txn_num"].astype(str).str.zfill(3)
    
    # Drop temporary columns
    user_df = user_df.drop(columns=["month_num", "txn_num"])

    # Update Txn ID in original df using boolean mask
    df.loc[mask, "Txn ID"] = user_df["Txn ID"].values

# save the updated dataframe to a new csv file
df.to_csv("data/data_generation/transaction_data/transaction_data_finalpart3_updated_txnid.csv", index=False)

# # Txn Date change year to 2023, date format is 2023/04/06, if its in 2023-04-06 on this format first convert it to 2023/04/06.
# df["Txn Date"] = df["Txn Date"].str.replace("-", "/")

# # if df["Txn Category"]=INCOME_WAGES and Txn Amount is positive, change it to negative
# df.loc[(df["Txn Category"] == "INCOME_WAGES") & (df["Txn Amount"] > 0), "Txn Amount"] *= -1

# # rest of "Txn Category" should have positive values
# df.loc[df["Txn Category"] != "INCOME_WAGES", "Txn Amount"] = df.loc[df["Txn Category"] != "INCOME_WAGES", "Txn Amount"].abs()

# set values of Txn ID  TU101001 – usernumber+monthnumber+transaction number(where number should be in 3 digit ie 001, 002)
# df["Txn ID"] = "T" + df["User Id"].astype(str) + df["Txn Date"].dt.month.astype(str).str.zfill(2) + (df.index + 1).astype(str).str.zfill(3)



