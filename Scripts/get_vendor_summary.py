import pandas as pd
import sqlite3
import logging
from ingestion_db import ingest_db

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):

    vendor_sales_summary = pd.read_sql("""WITH FrieghtSummary AS(
    SELECT
        VendorNumber , sum(Freight) as FreightCost
        from vendor_invoice
        group by VendorNumber
    ),
    Purchases_summary as(
        SELECT 
          P.VendorName , 
          P.VendorNumber,
          P.Brand,
          P.Description,
          P.PurchasePrice ,
          PP.Volume,
          PP.price as Actual_Price,
          SUM(P.Quantity) as TotalPurchaseQuantity,
          SUM(P.Dollars) as TotalPurchaseDollars
          From purchases P
          JOIN purchase_prices  PP on P.Brand = PP.Brand
          where P.PurchasePrice > 0 
          group by P.VendorNumber,P.VendorName, P.Brand , P.Description,P.PurchasePrice ,PP.price, PP.Volume
    ),
    sales_summary as (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesDollars) as TotalSalesDollars,
            SUM(SalesPrice) as TotalSalesPrice,
            SUM(SalesQuantity) as TotalSalesQuantity,
            SUM(ExciseTax) as TotalExciseTax
            from sales
            group by VendorNo , Brand 
    )
    SELECT 
          ps.VendorName , 
          ps.VendorNumber,
          ps.Brand,
          ps.Description,
          ps.PurchasePrice ,
          ps.Actual_Price,
          ps.Volume,
          ps.TotalPurchaseQuantity,
          ps.TotalPurchaseDollars,
          ss.TotalSalesDollars,
          ss.TotalSalesPrice,
          ss.TotalSalesQuantity,
          ss.TotalExciseTax,
          fs.FreightCost
    FROM Purchases_summary ps
    LEFT JOIN sales_summary ss
        on ps.VendorNumber = ss.VendorNo
        and ps.Brand = ss.Brand
    Left join FrieghtSummary fs
        on ps.VendorNumber = fs.VendorNumber
    order by ps.TotalPurchaseDollars Desc""",conn)

    return vendor_sales_summary




def clean_data(df):


    df['Volume'] = df['Volume'].astype('float')

    df.fillna(0, inplace = True)
    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()
    
    vendor_sales_summary['Gross Profit'] = vendor_sales_summary['TotalSalesDollars'] - vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin']  = (vendor_sales_summary['Gross Profit'] / vendor_sales_summary['TotalSalesDollars'] ) *100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity'] / vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalesToPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

    return df



if __name__ == '__main__':
    # creating database connection
    conn = sqlite3.connect('inventory.db')

    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data.....')
    ingest_db(clean_df, 'vendor_sales_summary', conn)
    logging.info('Completed')
