import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from babel.numbers import format_currency
import numpy as np

import mysql.connector
from mysql.connector import Error

from sqlalchemy import create_engine

engine = create_engine(
    'postgresql://orpctbsqvqtnrx:530428203217ce11da9eb9586a5513d0c7fe08555c116c103fd43fb78a81c944@ec2-34-202-53-101.compute-1.amazonaws.com:5432/d46bn1u52baq92', \
    echo=False)


txn_data = pd.read_sql_query('''
    WITH base AS (
        SELECT a.*, 
               DATE(b.\"CreationTime\") AS "registration_date", 
               DATE(b.\"LastSessionDate\") AS "last_login_date", 
               DATE(a.\"CreationTime\") AS "txn_date", 
               CASE 
                   WHEN a.\"CurrencyId\" = 'EUR' THEN \"Amount\" 
                   WHEN a."CurrencyId\" = 'USD' THEN \"Amount\" * 0.92 
                   ELSE \"Amount\" 
               END AS "Amount_Euro" 
        FROM customer_transactions_betfoxx AS a 
        LEFT JOIN customers_betfoxx AS b ON a.\"UserName\" = b.\"UserName\" 
        WHERE a.\"State\" IN (8, 12)
    ),
    life_time_txns AS (
        SELECT \"UserName\",  
               COUNT(DISTINCT \"Id\") AS "Life_Time_Dpst_Cnt", 
               SUM("Amount_Euro") AS "Life_Time_Dpst_Value", 
               MAX("txn_date") AS "last_Txn_Date" 
        FROM base
        GROUP BY 1
    ),
    prev_day AS (
        SELECT a.\"UserName\", 
               a.\"FirstName\", 
               a.\"LastName\", 
               a.\"CountryCode\", 
               a.\"AffiliateId\", 
               a."registration_date", 
               a."last_login_date", 
               a."txn_date",
               a."PaymentSystemId",
               SUM(a."Amount_Euro") AS "dpst_amount", 
               COUNT(\"Id\") AS "dpst_cnt" 
        FROM base AS a 
        WHERE "txn_date" = CURRENT_DATE - 1 
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,9
    )
    SELECT a."UserName", a."FirstName", a."LastName", a."CountryCode",
           a."AffiliateId", a."registration_date", a."last_login_date",
           case when a."PaymentSystemId" = 326 then 'Card' 
                when a."PaymentSystemId" = 147 then 'Crypto' 
                when a."PaymentSystemId" = 324 then 'Crypto Bridge' 
                else 'Others' end as "Payment_Method",
           b."last_Txn_Date", 
           b."Life_Time_Dpst_Cnt", 
           b."Life_Time_Dpst_Value", 
           a."txn_date", a."dpst_amount", a."dpst_cnt"
    FROM prev_day AS a  
    LEFT JOIN life_time_txns AS b ON a."UserName" = b."UserName"
''', con=engine)


mailer_df = txn_data.fillna(0)

mailer_df["Life_Time_Dpst_Value"] = mailer_df["Life_Time_Dpst_Value"].apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

total = mailer_df[['dpst_amount','dpst_cnt']].apply(np.sum)

total['UserName'] = 'Total'

DS_Overall = pd.concat([mailer_df,pd.DataFrame(total.values, index=total.keys()).T], ignore_index=True)

DS_Overall.rename(columns={'dpst_amount':'Deposit_Amount','dpst_cnt':'Deposit_Count'},inplace = True)

DS_Overall["Deposit_Amount"] = DS_Overall["Deposit_Amount"].apply(lambda x: format_currency(x, currency="EUR", locale="nl_NL"))

date = dt.datetime.today()-  timedelta(1)
date_1 = date.strftime("%m-%d-%Y")
filename = f'Betfoxx_Daily_Deposits_{date_1}.xlsx'



with pd.ExcelWriter(filename) as writer:
    DS_Overall.reset_index(drop=True).to_excel(writer, sheet_name="Betfoxx",index=False)
    
sub = f'Betfoxx_Deposits_Summary_{date_1}'

# Write the DataFrame to Excel
with pd.ExcelWriter(filename, engine='openpyxl') as writer:
    DS_Overall.reset_index(drop=True).to_excel(writer, sheet_name="Betfoxx", index=False)

# Open the workbook again to adjust column widths
with pd.ExcelWriter(filename, engine='openpyxl', mode='a') as writer:
    # Access the workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets['Betfoxx']

    # Adjust the width of each column based on the length of the column names
    for column in worksheet.columns:
        max_length = 0
        column_name = column[0].column_letter
        for cell in column:
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        adjusted_width = (max_length + 2) * 1.2
        worksheet.column_dimensions[column_name].width = adjusted_width


#!/usr/bin/python
import smtplib,ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders

def send_mail(send_from,send_to,subject,text,server,port,username='',password=''):
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(recipients)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = subject
    msg.attach(MIMEText(text))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(filename, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename={filename}')
    msg.attach(part)

    #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    #SSL connection only working on Python 3+
    smtp = smtplib.SMTP_SSL(server, port)
    smtp.login(username,password)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.quit()
    
subject = sub
body = f"Hi,\n\n Attached contains the Summary  of customers who made deposits on {date_1} for Betfoxx \n\nThanks,\nSaketh"
sender = "sakethg250@gmail.com"
recipients = ["saketh.sgc@gmail.com","alberto@crystalwg.com","isaac@crystalwg.com","sebastian@crystalwg.com","saketh@crystalwg.com"]
password = "xjyb jsdl buri ylqr"

send_mail(sender, recipients, subject, body, "smtp.gmail.com", 465,sender,password)