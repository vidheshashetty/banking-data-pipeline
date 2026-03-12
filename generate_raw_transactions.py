import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
num_records = 200000
data = []

for i in range(num_records):
    trans_id = f"TNX{1000000 + i}"
    trans_timestamp = datetime.now() - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
    acct_number = f"ACC{random.randint(10000,99999)}"
    customer_id = f"CUST{random.randint(1000,9999)}"
    trans_type = random.choice(["CREDIT", "DEBIT"])
    amount = round(random.uniform(100,999999), 2)
    currency = "INR"
    sender_bank = random.choice(["HDFC", "AXIS", "ICICI", "SBI", "HSBC", "Barclays"])
    receiver_bank = random.choice(["HDFC", "AXIS", "ICICI", "SBI", "HSBC", "Barclays"])
    status = random.choice(["SUCCESS", "FAILED"])
    load_date = datetime.now().date()

    data.append([trans_id, trans_timestamp, acct_number, customer_id, trans_type, amount, currency, sender_bank, receiver_bank, status, load_date])

df = pd.DataFrame(data, columns=["trans_id", "trans_timestamp", "acct_number", "customer_id", "trans_type",
                                     "amount", "currency", "sender_bank", "receiver_bank", "status", "load_date"])

neg_sample = df.sample(frac=0.02)
df.loc[neg_sample.index, "amount"] = -100

null_sample = df.sample(frac=0.01)
df.loc[null_sample.index, "trans_id"] = None

invalid_type_sample = df.sample(frac=0.01)
df.loc[invalid_type_sample.index, "trans_type"] = "TRANSFER"

dup_sample = df.sample(frac=0.005)
df = pd.concat([df,dup_sample])

df.to_csv("banking_data.csv", index=False)

print("A data set with 200K+ rows generated Successfully")


