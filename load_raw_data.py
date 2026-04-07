import pandas as pd
import random
from datetime import datetime, timedelta
import os

def generate_data():
    data = []

    for _ in range(100):  # incremental batch
        data.append({
            "trans_id": random.randint(1000, 9999),
            "trans_timestamp": datetime.now() - timedelta(days=random.randint(0, 5)),
            "acct_number": f"ACCT{random.randint(100000,999999)}",
            "customer_id": f"CUST{random.randint(1000,9999)}",
            "trans_type": random.choice(["DEBIT", "CREDIT", "INVALID"]),
            "amount": round(random.uniform(-500, 10000), 2)
        })

    df = pd.DataFrame(data)

    os.makedirs("data/raw", exist_ok=True)
    file_path = f"data/raw/transactions_{datetime.now().date()}.csv"

    df.to_csv(file_path, index=False)
    print(f"Data generated: {file_path}")
