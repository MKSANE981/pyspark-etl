"""Generate sample transactions CSV for ETL demo."""
import pandas as pd
import random
from datetime import date, timedelta

random.seed(42)

categories = ["electronics", "clothing", "food", "sports", "books"]
regions = ["north", "south", "east", "west"]

rows = []
start = date(2023, 1, 1)
for i in range(500):
    d = start + timedelta(days=random.randint(0, 364))
    rows.append({
        "transaction_id": i + 1,
        "date": d.isoformat(),
        "category": random.choice(categories),
        "region": random.choice(regions),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "customer_name": random.choice(["Alice", "Bob", "Charlie", "Diana", "Eve",
                                        " alice", "BOB", "DIANA "]),  # intentional noise
    })

# Add a few duplicates and nulls to demo quality checks
rows[10]["amount"] = None
rows[20]["category"] = None
rows.append(rows[5].copy())   # duplicate row

df = pd.DataFrame(rows)
df.to_csv("data/transactions.csv", index=False)
print(f"Saved {len(df)} rows to data/transactions.csv")
