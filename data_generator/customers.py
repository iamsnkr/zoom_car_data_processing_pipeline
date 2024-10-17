import json
import random
from faker import Faker
from datetime import datetime

fake = Faker()


def generate_customer_data(start_customer_id=1, count=5):
    customers = []
    for i in range(count):
        customer_id = f"C{start_customer_id + i:03d}"
        name = f"{fake.name()}"  # Placeholder name
        email = f"{fake.email()}"
        phone_number = f"{fake.phone_number()}"  # 10-digit phone number
        signup_date = datetime(2024, 1, 15).strftime("%Y-%m-%d")
        status = random.choice(["active", "inactive", "null"])

        customer = {
            "customer_id": customer_id,
            "name": name,
            "email": email,
            "phone_number": phone_number,
            "signup_date": signup_date,
            "status": status
        }

        customers.append(customer)

    return customers


def save_to_file(customers, filename):
    with open(filename, 'w') as f:
        json.dump(customers, f)


# Generate customer data
customer_data = generate_customer_data(start_customer_id=176, count=150)

# Create filename with current date
current_date = datetime.now().strftime("%Y%m%d")
filename = f"zoom_car_customers_{current_date}.json"

# Save to file
save_to_file(customer_data, filename)

print(f"Customer data saved to {filename}")
