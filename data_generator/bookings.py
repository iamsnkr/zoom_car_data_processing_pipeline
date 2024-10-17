import json
import random
from datetime import datetime, timedelta


def generate_booking_data(start_booking_id=1, start_customer_id=1, count=5):
    bookings = []
    for i in range(count):
        booking_id = f"B{start_booking_id + i:03d}"
        customer_id = f"C{start_customer_id + i:03d}"
        car_id = f"CARS{random.randint(100, 999)}"

        booking_date = datetime(2024, 7, 20)
        start_time = booking_date + timedelta(days=1, hours=10)
        end_time = start_time + timedelta(hours=8)

        total_amount = round(random.uniform(50.0, 300.0), 2)
        status = random.choice(["completed", "pending", "canceled", "null"])

        booking = {
            "booking_id": booking_id,
            "customer_id": customer_id,
            "car_id": car_id,
            "booking_date": booking_date.strftime("%Y-%m-%d"),
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_amount": total_amount,
            "status": status
        }

        bookings.append(booking)

    return bookings


def save_to_file(bookings, filename):
    with open(filename, 'w') as f:
        json.dump(bookings, f)


# Generate booking data
booking_data = generate_booking_data(start_booking_id=176, start_customer_id=176, count=175)

# Create filename with current date
current_date = datetime.now().strftime("%Y%m%d")
filename = f"zoom_car_bookings_{current_date}.json"

# Save to file
save_to_file(booking_data, filename)

print(f"Booking data saved to {filename}")
