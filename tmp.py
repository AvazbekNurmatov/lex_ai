import pickle
import csv

# Load the pickle file
with open("all_ids1.pkl", "rb") as f:
    data = pickle.load(f)

# Save to CSV file
with open("numbers.csv", "w", newline='') as csvfile:
    writer = csv.writer(csvfile)

    # If it's a list of numbers, write each number in a new row
    for number in data:
        writer.writerow([number])
