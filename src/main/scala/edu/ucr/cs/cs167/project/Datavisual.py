import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Load data from CSV
file_path = "part-00000-e4dd2c3f-b029-4fcc-ae1f-16f6d237d266-c000.csv"  # Update with your actual CSV file path
df = pd.read_csv(file_path)

# Convert Date column to datetime
df["Date"] = pd.to_datetime(df["Date"], format="%m/%d/%Y %I:%M:%S %p")
df["Month-Year"] = df["Date"].dt.to_period("M")  # Extract month-year

# Count crimes per month
crime_counts = df["Month-Year"].value_counts().sort_index()

# --- 📈 Crime Trends Over Time ---
plt.figure(figsize=(12, 5))
plt.plot(crime_counts.index.astype(str), crime_counts.values, marker='o', linestyle='-', color='b')

plt.xticks(rotation=45, ha="right")
plt.xlabel("Date (Month-Year)")
plt.ylabel("Number of Crimes") 
plt.title("Monthly Crime Trends")
plt.grid(True)
plt.show()

# --- 🗺️ Crime Location Scatter Plot ---
plt.figure(figsize=(8, 6))
plt.scatter(df["x"], df["y"], c="red", alpha=0.5, edgecolors="k")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Crime Locations")
plt.grid(True)
plt.show()
