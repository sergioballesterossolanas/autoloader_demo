# Databricks notebook source
# MAGIC %run ./common

# COMMAND ----------

assert my_user is not None

# COMMAND ----------

import os

def create_folder(directory):
    # directory is the path to the folder to be created

    # Check if the folder already exists
    if os.path.exists(directory):
        print(f"The folder '{directory}' already exists.")
    else:
        # Create the folder
        os.makedirs(directory)
        print(f"The folder '{directory}' has been created.")


# Example usage
create_folder(path)
create_folder(path_checkpoint)


# COMMAND ----------

import csv

def dump_to_csv(data, filename):
    # data is a list of dictionaries, where each dictionary represents a row in the CSV
    # filename is the name of the CSV file to be created

    # Extract the keys from the first dictionary to be used as the header in the CSV
    header = data[0].keys()

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)

        # Write the header row
        writer.writeheader()

        # Write the data rows
        writer.writerows(data)

    print(f"Data dumped successfully to '{filename}'.")



# COMMAND ----------

# Example usage
data = [
    {"Name": "John", "Age": 30, "City": "New York"}
]
filename = path + "data_0.csv"

dump_to_csv(data, filename)

# COMMAND ----------

# Example usage
data = [
    {"Name": "Emily", "Age": 25, "City": "Los Angeles"}
]
filename = path + "data_1.csv"
dump_to_csv(data, filename)

# COMMAND ----------

# Example usage
data = [
    {"Name": "Michael", "Age": 35, "City": "New York"}
]
filename = path + "data_2.csv"

dump_to_csv(data, filename)

# COMMAND ----------

# Example usage
data = [
    {"Name": "Michael", "Age": 35, "Address": "123", "City": "New York"}
]
filename = path + "data_3.csv"

dump_to_csv(data, filename)

# COMMAND ----------

# clean
import os

def delete_files_in_directory(directory):
    # directory is the path to the directory containing the files to be deleted

    # Iterate over all the files in the directory
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        # Check if the file path points to a file (not a directory)
        if os.path.isfile(file_path):
            # Delete the file
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

    print("All files in the directory have been deleted.")


# Example usage
delete_files_in_directory(path)
delete_files_in_directory(path_checkpoint)

# COMMAND ----------

