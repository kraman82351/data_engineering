import csv
from cryptography.fernet import Fernet  

# Replace this with your actual encryption key
key = Fernet.generate_key()  # Use the same key for encryption and decryption
cipher_suite = Fernet(key)

# Function to encrypt sensitive data
def encrypt_data(data):
    return cipher_suite.encrypt(data.encode()).decode()

# Function to mask sensitive data
def mask_data(data):
    return '*' * len(data)

# Function to encrypt and mask the entire CSV file
def encrypt_and_mask_csv(input_file, output_file):
    with open(input_file, 'r') as input_csv_file:
        reader = csv.reader(input_csv_file)
        with open(output_file, 'w', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
            for row in reader:
                encrypted_and_masked_row = [encrypt_data(cell) if index == 0 else mask_data(cell) for index, cell in enumerate(row)]
                writer.writerow(encrypted_and_masked_row)


# Example usage:
input_csv_file = 'C:/Users/Aman/Desktop/claim management system/data_engineering/CSV/datasheet.csv'
encrypted_and_masked_csv_file = 'C:/Users/Aman/Desktop/claim management system/data_engineering/CSV/encrypted_data.csv'
encrypt_and_mask_csv(input_csv_file, encrypted_and_masked_csv_file)
print("CSV file encrypted and masked successfully.")


