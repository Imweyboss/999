# Web Scraper for Real Estate Listings

*Description*

This project is a Python script that scans a website with real estate listings, extracts information about new listings, and stores it in a database. This database is stored in AWS S3 Bucket. In addition, the script sends notifications about new listings to Telegram.

*Installing*

Clone the repository:
https://github.com/Imweyboss/999.git

Set up the necessary dependencies:
pip install -r requirements.txt

*Configuring*

1. Create an S3 bucket on AWS and specify its name in the bucket_name variable in the script.
2. Create a Secret in AWS Secrets Manager with the following fields: URL, TOKEN, CHAT_ID, aws_access_key_id, aws_secret_access_key and specify its name in the secrets_name variable in the script.

*Running the script*

Each time the script runs, it checks if there is a database in S3. If there is no database, it is created locally. Then the script loads the page with ads, retrieves information about new ads and stores them in the database. If a new ad meets certain criteria (e.g., address and price), the script sends a message to Telegram with information about the ad.

After each run of the script, the database is loaded back into S3 for updating.

**Licensed under**

This project is licensed under the MIT License.

Telegram channel
https://t.me/moldova_rent_apart
