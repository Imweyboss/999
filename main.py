import sqlite3
import re
import botocore
import requests
import logging
from bs4 import BeautifulSoup
import boto3
import json

session = boto3.Session(profile_name='default')
secretsmanager = session.client('secretsmanager')
s3 = session.resource('s3')

bucket_name = 'bucket-for-tg-chat'
db_file_name = 'ads_database.db'


def get_secret(secret_name):
    try:
        get_secret_value_response = secretsmanager.get_secret_value(SecretId=secret_name)
    except Exception as e:
        logger.error(f"Ошибка при получении секрета {secret_name}: {e}")
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            logger.error(f"Секрет {secret_name} не содержит 'SecretString'")
            return None


secrets = get_secret('prod')

TOKEN = secrets.get('TELEGRAM_BOT_TOKEN')
CHAT_ID = secrets.get('TELEGRAM_CHAT_ID')
URL = secrets.get('URL')
aws_access_key_id = secrets.get('TERRAFORM_KEY_ID')
aws_secret_access_key = secrets.get('TERRAFORM_KEY')

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('parser_apart.txt')
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s\n')
handler.setFormatter(formatter)
logger.addHandler(handler)


def download_database_from_s3():
    try:
        s3.Bucket(bucket_name).download_file(db_file_name, 'ads_database.db')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.info("Database file does not exist in S3, will be created locally.")
        else:
            raise


def upload_database_to_s3():
    s3.Bucket(bucket_name).upload_file('ads_database.db', db_file_name)


def create_database():
    conn = sqlite3.connect('ads_database.db')
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS ads (
                 id INTEGER PRIMARY KEY,
                 about TEXT,
                 total_price TEXT,
                 update_time TEXT,
                 image_url TEXT,
                 ad_url TEXT,
                 views TEXT,
                 rooms TEXT,
                 address TEXT
                 )''')

    conn.commit()
    conn.close()


def save_ad_to_database(ad_info):
    conn = sqlite3.connect('ads_database.db')
    c = conn.cursor()

    c.execute('INSERT INTO ads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
              (ad_info['id'], ad_info['about'], ad_info['total_price'], ad_info['update_time'], ad_info['image_url'],
               ad_info['ad_url'], ad_info['views'], ad_info['rooms'], ad_info['address']))

    conn.commit()
    conn.close()
    upload_database_to_s3()


def ad_exists_in_database(ad_id):
    conn = sqlite3.connect('ads_database.db')
    c = conn.cursor()

    c.execute('SELECT * FROM ads WHERE id = ?', (ad_id,))
    ad = c.fetchone()

    conn.close()

    return ad is not None


def get_ad_information():
    ads_info_list = []

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    session = requests.Session()
    response = session.get(URL, headers=headers)

    soup = BeautifulSoup(response.content, "html.parser")

    ads = soup.select('#js-ads-container ul li')
    ads = ads[:6]

    for ad in ads:
        try:
            ad_url = f"https://999.md{ad.find('a', class_='js-item-ad')['href']}"
            ad_id = int(re.search(r'\d+', ad_url).group())

            ad_response = session.get(ad_url, headers=headers)
            ad_soup = BeautifulSoup(ad_response.content, "html.parser")

            xpaths = {
                'update_time': 'div.adPage__aside__stats__date',
                'image_url': 'a.js-fancybox.mfp-zoom.mfp-image',
                'about': 'h1',
                'price': 'span.adPage__content__price-feature__prices__price__value',
                'currency': 'span.adPage__content__price-feature__prices__price__currency',
                'views': 'div.adPage__aside__stats__views',
                'address': 'span.adPage__aside__address-feature__text'
            }
            ad_info = {}
            for key, css_selector in xpaths.items():
                try:
                    element = ad_soup.select_one(css_selector)
                    if key == 'image_url':
                        ad_info[key] = element['data-src']
                    else:
                        ad_info[key] = element.text.strip()
                except:
                    ad_info[key] = None

            ad_info['total_price'] = ad_info['price'] + ' ' + ad_info['currency']
            ad_info['ad_url'] = ad_url
            ad_id = int(re.findall(r'\d+', ad_url)[-1])
            ad_info['id'] = ad_id

            about_split = ad_info['about'].split(', ')
            rooms_number_match = re.search(r'\d+', about_split[0])
            if rooms_number_match and "комнатная" in about_split[0]:
                rooms_number = rooms_number_match.group()
                ad_info['rooms'] = f"{rooms_number}-комнатная"
            else:
                ad_info['rooms'] = "Комната"

            ad_info['address'] = ad_soup.select_one(xpaths['address']).text.strip()
            address_parts = ad_info['address'].split(', ')
            second_line_address = ', '.join(address_parts[2:4])
            third_line_address = ', '.join(address_parts[4:]) if len(address_parts) > 4 else ''

            ad_info['second_line_address'] = second_line_address
            ad_info['third_line_address'] = third_line_address

            ads_info_list.append(ad_info)
        except Exception as e:
            logger.error(f"Ошибка при обработке объявления {ad}: {e}")

    return ads_info_list


session = requests.Session()

def send_telegram_message(TOKEN, CHAT_ID, ad_info):
    modified_ad_url = ad_info['ad_url'] + "#gallery-1"
    warning_message = "НЕ ОСТАВЛЯЙТЕ ЗАЛОГ БЕЗ ДОГОВОРА"
    message = f" *{ad_info['rooms']} за {ad_info['total_price']}*\n{ad_info['second_line_address']}\n📍*{ad_info['third_line_address']}*\n\n⏱️ {ad_info['update_time']}\n👁️‍🗨️ {ad_info['views']}\n\n\n ⚠️*{warning_message}*⚠️"

    if ad_info['price'] == '1':
        logger.debug("Ad has a price of 1, not sending to Telegram.")
        return
    session.post(
        url=f'https://api.telegram.org/bot{TOKEN}/sendPhoto',
        data={
            'parse_mode': 'Markdown',
            'chat_id': CHAT_ID,
            'photo': ad_info['image_url'],
            'caption': message,
            'reply_markup': json.dumps({
                "inline_keyboard": [[
                    {
                        "text": "Посмотреть 👀",
                        "url": modified_ad_url
                    }
                ]]
            })
        }
    ).json()


if __name__ == "__main__":
    download_database_from_s3()
    create_database()

    try:
        ads_info = get_ad_information()
        for ad_info in ads_info:
            logger.debug(f"Ad information: {ad_info}\n")

            if not ad_exists_in_database(ad_info['id']):
                logger.info("Ad doesn't exist in the database. Saving...")
                save_ad_to_database(ad_info)

                if ad_info['total_price'] is not None and 'Кишинёв' in ad_info['address'] and 'Кишинёв' in ad_info[
                    'about']:
                    logger.info("Sending ad to Telegram...")
                    if ad_info['price'] == '1':
                        logger.debug("Ad has a price of 1, not sending to Telegram.")
                        continue
                    send_telegram_message(TOKEN, CHAT_ID, ad_info)
                else:
                    logger.debug(
                        "Ad has no price, city is not Кишинёв or Кишинёв is not in the about text, not sending to "
                        "Telegram.")
            else:
                logger.debug("Ad already exists in the database.")
    except Exception as e:
        logger.exception(e)
