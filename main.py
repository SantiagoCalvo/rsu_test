import os
import logging
from time import sleep
import threading

import google.cloud.logging
from google.cloud import storage
from google.cloud import bigquery
from dotenv import load_dotenv
import sentry_sdk
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


def sleeping(secs=3, silent=False):
    if not silent:
        logging.info(f'Sleeping...{secs} secs')
    sleep(secs)


def truncate_query(client, dataset, tablename, GCP_PROJECT_ID):
    '''Truncate to simulate the create or replace effect in BigQuery
    '''

    # TODO: avoid exception when table is already truncated

    # QUERY = f"TRUNCATE TABLE {GCP_PROJECT_ID}.{dataset}.{tablename}"
    # query_job = client.query(QUERY)

    return True


def insert_urls_to_gcp(rows_to_insert, GCP_PROJECT_ID):
    '''
    Insert BBL and its URLS to GCP

    Docs: https://cloud.google.com/bigquery/docs/samples/bigquery-table-insert-rows?hl=es-419
    '''

    errors = client.insert_rows_json(
        f'{GCP_PROJECT_ID}.processing.RSU_URLs',
        rows_to_insert
    )  # Make an API request.

    if errors == []:
        logging.info("New rows have been added.")
        return True
    else:
        logging.warning("Encountered errors while inserting rows: {}".format(errors))
    return False


def _convert_to_array_of_structs(units):
    the_array = []

    for key, value in units.items():
        data_row = dict()
        data_row['year'] = key
        for quarter, link in value.items():
            data_row[quarter] = link
        the_array.append(data_row)
    return the_array


def gather_links(chrome_options, Borough, Block, Lot, bbl, RSU_LINK):
    logging.info(f'* Working with BBL: {bbl}')
    year_counter = 0
    index = 2
    units = dict()

    driver = webdriver.Chrome('chromedriver', options=chrome_options)
    driver.get(RSU_LINK)

    sleeping(2, silent=True)

    selenium_wait = WebDriverWait(driver, 10)
    button_agree = selenium_wait.until(
        EC.presence_of_element_located((By.ID, "btAgree"))
    )
    button_agree.click()

    selenium_wait.until(
        EC.presence_of_element_located((By.ID, "inpParid"))
    )

    Borough_select = Select(driver.find_element(By.ID, "inpParid"))
    Block_input = driver.find_element(By.ID, "inpTag")
    lot_input = driver.find_element(By.ID, "inpStat")

    Block_input.send_keys(Block)
    lot_input.send_keys(Lot)
    Borough_select.select_by_value(Borough)

    driver.find_element(By.ID, "btSearch").click()

    sleeping(3, silent=True)

    try:
        side_menu = selenium_wait.until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="sidemenu"]/ul/li[7]/a'))
        )
    except TimeoutException:
        logging.info("----- TimeoutException for XPATH: '//*[@id='sidemenu']/ul/li[7]/a'  -------")
        logging.info('Trying with search results now')

        try:
            search_results_path = f'//table[@id="searchResults"]/tbody/tr[@class="SearchResults"]/td/div[text() = "{bbl}"]'
            search_result_item = selenium_wait.until(
                EC.presence_of_element_located((By.XPATH, search_results_path))
            )
        except TimeoutException:
            logging.info('----- Another TimeoutException')

            paragraph = '//p[contains(., "Your search did not find any records.")]'
            selenium_wait.until(
                EC.presence_of_element_located((By.XPATH, paragraph))
            )

            # No records found, therefore units are empty
            return units
        else:
            search_result_item.click()

    side_menu = selenium_wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="sidemenu"]/ul/li[7]/a'))
    )

    side_menu.click()

    sleeping(3, silent=True)

    while year_counter < 27:

        path_table_rows = f'//table[@id="Property Tax Bills"]/tbody/tr[{index}]/td[1]'

        selenium_wait.until(
            EC.presence_of_element_located((By.XPATH, path_table_rows))
        )

        table_rows = driver.find_element(By.XPATH, path_table_rows)

        if table_rows.text:
            # logging.info(index, '- year: ', table_rows.text) # i.e: 2022-2023
            year = table_rows.text

            try:
                units[year]
            except KeyError:
                # only the first time
                units[year] = dict()

            path_link = f'//table[@id="Property Tax Bills"]/tbody/tr[{index}]/td[3]/a'

            result = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, path_link))
            )

            if result:
                link_year = driver.find_element(By.XPATH, path_link)

                path_quarter = path_link + '/font/u'
                quarter_of_year = driver.find_element(By.XPATH, path_quarter)
                quarter_text = quarter_of_year.text.strip().split(':')[0]
                units[year][quarter_text] = link_year.get_attribute("href")
            else:
                logging.warning(f"Index {index} Doesn't have link__")

        year_counter += 1
        index += 1

    driver.quit()

    new_units = _convert_to_array_of_structs(units)
    return new_units


if __name__ == '__main__':
    # Configure logging

    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging()

    # Load env variables
    filename = 'bankruptcy/.env'

    storage_client = storage.Client()
    bucket = storage_client.get_bucket('skw-data-lake')
    blob = bucket.blob(filename)
    blob.download_to_filename('.env')

    load_dotenv()

    GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
    GCP_TOKEN = os.getenv('GCP_TOKEN')
    # SENTRY_DSN = os.getenv('SENTRY_DSN')
    RSU_LINK = os.getenv('RSU_LINK')

    sentry_sdk.init(
        dsn="https://7044853c7f7a452599d5a98060f21bff@o1357981.ingest.sentry.io/6644795",
        traces_sample_rate=1.0
    )

    logging.info(f'GCP_PROJECT_ID set to: {GCP_PROJECT_ID}')
    logging.info(f'GCP_TOKEN set to: {GCP_TOKEN}')
    logging.info(f'RSU_LINK set to: {RSU_LINK}')

    # Bigquery Query configurtion

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # truncate_query(client, processing, RSU_URLs, GCP_PROJECT_ID)

    insert_counter = 0

    QUERY = f"SELECT * FROM {GCP_PROJECT_ID}.staging.RSU LIMIT 1357 OFFSET 1388"
    query_job = client.query(QUERY)

    # Setting up Selenium
    thread_local = threading.local()

    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    for row in query_job:
        sleeping(5, silent=True)
        Borough = str(row['borocode'])
        Block = row['block']
        Lot = row['lot']
        bbl = row['bbl']

        units = gather_links(
            chrome_options,
            Borough,
            Block,
            Lot,
            bbl,
            RSU_LINK
        )

        if not units:
            insert_counter += 1
            logging.info(
                f'=== No records were found. Insert counter: {insert_counter} ==='
            )
            continue

        rows_to_insert = [
            {'bbl': bbl, 'links': units}
        ]

        insert_data_status = insert_urls_to_gcp(rows_to_insert, GCP_PROJECT_ID)

        insert_counter += 1
        logging.info(
            f'=== Insert counter: {insert_counter} ==='
        )

        if not insert_data_status:
            logging.warning('_____ An error occurred ______')
            logging.info('========= Units gathered ==========')
            logging.info(units)

    logging.info('_________________ end of scraping ___________________')
