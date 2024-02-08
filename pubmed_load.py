from enum import Enum
import gzip
import os
import re
import shutil
import time
import traceback
from typing import Dict, Tuple
from bs4 import BeautifulSoup
import requests
from db_manager import SQLManager
from elasticsearch_post import ElasticPush
from elasticsearch.helpers import BulkIndexError

from parse_xml import process_xml_to_article_list
from utils import Utils


UPDATE_FILES = "https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/"
BASELINE_FILES = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"

class PubmedUpdate:
    current_location = BASELINE_FILES

    def __init__(self):
        self.db = SQLManager()
        self.ep = ElasticPush()

    @staticmethod
    def extract_number_from_filename(filename):
        # Use regex to extract number after 'n'
        match = re.search(r'n(\d+)', filename)
        if match:
            return int(match.group(1))
        return 0  # return 0 if no match is found
    
    @staticmethod 
    def locate_files(url: str, previously_seen_files):
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        file_names = [link.get("href") for link in soup.find_all('a')]
        gz_files = [f for f in file_names if f.endswith('.gz') and f not in previously_seen_files]
        return gz_files

    @staticmethod
    def find_unread_web_files(previously_seen_files):
        unread_web_files = PubmedUpdate.locate_files(BASELINE_FILES, previously_seen_files)
        if len(unread_web_files) > 0:
            PubmedUpdate.current_location = BASELINE_FILES
            return unread_web_files

        unread_web_files = PubmedUpdate.locate_files(UPDATE_FILES, previously_seen_files)
        PubmedUpdate.current_location = UPDATE_FILES
        return unread_web_files

    @staticmethod
    def download_file(url: str, save_path: str):
        with requests.get(url, stream=True) as response:
            with open(save_path, 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)

    @staticmethod
    def download_all_gz_files(gz_file_names):
        for file_name in gz_file_names:
            if not os.path.exists(file_name) and not os.path.exists(file_name[:-3]):
                full_url = PubmedUpdate.current_location + file_name
                Utils.print('Attempting download for', file_name)
                PubmedUpdate.download_file(full_url, file_name)

    @staticmethod
    def get_gz_files():
        gz_files = [file for file in os.listdir() if file.endswith('.gz')]
        gz_files = sorted(gz_files, key=PubmedUpdate.extract_number_from_filename)
        return gz_files

    @staticmethod
    def get_xml_files():
        xml_files = [file for file in os.listdir() if file.endswith('.xml')]
        xml_files = sorted(xml_files, key=PubmedUpdate.extract_number_from_filename)
        return xml_files

    @staticmethod
    def write_xml_file_from_gz(gz_file: str, max_retries: int = 2):
        backoff_time = 2
        for attempt in range(max_retries):
            try:
                with gzip.open(gz_file, 'rb') as f_in:
                    with open(gz_file[:-3], 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                return True
            except Exception as e:
                Utils.print(f"Error unpacking {gz_file} on attempt {attempt+1}: {e}", color='red')
                time.sleep(backoff_time)
                backoff_time *= 2  # Double the wait time for the next attempt
        Utils.print(f"Failed to unpack {gz_file} after {max_retries} attempts.", color='red')
        return False

    def reset_state(self, sleep_time:int=60, update_elastic=True):
        if update_db:
            self.db = SQLManager()
        if update_elastic:
            self.ep = ElasticPush()
        Utils.print(f'Sleeping for {sleep_time} seconds')
        time.sleep(sleep_time)

    def push_xml_file(self, xml_file: str, max_retries:int=2):
        list_of_articles = process_xml_to_article_list(xml_file)
        for attempt in range(max_retries):
            try:
                if self.db.push_pubmed_articles(list_of_articles):
                    Utils.print('Successfully pushed', xml_file, 'to database.', color='green')
                    break
            except Exception as e:
                Utils.print(f"Error pushing to DB on attempt {attempt+1}: {e}", color='red')
            self.reset_state(update_elastic=False, sleep_time=60*(attempt + 1))
        else:
            Utils.print(f"Failed to push to db after {max_retries} attempts.", color='red')
            return False

        for attempt in range(max_retries):
            try:
                if self.ep.bulk_insert(list_of_articles):
                    Utils.print('Successfully pushed', xml_file, 'to ElasticSearch.', color='green')
                    break
            except BulkIndexError as bie:
                Utils.print('ElasticSearch BulkIndexError. Successful inserts: {bie.successful}. Failed inserts: {bie.failed}', color='red')
                for error in bie.errors:
                    Utils.print(error)
                raise bie
            except Exception as e:
                Utils.print(f"Error pushing to Elasticsearch on attempt {attempt+1}: {e}", color='red')
                error_context = f"Context: Attempt {attempt + 1}, XML File: {xml_file}"
                Utils.print(error_context, color='red')
                Utils.print("Traceback:", color='red')
                Utils.print(traceback.format_exc(), color='red')

                self.reset_state(sleep_time=60*(attempt + 1))
        else:
            Utils.print(f"Failed to push to Elasticsearch after {max_retries} attempts.", color='red')
            return False
        
        for attempt in range(max_retries):
            try:
                # This should alter db.get_indexed_pubmed_files(), and trigger a deletion of the logged files.
                if self.db.write_file_name(xml_file + '.gz'): # Write the name of the .gz file for easy-checking when reading DB.
                    Utils.print('Successfully wrote file name', xml_file, "to database.", color='green')
                    break
            except:
                Utils.print(f"Error writing file name '{xml_file}' to database {attempt+1}: {e}", color='red')
            time.sleep(5)
        return True

    class State(Enum):
        SLEEP = 1
        DOWNLOAD_WEB_FILES = 2
        DELETE_DATA_FILES = 3
        PROCESS_GZ_TO_XML = 4
        PROCESS_XML_TO_DB = 5

    def get_state(self) -> Tuple[int, Dict]:
        gz_files = PubmedUpdate.get_gz_files()
        files_recorded_from_db = self.db.get_indexed_pubmed_files()
        unread_local_gz_files = [f for f in gz_files if f not in files_recorded_from_db]
        xml_files = PubmedUpdate.get_xml_files()

        if len(unread_local_gz_files) > 0:
            if len(xml_files) > 0:
                Utils.print('Discovered xml files', xml_files)
                return PubmedUpdate.State.PROCESS_XML_TO_DB, {'xml_files': xml_files}
            Utils.print('Discovered unread gz files', unread_local_gz_files)
            return PubmedUpdate.State.PROCESS_GZ_TO_XML, {'gz_files': gz_files}

        if len(gz_files) > 0 or len(xml_files) > 0:
            return PubmedUpdate.State.DELETE_DATA_FILES, {'gz_files': gz_files}

        unread_web_files = PubmedUpdate.find_unread_web_files(files_recorded_from_db)
        if len(unread_web_files) > 0:
            if len(unread_web_files) > 5:
                unread_web_files = unread_web_files[0:5]
            return PubmedUpdate.State.DOWNLOAD_WEB_FILES, {'unread_web_files': unread_web_files}

        return PubmedUpdate.State.SLEEP, {'duration': 60*60} # 1 hour

    def execute(self, state, data):
        if state == PubmedUpdate.State.SLEEP:
            sleep_duration = data['duration']
            Utils.print('Sleeping for', sleep_duration, "seconds...")
            time.sleep(sleep_duration)
            return
        if state == PubmedUpdate.State.DOWNLOAD_WEB_FILES:
            unread_web_files = data['unread_web_files']
            Utils.print('Found unread web files. Downloading...', unread_web_files)
            self.download_all_gz_files(unread_web_files)
            time.sleep(5)
            return
        if state == PubmedUpdate.State.PROCESS_GZ_TO_XML:
            gz_files = data['gz_files']
            Utils.print('Unpacking all .gz files ', gz_files)
            for gz_file in gz_files:
                self.write_xml_file_from_gz(gz_file)
                time.sleep(1)
            time.sleep(5)
            return
        if state == PubmedUpdate.State.DELETE_DATA_FILES:
            Utils.print('Removing all .gz and .xml files...')
            deleted_gz_list = []
            deleted_xml_list = []
            for file_name in os.listdir():
                if file_name.endswith('.gz') or file_name.endswith('.xml'):
                    try:
                        os.remove(file_name)
                        if file_name.endswith('.gz'):
                            deleted_gz_list.append(file_name)
                        else:
                            deleted_xml_list.append(file_name)
                    except Exception as e:
                        Utils.print(f'Failed to remove {file_name}: {e}', color='red')
                        continue
            Utils.print('Deleted gz files', deleted_gz_list, color='green')
            Utils.print('Deleted xml files', deleted_xml_list, color='green')
            time.sleep(10)
            return
        # state = PROCESS_XML_TO_DB
        xml_files = data['xml_files']
        for xml_file in xml_files:
            Utils.print('Pushing xml file', xml_file, '...')
            self.push_xml_file(xml_file)
            time.sleep(5)
            Utils.print('Removing xml file', xml_file, '...')
            os.remove(xml_file)
        Utils.print('Refreshing ElasticSearch index...')
        self.ep.refresh()
        time.sleep(5)

    def run(self):
        state, data = self.get_state()
        # while state != PubmedUpdate.State.SLEEP:
        while True:
            self.execute(state, data)
            state, data = self.get_state()
        
