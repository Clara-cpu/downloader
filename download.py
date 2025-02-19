#!/home/test/venv/bin/python

import json
import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import concurrent.futures

log = logging.getLogger(__name__)

"""
Keep track of files that are downloaded, so that we do not download again. May not be complete at time of download (X)
Delete / move files already downloaded - When? 
Encrypt files again before uploading.
Delete files from local drive and source drive after uploading 
Scrape frequency???
"""

class Downloader():
    def __init__(self, m_token, m_remote_dir, m_to_download, m_local_dir, m_max_download_threads, m_to_delete, m_to_backup):
        self.TOKEN = m_token
        self.REMOTE_DIR = m_remote_dir                  # Remote root folder
        self.to_download = m_to_download
        self.LOCAL_DIR = m_local_dir                    # Local root folder
        self.MAX_THREADS = m_max_download_threads       # No. of concurrent downloads
        self.to_delete = m_to_delete
        self.to_backup = m_to_backup
        self.folders = {}                               # Folders
        self.files_to_download = []                     # List of files to download
        self.files_to_delete_or_backup = []             # List of files to delete or backup

    def get_all_metadata(self, folder='/'):
        """
        Get list of files to download

        @param folder: Full path of source folder
        
        @returns JSON response if successful
        """
        URL = 'https://api.pcloud.com/listfolder'
        data={'access_token': self.TOKEN, 'path': folder, 'recursive': True}
        resp = requests.post(URL, data=data)
        
        if resp.status_code == 200:
            return json.loads(resp.content)
        else:
            return resp
             
    def get_file_link(self, fileid):
        """
        Get download link of file

        @param fileid: File id of file to download

        @returns JSON response if successful
        """
        URL = 'https://api.pcloud.com/getfilelink'

        data={'access_token': self.TOKEN, 'fileid':fileid}
        resp = requests.post(URL, data=data)
        
        if resp.status_code == 200:
            return json.loads(resp.content)
        else:
            return resp

    def get_file_objects(self, file_objs):
        """
        Iterates and downloads list of files. Skips download if files exist locally.

        @param file_objs: Metadata of files to download
        """
        for obj in file_objs:
            if obj['isfolder'] is True:
                # Folders

                self.set_folder(obj['parentfolderid'], obj['folderid'], obj['name'])
            else:
                # Files
                
                localFileName = f"{self.LOCAL_DIR}/{self.folders[str(obj['parentfolderid'])]}/{obj['name']}"
                # Skips download if the file exists locally, i.e. already downloaded
                if os.path.exists(localFileName) and os.stat(localFileName).st_size == int(obj['size']):
                    continue

                filelink_obj = self.get_file_link(obj['fileid'])
                if filelink_obj['result'] == 0:
                    self.set_file(obj['parentfolderid'], obj['name'], obj['fileid'], obj['size'], filelink_obj)
        
            # Downloads files concurrently
            if len(self.files_to_download) >= self.MAX_THREADS:
                log.debug(f"Downloading {len(self.files_to_download)} files")
                self.download_files_concurrent()

            # Move to next set of files
            if 'contents' in obj:
                self.get_file_objects(obj['contents'])

        # Downloads remaining files
        if len(self.files_to_download) > 0:
            log.debug(f"Downloading {len(self.files_to_download)} files")
            self.download_files_concurrent()

    def download_file(self, file):
        """
        Download a single file

        @param file: File id and download url
        """
        try:
            with requests.get(file['url'], stream=True) as resp:
                resp.raise_for_status()
                with open(f"{self.LOCAL_DIR}/{file['filePath']}", 'wb') as fout:
                    for chunk in resp.iter_content(chunk_size=8192):
                        fout.write(chunk)

                    return f"{file['filePath']}"
        except requests.exceptions.RequestException as e:
            log.error('Error', e)    

    def download_files_concurrent(self):
        """
        Download files concurrently using MAX_THREADS workers
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for file in self.files_to_download:
                futures.append(executor.submit(self.download_file, file=file))
                
            for future in concurrent.futures.as_completed(futures):
                log.info(future.result())

        # Release resources
        executor.shutdown()

        if self.to_backup or self.to_delete:
            # Queue files for backup or deletion
            self.files_to_delete_or_backup = self.files_to_download.copy()

        # Clear pending files
        self.files_to_download.clear()

    def backup_files_concurrent(self):
        """
        Move files concurrently using MAX_THREADS workers
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for file in self.files_to_delete_or_backup:
                futures.append(executor.submit(self.move_file, file=file))
                
            for future in concurrent.futures.as_completed(futures):
                log.info(future.result())

        # Release resources
        executor.shutdown()
        # Clear pending files
        self.files_to_delete_or_backup.clear()

    def delete_files_concurrent(self):
        """
        Delete files concurrently using MAX_THREADS workers
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for file in self.files_to_delete_or_backup:
                futures.append(executor.submit(self.delete_file, file=file))
                
            for future in concurrent.futures.as_completed(futures):
                log.info(future.result())

        # Release resources
        executor.shutdown()
        # Clear pending files
        self.files_to_delete_or_backup.clear()

    def set_file(self, parentfolderid, filename, fileid, filesize, filelink):
        """
        Adds a file obj to the list of files to be downloads

        @param parentfolderid: Folder id of parent folder
        @param filename: Name of file
        @param fileid: File id
        @param filesize: File size (bytes)
        @param filelink: Hostnames and path (from getfilelink api)  
        """        
        file = {}
        if parentfolderid:
            file['filePath'] = f"{self.folders[str(parentfolderid)]}/{filename}"
        else:
            file['filePath'] = f"{filename}"
        
        file['fileId'] = f"{fileid}"
        file['fileSize'] = f"{filesize}"
        file['url'] = f"https://{filelink['hosts'][1]}{filelink['path']}"
        self.files_to_download.append(file)

    def set_folder(self, parentfolderid, folderid, name):
        """
        Builds folder structure and creates folders locally

        @param parentfolderid: Folder id of parent folder
        @param folderid: Folder id
        @param name: Folder name

        """
        if parentfolderid:
            self.folders[str(folderid)] = f"{self.folders[str(parentfolderid)]}/{name}"
        else:
            self.folders[str(folderid)] = f"{name}"

        # Create the folder struture locally
        os.makedirs(f"{self.LOCAL_DIR}/{self.folders[str(folderid)]}", exist_ok=True)


# Logging
rotatingHandler = RotatingFileHandler('l.log', maxBytes=500_000_000, backupCount=5)
logging.basicConfig(handlers=[rotatingHandler], encoding='utf-8', level=logging.INFO, format='%(asctime)s\t%(message)s',  datefmt='%Y-%m-%d %H:%M:%S')

if __name__ == '__main__':
    import argparse
    import configparser

    parser = argparse.ArgumentParser()
    parser.add_argument('-config')
    parser.add_argument('--subdir', default='')
    args = parser.parse_args()
    
    config = configparser.ConfigParser()
    config.read(args.config)
    pc = Downloader(
        m_token=config.get('', 'token'), 
        m_remote_dir=config.get('', 'remote_dir'), 
        m_to_download=config.getboolean('', 'download'),
        m_local_dir=config.get('', 'local_dir'), 
        m_max_download_threads=config.getint('', 'max_download_threads'),
        m_to_delete=config.getboolean('', 'delete'),
        m_to_backup=config.getboolean('', 'backup'),
    )
    
    try:
        resp = pc.get_all_metadata(pc.REMOTE_DIR + args.subdir.rstrip("/")) # Remove trailing slash, else api will return not found
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
        
    if pc.to_download:
        if isinstance(resp, dict) and resp['result'] == 0:
            if 'metadata' in resp:
                # Root folder
                if resp['metadata']['isfolder'] is True:
                    pc.set_folder(None, resp['metadata']['folderid'], resp['metadata']['path'])
            
                try:
                    pc.get_file_objects(resp['metadata']['contents'])
                except requests.exceptions.RequestException as e:
                    raise SystemExit(e)
        else:
            print(f"[Error] {resp}")

    if len(pc.files_to_download) == 0:
        print('No remaining files')
