import json
import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import concurrent.futures

log = logging.getLogger(__name__)

class Downloader():
    def __init__(self, m_token, m_remote_dir, m_local_dir, m_max_download_threads):
        self.TOKEN = m_token
        self.REMOTE_DIR = m_remote_dir                  # Remote folder
        self.LOCAL_DIR = m_local_dir                    # Local folder
        self.MAX_THREADS = int(m_max_download_threads)  # No. of concurrent downloads
        self.folders = {}                               # Folders
        self.files = []                                 # List of files to download

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
        Iterates and downloads list of files

        @param file_objs: Metadata of files to download
        """
        for obj in file_objs:
            if obj['isfolder'] is True:
                # Folders
                self.set_folder(obj['parentfolderid'], obj['folderid'], obj['name'])
            else:
                # Files
                filelink_obj = self.get_file_link(obj['fileid'])
                if filelink_obj['result'] == 0:
                    self.set_file(obj['parentfolderid'], obj['name'], filelink_obj)
        
            # Downloads files concurrently
            if len(self.files) >= self.MAX_THREADS:
                log.debug(f"Downloading {len(self.files)} files")
                self.download_files_concurrent()

            if 'contents' in obj:
                self.get_file_objects(obj['contents'])

        # Downloads remaining files
        if len(self.files) > 0:
            log.debug(f"Downloading {len(self.files)} files")
            self.download_files_concurrent()

    """
    Keep track of files that are downloaded, so that we do not download again.
    Delete files already downloaded, so that we do not download again.
    Encrypt file again before uploading.
    Delete file from local drive and source drive after uploading 

    """
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

                    return f"Downloaded {self.LOCAL_DIR}/{file['filePath']}"
        except requests.exceptions.RequestException as e:
            log.error('Error', e)    

    def download_files_concurrent(self):
        """
        Download files concurrently using MAX_THREADS workers
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_THREADS) as executor:
            futures = []
            for file in self.files:
                futures.append(executor.submit(self.download_file, file=file))
                
            for future in concurrent.futures.as_completed(futures):
                log.info(future.result())

        # Release resources
        executor.shutdown()
        # Clear pending files
        self.files.clear()

    def set_file(self, parentfolderid, name, filelink):
        """
        Adds file to list of files to be downloads

        @param parentfolderid: Folder id of parent folder
        @param name: Name of file
        @param filelink: Hostnames and path (from the getfilelink api)  
        """        
        file = {}
        if parentfolderid:
            file['filePath'] = f"{self.folders[str(parentfolderid)]}/{name}"
        else:
            file['filePath'] = f"{name}"

        file['url'] = f"https://{filelink['hosts'][1]}{filelink['path']}"
        self.files.append(file)

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
    import configparser

    config = configparser.ConfigParser()
    config.read('.conf')
    pc = Downloader(config.get('funan', 'token'), config.get('funan', 'remote_dir'), config.get('funan', 'local_dir'), config.get('funan', 'max_download_threads'))

    resp = pc.get_all_metadata(pc.REMOTE_DIR)
    if isinstance(resp, dict) and resp['result'] == 0:
        if 'metadata' in resp:
            # Root folder
            if resp['metadata']['isfolder'] is True:
                pc.set_folder(None, resp['metadata']['folderid'], resp['metadata']['path'])

            pc.get_file_objects(resp['metadata']['contents'])

    if len(pc.files) == 0:
        print('No remaining files')
    
    print('Done')
