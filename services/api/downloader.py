import urllib.request
import zipfile

class Dataset_downloader():
    def __init__(self, url, filename):
        self.url  = url
        self.filename = filename

    def download_dataset(self):
        dataset_zip = "dataset.zip" # file where to download
        

        urllib.request.urlretrieve(self.url, dataset_zip)

        with zipfile.ZipFile(dataset_zip, 'r') as zip_ref:
            zip_ref.extractall()
        
