from contextlib import contextmanager
from insights_messaging.downloaders.s3 import S3Downloader as S3Downloader

class IDPDownloader(S3Downloader):
    def __init__(self, **kwargs):
        if not kwargs['access_key']:
            raise ConfigurationError("Access Key environment variable not set.")
        if not kwargs['secret_key']:
            raise ConfigurationError("Secret Key environment variable not set.")
        if not kwargs['endpoint_url']:
            raise ConfigurationError("Endpoint environment variable not set.")
        if not kwargs['bucket']:
            raise ConfigurationError("Bucket environment variable not set.")
        self.access_key = kwargs['access_key']
        self.secret_key = kwargs['secret_key']
        self.endpoint_url = kwargs['endpoint_url']
        self.bucket = kwargs['bucket']
        super().__init__(key=self.access_key,secret=self.secret_key,client_kwargs={'endpoint_url':self.endpoint_url})

    
    @contextmanager
    def get(self,path):
        yield super().get(f"{self.bucket}/{path}")

class ConfigurationError(Exception):
    pass