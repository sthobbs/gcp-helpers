import logging
import google.cloud.logging
from google.cloud.logging_v2.handlers import CloudLoggingHandler
from google.oauth2 import service_account


class Logger():

    def __init__(self, project_id):

        self.project_id = project_id

        # pass service account key into credentials
        key_path = './service_account_key.json'
        credentials = service_account.Credentials.from_service_account_file(key_path)
        self.client = google.cloud.logging.Client(credentials=credentials,
                                                  project=project_id)

        # set up stackdriver logging
        self.client.setup_logging()
        self.logger = logging.getLogger('cloudLogger')
        self.logger.setLevel(logging.INFO)

        # add handlers to logger
        if not self.logger.handlers:

            # set up formatter
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

            # set up cloud logging handler
            handler = CloudLoggingHandler(self.client)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

            # set up console logging handler
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
