from ConfigParser import SafeConfigParser

class Config():
    def __init__(self, path):
        self.config = SafeConfigParser()
        self.config.read(path)

    def get_aws_keys(self):
        key = self.config.get('aws', 'key')
        secret_key = self.config.get('aws', 'secret_key')
        return {'key': key,
                'secret': secret_key}

    def get_bucket_name(self):
        return self.config.get('aws', 'bucket_name')

    def get_encrypt_password(self):
        return self.config.get('core', 'encrypt_password')
        
