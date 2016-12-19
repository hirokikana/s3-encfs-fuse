import base64

class Encryptor():
    def __init__(self, password):
        self.password = password
        
    def deencrypt(self, content):
        password_length = len(self.password)
        deencrypted = []
        for index in range(0, len(content), password_length):
            deencrypted_parts =  [chr(ord(i) ^ ord(j)) for (i,j) in zip(content[index:index+password_length],self.password)]
            deencrypted += deencrypted_parts
        return base64.b64decode("".join(deencrypted))

    def encrypt(self, content):
        encoded = base64.b64encode(content)
        password_length = len(self.password)
        encrypted = []
        for index in range(0, len(encoded), password_length):
            encrypted_parts =  [chr(ord(i) ^ ord(j)) for (i,j) in zip(encoded[index:index+password_length],self.password)]
            encrypted += encrypted_parts
        return "".join(encrypted)
    
