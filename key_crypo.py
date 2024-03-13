from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

class AsymmetricCipher:
    def __init__(self):
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        self.public_key = self.private_key.public_key()
        self.padding = padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
        
    def encrypt(self, message):
        encrypted = self.public_key.encrypt(
            message,
            self.padding
        )
        return encrypted
        
    def decrypt(self, encrypted):
        original_message = self.private_key.decrypt(
            encrypted,
            self.padding
        )
        return original_message

# usage example
cipher = AsymmetricCipher()
message = b'A secret message'
encrypted = cipher.encrypt(message)
print('Encrypted message:', encrypted)
decrypted = cipher.decrypt(encrypted)
print('Decrypted message:', decrypted)
