from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import serialization

class AsymmetricCipher:
    def __init__(self, private_key=None, public_key=None):
        if private_key and public_key:
            self._load_keys(private_key, public_key)
        else:
            self._generate_keys()
        
        self.padding = padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )

    def _load_keys(self, private_key, public_key):
        self.private_key = serialization.load_pem_private_key(private_key, None)
        self.public_key = serialization.load_pem_public_key(public_key)
        
    def _generate_keys(self):
        self.private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        self.public_key = self.private_key.public_key()
        
    def encrypt(self, message):
        encrypted = self.public_key.encrypt(message, self.padding)
        return encrypted

    def decrypt(self, encrypted):
        original_message = self.private_key.decrypt(encrypted, self.padding)
        return original_message

    def get_public_key(self):
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )

    def get_private_key(self):
        return self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )


# Replace with your actual keys
private_key_pem = b'-----BEGIN PRIVATE KEY-----\n...'
public_key_pem = b'-----BEGIN PUBLIC KEY-----\n...'

cipher = AsymmetricCipher(private_key_pem, public_key_pem)
message = b'A secret message'
encrypted = cipher.encrypt(message) 
decrypted = cipher.decrypt(encrypted) 
