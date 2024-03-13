from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# Generate a RSA Key Pair
private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)

# Get Public Key from the Private Key
public_key = private_key.public_key()

# Message to be encrypted
message = b"A secret message"

# Encryption
encrypted = public_key.encrypt(
    message,
    padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
    )
)

print("Encrypted message:",encrypted)

# Decryption
original_message = private_key.decrypt(
    encrypted,
    padding.OAEP(
        mgf=padding.MGF1(algorithm=hashes.SHA256()),
        algorithm=hashes.SHA256(),
        label=None
    )
)

print("Original message:", original_message)
