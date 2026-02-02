import base64
import struct
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

def decrypt_packet():
    # Input data
    packet_data = {
        "from": 3581361866,
        "to": 4294967295,
        "channel": 179,
        "encrypted": "YbmTbIA6Q95zVawC",
        "id": 784053179,
        "rxTime": 1769589707,
        "rxSnr": 10.5,
        "hopLimit": 6,
        "rxRssi": -46,
        "hopStart": 6,
        "relayNode": 202
    }
    
    key_b64 = "pAV8h4y66Vm++Cq0KTwjlw=="
    
    # Decode inputs
    ciphertext = base64.b64decode(packet_data["encrypted"])
    key = base64.b64decode(key_b64)
    
    print(f"Update: Ciphertext length: {len(ciphertext)} bytes")
    print(f"Update: Key length: {len(key)} bytes (should be 16 for AES-128)")

    # Construct Nonce (16 bytes for CTR mode)
    # Based on research: using packet_id (64-bit) + from_node (32-bit) + counter (32-bit)
    # Search results suggest: Nonce (96 bits) = PacketId + FromNode.
    # Actually, let's look at the "Nonce" construction carefully.
    # Some docs say PacketID is 4 bytes. 
    # But initNonce(uint32_t fromNode, uint64_t packetId) uses 64-bit packetId.
    # The PacketID in the JSON is 32-bit int. We'll cast to 64-bit.
    
    packet_id = packet_data["id"] 
    from_node = packet_data["from"]
    
    # Layout:
    # 0-7: PacketID (Little Endian)
    # 8-11: FromNode (Little Endian)
    # 12-15: Counter (Big Endian 0)
    
    nonce_part = struct.pack('<Q', packet_id) + struct.pack('<I', from_node)
    
    # In cryptography lib, CTR mode requires the full 16-byte nonce (IV).
    # We'll set the initial counter to 0.
    # Note: CTR mode in 'cryptography' usually takes the full 16-byte initial counter block as 'nonce'.
    # We need to be careful about the endianness of the counter.
    # Usually the counter is the last 4 bytes big endian.
    
    iv = nonce_part + b'\x00\x00\x00\x00'
    
    print(f"IV (hex): {iv.hex()}")

    try:
        cipher = Cipher(algorithms.AES(key), modes.CTR(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        
        print(f"Plaintext (hex): {plaintext.hex()}")
        print(f"Plaintext (utf-8, tentative): {plaintext}")
        
        # Determine if it's a valid protobuf or text
        # If the first byte is small, it might be a protobuf field tag.
        
    except Exception as e:
        print(f"Error during decryption: {e}")

if __name__ == "__main__":
    decrypt_packet()
