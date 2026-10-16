import datetime
import os
import stat

from hydrus.core import HydrusDateTime

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    
    CRYPTO_OK = True
    
except Exception:
    CRYPTO_OK = False

try:
    import OpenSSL
    
    OPENSSL_OK = True
    
except Exception:
    OPENSSL_OK = False

def generate_open_ssl_cert_and_key_file(cert_path, key_path):
    
    if not CRYPTO_OK:
        
        raise Exception( 'Sorry, to make your own cert, you need "cryptography" library! You should be able to get it with pip.' )
        
    
    # cribbed from here https://cryptography.io/en/latest/x509/tutorial/#creating-a-self-signed-certificate
    
    # private key
    key = rsa.generate_private_key( public_exponent = 65537, key_size = 2048 )
    
    # create a self-signed cert
    
    subject = issuer = x509.Name(
        [
        x509.NameAttribute( NameOID.COUNTRY_NAME, 'HN' ),
        x509.NameAttribute( NameOID.ORGANIZATION_NAME, 'hydrus network' ),
        x509.NameAttribute( NameOID.ORGANIZATIONAL_UNIT_NAME, os.urandom( 32 ).hex() )
        ]
    )
    
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        HydrusDateTime.now_utc()
    ).not_valid_after(
        HydrusDateTime.now_utc() + datetime.timedelta( days = 365 * 10 )
    ).add_extension(
        x509.SubjectAlternativeName( [ x509.DNSName( 'localhost' ) ] ),
        critical = False
    ).sign( key, hashes.SHA256() )
    
    cert_bytes = cert.public_bytes( serialization.Encoding.PEM )
    
    with open( cert_path, 'wb' ) as f:
        
        f.write( cert_bytes )
        
    
    os.chmod( cert_path, stat.S_IREAD )
    
    # no pass, we are full jej mode here for ease (it wants the pass prompt on service startup)
    # encryption_algorithm = serialization.BestAvailableEncryption( b"passphrase" )
    
    key_bytes = key.private_bytes(
        encoding = serialization.Encoding.PEM,
        format = serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm = serialization.NoEncryption()
    )
    
    with open( key_path, 'wb' ) as f:
        
        f.write( key_bytes )
        
    
    os.chmod( key_path, stat.S_IREAD )
