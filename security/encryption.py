from cryptography.fernet import Fernet

SENSITIVE_COLUMNS = [
    "Name", "Birthday", "City", "State", "Zip Code"
]

def get_cipher():
    key = Fernet.generate_key()
    return Fernet(key)

def encrypt_dataframe(df, cipher):
    for col in SENSITIVE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(
                lambda x: cipher.encrypt(x.encode()).decode()
            )
    return df
