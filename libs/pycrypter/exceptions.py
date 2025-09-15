class DecryptFileError(Exception):
    def __str__(self):
        return "Failed to decrypt the file. The wrong key may be used."

class EncryptFileError(Exception):
    def __str__(self):
        return "Data is not serializable"
