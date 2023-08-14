# exceptions.py

class LPFValidationError(Exception):
    pass

class DelimValidationError(Exception):
    pass

class DelimInsertError(Exception):
    pass

class DataAlreadyProcessedError(DelimInsertError):
    pass

