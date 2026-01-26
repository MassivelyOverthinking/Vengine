# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# CUSTOM ERRORS & EXCEPTIONS
# ---------------------------------------------------------------

class ReaderError(Exception):
    pass

class ReaderConfigError(ReaderError):
    pass

class ReaderSchemaError(ReaderError):
    pass

class ReaderBuildError(ReaderError):
    pass

class ReaderExecutionError(ReaderError):
    pass