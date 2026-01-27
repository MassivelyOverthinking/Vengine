# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# CUSTOM ERRORS & EXCEPTIONS
# ---------------------------------------------------------------

# Reader-related Exceptions --------------------------------------------------

class ReaderError(Exception):
    pass

class ReaderConfigError(ReaderError):
    GENERAL_MESSAGE = "Configuration Error"

    def __init__(self, source: str, message: str):
        source_name = source.__class__.__name__
        full_message = f"{self.GENERAL_MESSAGE}: {source_name} | {message}"

        super().__init__(full_message)

class ReaderSchemaError(ReaderError):
    GENERAL_MESSAGE = "Schema Error"

    def __init__(self, source: str, message: str):
        source_name = source.__class__.__name__
        full_message = f"{self.GENERAL_MESSAGE}: {source_name} | {message}"

        super().__init__(full_message)

class ReaderBuildError(ReaderError):
    GENERAL_MESSAGE = "Buld/Construction Error"

    def __init__(self, source: str, message: str):
        source_name = source.__class__.__name__
        full_message = f"{self.GENERAL_MESSAGE}: {source_name} | {message}"

        super().__init__(full_message)

class ReaderExecutionError(ReaderError):
    GENERAL_MESSAGE = "Execution Error"

    def __init__(self, source: str, message: str):
        source_name = source.__class__.__name__
        full_message = f"{self.GENERAL_MESSAGE}: {source_name} | {message}"

        super().__init__(full_message)

# Waypoint-related Exceptions --------------------------------------------------

class WaypointError(Exception):
    pass

class WaypointBuildError(WaypointError):
    GENERAL_MESSAGE = "Build/Construction Error"

    def __init__(self, source: str, message: str):
        source_name = source.__class__.__name__
        full_message = f"{self.GENERAL_MESSAGE}: {source_name} | {message}"

        super().__init__(full_message)
