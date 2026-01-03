# ---------------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------------

# ---------------------------------------------------------------
# FORMATTING LISTS
# ---------------------------------------------------------------

return_format_list = {
    "json": "json",
    "yaml": "yaml",
    "toon": "toon"
}

error_severity_list = {
    "ok": 0,
    "debug": 1,
    "error": 2,
    "fatal": 3
}

def retrieve_return_format(input: str) -> str:
    if not isinstance(input, str):
        raise TypeError(f"Input string must be of Type: str - Currenty type: {type(input)}")
    
    input = input.lower()
    final_format = return_format_list.get(input)

    if final_format is None:
        raise ValueError(
            f"Format type: {input} not supported!\n List of supported format types: {return_format_list.keys}"
        )
    else:
        return final_format

def retrieve_conduit_severity(input: str) -> int:
    if not isinstance(input, str):
        raise TypeError(f"Input string must be of Type: str - Currenty type: {type(input)}")
    
    input = input.lower()
    final_severity = error_severity_list.get(input)

    if final_severity is None:
        raise ValueError(
            f"Severity type: {input} not supported!\n List of supported severity types: {error_severity_list.keys}"
        )
    else:
        return final_severity