class SchwabError(Exception):
    """
    The base class for all Schwab specific errors.
    """

    def __init__(self, status, message, headers):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers


def should_retry(error: BaseException) -> bool:
    status_code = getattr(error, "status_code", None)

    if status_code is None:
        status_code = getattr(error, "status", None)

    response = getattr(error, "response", None)
    if status_code is None and response is not None:
        status_code = getattr(response, "status_code", None)

    try:
        status_code = int(status_code)
    except (TypeError, ValueError):
        return False

    return status_code == 429 or status_code >= 500
