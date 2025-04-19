

class ScrapeError(Exception):
    """Exception in scraper"""
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ScrapeTimeoutError(ScrapeError):
    """Timeout Exception in scraper"""
    def __init__(self, message: str):
        super().__init__(message)


class ScrapeNoWorksheetsAfterLoad(ScrapeError):
    """No worksheets after load Exception in scraper"""
    def __init__(self, message: str):
        super().__init__(message)

