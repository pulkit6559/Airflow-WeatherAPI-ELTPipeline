from weather_pipeline.utils import _get_logger
from weather_pipeline.extract.extractor import webSource

class Extractor:
    """    
    Extractor driver Factory class that provides the drivers at runtime
    based on which type of driver is requested.
    
    Method:
    ------
    init():
        This returns the initialized driver based on user's request.
    """
    
    def __init__(self, source_type, **kwargs):
        self.logger = _get_logger(name=__name__)
        if source_type == "web":
            self.driver = webSource(**kwargs)
        else:
            raise NotImplementedError
    
    def init(self):
        return self.driver