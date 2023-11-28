import abc
import requests
from tqdm import tqdm
from pathlib import Path

from weather_pipeline.utils import _get_logger


class BaseSource(metaclass=abc.ABCMeta):
    """BaseSource
    Base class to define the interface of all the sources
    that can be added to the pipeline.
    
    Methods:
    -------
    1. get():
        Abstract method beared by all the sources of extraction
        to define a single point of fetching content.
    """
    def __init__(self, _name:str=__name__):
        self.logger= _get_logger(name=_name)
    
    @abc.abstractmethod
    def get(self):
        raise NotImplementedError


class webSource(BaseSource):
    """webSource
    
    Deriver from BaseSource, implements extraction from web as 
    source.
    
    Methods:
    -------
    get(chunk_size)
    Responsible to fetch data from web as chunk.
        Params:
        1. chunk_size (INT): Default 1024. chunk_size to read and write. 
    """
    
    def __init__(self, **kwargs) -> None:
        super().__init__(_name = __name__)
        self.source = kwargs.get("source_url", None)
        self.temp_destination = kwargs.get("temp_location", None)
        
        if self.temp_destination is None:
            filename = self.source.rsplit('/')[-1]
            self.temp_destination = f"tmp/{filename}"
        
        self.temp_destination= Path(self.temp_destination)
        self.__validator()
    
    def __validator(self):
        assert self.source is not None, "No source given to start extraction from"
    
    @property
    def filepath(self):
        return self.temp_destination
    
    def get(self, chunk_size:int=1024) -> None:
        try:
            self.logger.info(f"Beginning to download, file at {self.source}" \
                f" :: {self.temp_destination}")
            
            response = requests.get(self.source, stream=True)
            self.temp_destination.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.temp_destination, "wb") as fl:
                for chunk in tqdm(response.raw.stream(chunk_size, decode_content=True)):
                    fl.write(chunk)
        
        except Exception as e:
            self.logger.error(f"Failed to download file: {self.source} => {e}")