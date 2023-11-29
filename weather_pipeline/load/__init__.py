# from weather_pipeline.utils import _get_logger
from weather_pipeline.load.loader import Loader

# class Loader:
#     """
#     Loader Driver Factory class that provides the drivers at runtime
#     based on which type of driver is requested.
    
#     Method:
#     ------
#     init():
#         This returns the initialized driver based on user's request.
#     """
#     def __init__(self, destination_type, **kwargs):
#         self.logger = _get_logger(name=__name__)
#         if destination_type == "postgres":
#             self.driver = dbLoader(**kwargs)
#         else:
#             raise NotImplementedError
    
#     def init(self):
#         return self.driver