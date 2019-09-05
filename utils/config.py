import configparser
from typing import Dict, Any


def create_configuration_object(file_path: str) -> Dict[str, Any]:
    
    conf = configparser.ConfigParser()
    conf.read(file_path)
    
    return conf