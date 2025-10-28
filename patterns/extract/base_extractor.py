from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

class BaseExtractor(ABC):
    def __init__(self, connection: Optional[Dict[str, Any]] = None):
        self.connection = connection or {}

    @property
    @abstractmethod
    def platform(self) -> str:
        pass

    @abstractmethod
    def extract_tables(self) -> List[Dict[str, str]]:
        pass

    @abstractmethod
    def extract_query_history(
        self, start_time: Optional[str] = None, end_time: Optional[str] = None
    ) -> List[str]:
        pass

    @abstractmethod
    def get_type_map(self) -> dict:
        pass

    def normalize_type_category(self, data_type: str) -> str:
        data_type = data_type.strip().upper()
        type_map = self.get_type_map()

        for category, types in type_map.items():
            if data_type in types:
                return category

        return "unknown"