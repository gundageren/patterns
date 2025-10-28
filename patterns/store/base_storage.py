from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class BaseStorage(ABC):
    @abstractmethod
    def save_tables(self, tables: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def load_tables(self) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_queries(self, queries: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def load_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_read_table_queries(self, data: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def load_read_table_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_select_star_queries(self, data: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def load_select_star_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def save_partition_candidates(self, data: List[Dict[str, Any]]) -> None:
        pass

    @abstractmethod
    def load_partition_candidates(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None) -> List[Dict[str, Any]]:
        pass
