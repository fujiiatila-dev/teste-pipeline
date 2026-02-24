from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime

class BaseConnector(ABC):
    @abstractmethod
    def extract(self, date_start: datetime, date_stop: datetime) -> dict:
        """
        Extrai dados do provedor e retorna um dicionário de DataFrames.
        Ex: {"campaigns": df_camp, "insights": df_ins}
        """
        pass

    @abstractmethod
    def get_tables_ddl(self) -> list:
        """
        Retorna uma lista de strings SQL para criação das tabelas no ClickHouse.
        """
        pass
