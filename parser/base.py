
from abc import ABC, abstractmethod
import pandas as pd

STANDARD_COLS = ["invoice_date","UPC","Brand","Description","Pack","Size","Cost","+Cost","Case Qty"]

class InvoiceParser(ABC):
    name: str = "base"
    tokens: list[str] = []

    @abstractmethod
    def parse(self, uploaded_file) -> pd.DataFrame:
        raise NotImplementedError
