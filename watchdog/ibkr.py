import os
from ib_insync import IB, ContFuture

def make_es_contfut(symbol: str, exchange: str):
    return ContFuture(symbol=symbol, exchange=exchange)

def connect_ib(client_id: int) -> IB:
    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "4002"))
    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=20)
    return ib
