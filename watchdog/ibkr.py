import os
from ib_insync import IB, ContFuture, Contract


def make_es_contfut(symbol: str, exchange: str):
    return ContFuture(symbol=symbol, exchange=exchange)


def front_month(ib: IB, symbol: str, exchange: str) -> Contract:
    """The real contract behind the continuous future (e.g. MESZ6).

    IB refuses an end date on continuous-future history requests (error 10339),
    so paged backfills must use the actual contract.
    """
    cont = make_es_contfut(symbol, exchange)
    ib.qualifyContracts(cont)
    fut = Contract(conId=cont.conId, exchange=exchange)
    ib.qualifyContracts(fut)
    return fut


def connect_ib(client_id: int | None = None) -> IB:
    """Connects with this container's own IB_CLIENT_ID (each container needs a unique one)."""
    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "4002"))
    cid = client_id if client_id is not None else int(os.getenv("IB_CLIENT_ID", "102"))
    ib = IB()
    ib.connect(host, port, clientId=cid, timeout=20)
    return ib
