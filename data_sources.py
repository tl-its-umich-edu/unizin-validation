from typing import Literal, TypedDict


# Types

class DatabaseParams(TypedDict):
    host: str
    port: str
    name: str
    user: str
    password: str


class DataSource(TypedDict):
    type: Literal['PostgreSQL']
    params: DatabaseParams


DataSourceName = Literal['UDP_Context_Store']
