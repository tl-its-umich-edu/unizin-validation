# Job configuration for validate.py

# Each key in the dictionary assigned to JOBS should be an abbreviated name for the job. The value should be a
# dictionary with two key-value pairs: the "full_name" key should have as its value a human-readable string identifying
# the job; the "queries" key should have as its value a list of query names from dbqueries.py.

from typing import Literal, TypedDict

from dbqueries import QueryName


# Types

JobName = Literal['UDW', 'Unizin']


class JobData(TypedDict):
    full_name: str
    queries: list[QueryName]


class JobDict(TypedDict):
    UDW: JobData
    Unizin: JobData


# Jobs configuration

JOBS: JobDict = {
    "UDW": {
        "full_name": "UDW Daily Status Report",
        "queries": [
            "unizin_metadata",
            "udw_table_counts",
            "number_of_courses_by_term"
        ]
    },
    "Unizin": {
        "full_name": "Unizin Daily Status Report",
        "queries": [
            "unizin_metadata",
            "udw_table_counts",
            "udp_context_store_view_counts",
            "number_of_courses_by_term"
        ]
    }
}
