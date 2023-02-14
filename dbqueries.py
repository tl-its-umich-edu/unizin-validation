# Standard modules
from datetime import datetime, timezone
from typing import Callable, Literal, TypedDict, Union

# Local modules
from data_sources import DataSourceName


# Types

class CheckData(TypedDict):
    color: Literal['YELLOW', 'RED']
    condition: Union[Callable[[str], bool], Callable[[int], bool]]
    rows_to_ignore: list[str]


class QueryData(TypedDict):
    output_file_name: str
    data_source: DataSourceName
    query_name: str
    checks: dict[str, CheckData]


class StandardQueryData(QueryData):
    type: Literal['standard']
    query: str


class TableCountsQueryData(QueryData):
    tables: list[str]


class RecordCountsQueryData(TableCountsQueryData):
    type: Literal['table_record_counts']


class QueryDict(TypedDict):
    udw_number_of_courses_by_term: StandardQueryData
    udw_unizin_metadata: StandardQueryData
    udw_table_counts: RecordCountsQueryData
    udw_duplicate_course_ids: StandardQueryData
    udw_duplicate_assignment_ids: StandardQueryData
    udp_context_store_view_counts: RecordCountsQueryData


QueryName = Literal[
    'udw_number_of_courses_by_term',
    'udw_unizin_metadata',
    'udw_table_counts',
    'udw_duplicate_course_ids',
    'udw_duplicate_assignment_ids',
    'udp_context_store_view_counts'
]


# Check functions

NOT_ZERO: Callable[[int], bool] = (lambda x: x != 0)
LESS_THAN_TWO: Callable[[int], bool] = (lambda x: x < 2)
LESS_THAN_TWO_DAYS: Callable[[str], bool] = (lambda x: (datetime.now(tz=timezone.utc) - datetime.fromisoformat(x)).days < 2)

# Queries configuration

QUERIES: QueryDict = {
    'udw_number_of_courses_by_term': {
        'output_file_name': 'udw_number_of_courses_by_term.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Course Counts by Term',
        "type": "standard",
        'query': """
            SELECT * FROM (
              SELECT DISTINCT(ed.name) AS term, COUNT(ed.name) as course_count 
              FROM course_dim cd 
              JOIN enrollment_term_dim ed 
                  ON enrollment_term_id = ed.id
              WHERE (ed.name !~ '.*M[[:digit:]]' AND ed.name != 'Test Term') 
              GROUP BY ed.name
            )
            ORDER BY REGEXP_SUBSTR(term, '^\\\\D+') DESC,
            NULLIF(REGEXP_SUBSTR(term, '.+', REGEXP_INSTR(term, '\\\\d+')+4), '') ASC NULLS FIRST, 
                REGEXP_SUBSTR(term, '\\\\d+') DESC;
        """,
        'checks': {}
    },
    'udw_unizin_metadata': {
        'output_file_name': 'udw_unizin_metadata.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Unizin Metadata',
        'type': 'standard',
        'query': """
            SELECT * FROM unizin_metadata;
        """,
        'checks': {
            'less_than_two_days': {
                'color': 'YELLOW',
                'condition': LESS_THAN_TWO_DAYS,
                'rows_to_ignore': ['schemaversion']
            }
        }
    },
    'udw_table_counts': {
        'output_file_name': 'udw_table_counts.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Table Record Counts',
        'type': 'table_record_counts',
        'tables': [
            'ASSIGNMENT_DIM',
            'COURSE_DIM',
            'COURSE_SCORE_FACT',
            'ENROLLMENT_TERM_DIM',
            'PSEUDONYM_DIM',
            'SUBMISSION_COMMENT_DIM',
            'SUBMISSION_DIM',
            'SUBMISSION_FACT',
            'USER_DIM',
        ],
        'checks': {
            'not_zero': {
                'color': 'RED',
                'condition': NOT_ZERO,
                'rows_to_ignore': []
            }
        }
    },
    'udw_duplicate_course_ids': {
        'output_file_name': 'udw_duplicate_course_ids.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Duplicate Course IDs',
        'type': 'standard',
        'query': '''
            select c.canvas_id, count(c.canvas_id)
            from course_dim c
            left join enrollment_term_dim t
                on t.id=c.enrollment_term_id
            where c.workflow_state != 'deleted'
                and t.date_start <= current_timestamp
                and t.date_start + interval '120 days' >= current_timestamp
            group by c.canvas_id
            having count(*) > 1;
        ''',
        'checks': {
            'less_than_two': {
                'color': 'RED',
                'condition': LESS_THAN_TWO,
                'rows_to_ignore': []
            }
        }
    },
    'udw_duplicate_assignment_ids': {
        'output_file_name': 'udw_duplicate_assignment_ids.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Duplicate Assignment IDs',
        'type': 'standard',
        'query': '''
            select a.canvas_id, count(a.canvas_id)
            from assignment_dim a
            left join course_dim c
                on c.id=a.course_id
            left join enrollment_term_dim t
                on t.id=c.enrollment_term_id
            where a.workflow_state != 'deleted'
                and t.date_start <= current_timestamp
                and t.date_start + interval '120 days' >= current_timestamp
            group by a.canvas_id
            having count(*) > 1;
        ''',
        'checks': {
            'less_than_two': {
                'color': 'RED',
                'condition': LESS_THAN_TWO,
                'rows_to_ignore': []
            }
        }
    },
    'udp_context_store_view_counts': {
        'output_file_name': 'udp_context_store_view_counts.csv',
        'data_source': 'UDP_Context_Store',
        'query_name': 'UDP Context Store View Record Counts',
        'type': 'table_record_counts',
        'tables': [
            'entity.learner_activity',
            'entity.course_offering',
            'entity.course_grade',
            'entity.academic_term',
            'entity.annotation',
            'entity.learner_activity_result',
            'entity.person',
        ],
        'checks': {
            'not_zero': {
                'color': 'YELLOW',
                'condition': NOT_ZERO,
                'rows_to_ignore': []
            }
        }
    }
}