from datetime import datetime
import pytz

QUERIES = {
    'number_of_courses_by_term': {
        'output_file_name': 'number_of_courses_by_term.csv',
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
    'unizin_metadata': {
        'output_file_name': 'unizin_metadata.csv',
        'data_source': 'UDW',
        'query_name': 'Unizin Metadata',
        'type': 'standard',
        'query': """
            SELECT * FROM unizin_metadata;
        """,
        'checks': {
            'less_than_two_days': {
                'level': 'yellow',
                'condition': (lambda x: (datetime.now(tz=pytz.UTC) - datetime.fromisoformat(x)).days < 2),
                'rows_to_ignore': [0]
            }
        }
    },
    'udw_table_counts': {
        'output_file_name': 'udw_table_counts.csv',
        'data_source': 'UDW',
        'query_name': 'UDW Table Record Counts',
        'type': 'table_counts',
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
                'level': 'red',
                'condition': (lambda x: x != 0),
                'rows_to_ignore': []
            }
        }
    }
}