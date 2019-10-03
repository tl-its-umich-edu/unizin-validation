QUERIES = { 
'number_of_courses_by_term' : {
    'index' : 'term',
    'sis_file' : 'number_of_courses_by_term.csv',
    'dsn' : 'udw',
    'query_name': 'UDW Daily Status Report',
    'query' : """
              SELECT * FROM (
                SELECT DISTINCT(ed.name) AS Term, COUNT(ed.name) as TermCount FROM course_dim cd JOIN enrollment_term_dim ed on enrollment_term_id = ed.id
                WHERE (ed.name !~ '.*M[[:digit:]]' and ed.name != 'Test Term') GROUP BY ed.name
                )
              ORDER BY 
                REGEXP_SUBSTR(term, '^\\\\D+') DESC,
                NULLIF(REGEXP_SUBSTR(term, '.+', REGEXP_INSTR(term, '\\\\d+')+4), '') ASC NULLS FIRST,
                REGEXP_SUBSTR(term, '\\\\d+') DESC
  """},
'unizin_metadata' : {
    'index' : '',
    'sis_file' : 'metadata.csv',
    'dsn' : 'udw',
    'query_name': 'Unizin Metadata',
    'query' : """
              SELECT * FROM unizin_metadata
  """},
}