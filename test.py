# Standard modules
import unittest
from datetime import datetime, timedelta, timezone

# Third-party modules
import pandas as pd

# Local modules
import validate
from dbqueries import QUERIES


class TestFlagRaising(unittest.TestCase):
    def test_udp_context_store_view_counts_check(self):
        # Set up
        udp_view_counts_df = pd.DataFrame({
            'table_name': [
                'entity.learner_activity',
                'entity.course_offering',
                'entity.course_grade',
                'entity.academic_term',
                'entity.annotation',
                'entity.learner_activity_result',
                'entity.person',
            ],
            'record_count': [1000, 1000, 0, 1000, 1000, 1000, 1000]
        })
        query_dict = QUERIES['udp_context_store_view_counts']

        # Test
        checks_result = validate.run_checks_on_output(query_dict['checks'], udp_view_counts_df)
        self.assertListEqual(['table_name', 'record_count', 'not_zero'], checks_result.checked_output_df.columns.to_list())
        self.assertListEqual(['entity.course_grade', 0, False], checks_result.checked_output_df.loc[2].to_list())
        self.assertTrue(checks_result.flags == ['YELLOW'])

        result_text = validate.generate_result_text(query_dict['query_name'], checks_result.checked_output_df)
        self.assertIn('<-- "not_zero" condition failed', result_text)

unittest.main()