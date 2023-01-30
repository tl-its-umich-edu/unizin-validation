# Standard modules
import unittest
from datetime import datetime, timedelta, timezone

# Third-party modules
import pandas as pd

# Local modules
import validate
from dbqueries import QUERIES


class TestFlagRaising(unittest.TestCase):

    def test_udw_table_counts_check(self):
        # Set up
        udw_table_counts_df = pd.DataFrame({
            'table_name': [
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
            'record_count': [1000, 1000, 1000, 0, 1000, 1000, 1000, 1000, 0]
        })
        query_dict = QUERIES['udw_table_counts']
        # Test
        checks_result = validate.run_checks_on_output(query_dict['checks'], udw_table_counts_df)
        self.assertListEqual(['table_name', 'record_count', 'not_zero'], checks_result.checked_output_df.columns.to_list())

        self.assertListEqual(['ASSIGNMENT_DIM', 1000, True], checks_result.checked_output_df.loc[0].to_list())
        self.assertListEqual(['ENROLLMENT_TERM_DIM', 0, False], checks_result.checked_output_df.loc[3].to_list())
        self.assertListEqual(['USER_DIM', 0, False], checks_result.checked_output_df.loc[8].to_list())

        self.assertTrue(checks_result.flags == ["RED"])
        result_text = validate.generate_result_text(query_dict['query_name'], checks_result.checked_output_df)
        self.assertEqual(result_text.count('<-- "not_zero" condition failed'), 2)

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

    def test_udw_duplicate_id_test(self):
        # Set up
        udw_duplicate_id_counts_df = pd.DataFrame({
            'table_name': [
                "course_dim",
                "assignment_dim"
            ],
            'count': [0, 1]
        })
        query_dict = QUERIES['udw_table_duplicate_ids']
        # Test
        checks_result = validate.run_checks_on_output(query_dict['checks'], udw_duplicate_id_counts_df)
        self.assertListEqual(['table_name', 'count', 'zero'], checks_result.checked_output_df.columns.to_list())
        self.assertListEqual(['assignment_dim', 1, False], checks_result.checked_output_df.loc[1].to_list())
        self.assertTrue(checks_result.flags == ['RED'])
        result_text = validate.generate_result_text(query_dict['query_name'], checks_result.checked_output_df)
        self.assertIn('<-- "zero" condition failed', result_text)


    def test_unizin_metadata_check(self):
        # Set up
        delta_obj = timedelta(days=-3)
        three_days_ago = datetime.now(tz=timezone.utc) + delta_obj
        unizin_metadata_df = pd.DataFrame({
            'key': ['schemaversion', 'canvasdatadate'],
            'value': ['X.X.X', three_days_ago.isoformat()]
        })
        query_dict = QUERIES['udw_unizin_metadata']
        # Test
        checks_result = validate.run_checks_on_output(query_dict['checks'], unizin_metadata_df)
        self.assertListEqual(['key', 'value', 'less_than_two_days'], checks_result.checked_output_df.columns.to_list())
        self.assertListEqual(
            ['canvasdatadate', three_days_ago.isoformat(), False],
            checks_result.checked_output_df.loc[1].to_list()
        )
        self.assertTrue(checks_result.flags == ['YELLOW'])
        result_text = validate.generate_result_text(query_dict['query_name'], checks_result.checked_output_df)
        self.assertIn('<-- "less_than_two_days" condition failed', result_text)


unittest.main()