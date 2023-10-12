from datasets.utils import validate_delim
from datasets.exceptions import DelimValidationError #, LPFValidationError
from django.test import TestCase
import pandas as pd

class DelimValidationTest(TestCase):
    def test_missing_required_field(self):
        # Set up test data
        headers = ["id", "title", "title_source", "start", "attestation_year", "parent_id"]
        data = [
            # [1, None, "a source", "1825", None, "http://example.com/1"],  # missing title
            [2, "a title", "a source", None, None, "http://example.com/2"]  # missing both start and attestation_year
        ]
        # Create a DataFrame
        df = pd.DataFrame(data, columns=headers)

        # Call the validation function
        try:
            # If the function raises an error, it will be caught
            validate_delim(df)
        except DelimValidationError as e:
            # Handle the exception and perform assertions
            errors = e.args[0] if e.args else []
            print(errors)
            # self.assertIn('Required field missing: title', [err['error'] for err in errors])
            self.assertIn('Field start contains a value that does not match the required pattern',
                          [err['error'] for err in errors])
