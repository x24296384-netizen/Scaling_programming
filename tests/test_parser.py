""" Tests for the web access-log parser."""
import unittest
from producer.log_parser import parse_log_line

class TestLogParser(unittest.TestCase):
    """Automated tests for the log parser function."""

    def test_valid_log_line(self):
        """Test parsing a valid log line."""
        line = (
            '54.63.149.41 - - [10/Oct/2000:13:55:36 -0700] '
            '"GET /filter/test HTTP/1.1" 200 1234 "-" ' 
            '"Mozilla/5.0" "-" '
        )

        #Pass the raw log line to the parser
        result = parse_log_line(line)

        #Check that the result is a dictionary with expected keys
        #instead of none
        self.assertIsInstance(result, dict)

        #Confirm that each important field was extracted correctly
        self.assertEqual(result['client_ip'], '54.63.149.41')
        self.assertEqual(result['timestamp'], '2000-10-10T13:55:36-07:00',)
        self.assertEqual(result['method'], 'GET')
        self.assertEqual(result['resource'], '/filter/test')
        self.assertEqual(result['protocol'], 'HTTP/1.1')
        self.assertEqual(result['status_code'], 200)
        self.assertEqual(result['response_bytes'], 1234)
        self.assertEqual(result['referrer'], '-')
        self.assertEqual(result['user_agent'], 'Mozilla/5.0')
        self.assertEqual(result['extra'], '-')

    def test_invalid_log_line(self):
        """Confirm that an invalid line does not crash the application.
        The parser should return None for invalid lines."""
        invalid_line = 'Invalid log line that does not match the expected format'

        #Pass the raw log line to the parser
        result = parse_log_line(invalid_line)

        #Check that the result is None for an invalid log line
        self.assertIsNone(result)

#Run the test when this file is executed directly
if __name__ == '__main__':
    unittest.main()