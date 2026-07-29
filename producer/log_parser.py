import re
from datetime import datetime
from typing import Dict, Any

LOG_PATTERN = re.compile(
    r'^(?P<client_ip>\S+) '
    r'\S+ \S+ '
    r'\[(?P<timestamp>[^\]]+)\] '
    r'"(?P<request>[^"]*)" '
    r'(?P<status_code>\d{3}) '
    r'(?P<response_bytes>\S+) '
    r'"(?P<referrer>[^"]*)" '
    r'"(?P<user_agent>[^"]*)" '
    r'"(?P<extra>[^"]*)"$'
)

def parse_log_line(line: str) -> Dict[str, Any] | None:
    """
    Convert one Nginx access-log line into the frozen base schema.

    Return None when the line, request or timestamp is invalid.
    """

    match = LOG_PATTERN.match(line.strip())

    # Reject lines that do not match the expected Nginx format.
    if match is None:
        return None

    data = match.groupdict()

    # A valid HTTP request must contain method, endpoint and protocol.
    request_parts = data["request"].split(" ", maxsplit=2)

    if len(request_parts) != 3:
        return None

    method, endpoint, protocol = request_parts

    # Nginx may use "-" when the response size is unknown.
    response_bytes = (
        int(data["response_bytes"])
        if data["response_bytes"].isdigit()
        else 0
    )

    # Reject malformed timestamps without stopping the complete replay.
    try:
        timestamp = datetime.strptime(
            data["timestamp"],
            "%d/%b/%Y:%H:%M:%S %z",
        )
    except ValueError:
        return None

    # Return only the fields agreed in the frozen base schema.
    return {
        "client_ip": data["client_ip"],
        "timestamp": timestamp.isoformat(),
        "method": method,
        "endpoint": endpoint,
        "protocol": protocol,
        "status_code": int(data["status_code"]),
        "response_bytes": response_bytes,
        "referrer": data["referrer"],
        "user_agent": data["user_agent"],
    }