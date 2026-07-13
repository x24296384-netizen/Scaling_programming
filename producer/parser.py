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
    Convert one access log line into a dictionary. 
    Returns None when the line does not match the expected format.
    """
    match = LOG_PATTERN.match(line.strip())
    if match is None:
        return None

    data = match.groupdict()

    request_parts = data['request'].split(" ", maxsplit=2)

    if len(request_parts) == 3:
        method, resource, protocol = request_parts
    else:
        method = None
        resource = data['request']
        protocol = None

    response_bytes = (
        int(data['response_bytes']) 
        if data['response_bytes'].isdigit() 
        else 0
    )

    timestamp = datetime.strptime(
        data['timestamp'], 
        "%d/%b/%Y:%H:%M:%S %z"
    )

    return {
        "client_ip": data['client_ip'],
        "timestamp": timestamp.isoformat(),
        "method": method,
        "resource": resource,
        "protocol": protocol,
        "status_code": int(data['status_code']),
        "response_bytes": response_bytes,
        "referrer": data['referrer'],
        "user_agent": data['user_agent'],
        "extra": data['extra'],
    }