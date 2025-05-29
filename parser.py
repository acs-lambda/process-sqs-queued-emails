# parser.py
from email import policy
from email.parser import BytesParser
import re


def parse_email(email_content: bytes):
    """Parses raw email bytes and returns the email message and plain text part."""
    msg = BytesParser(policy=policy.default).parsebytes(email_content)
    plain_text = None
    if msg.is_multipart():
        for part in msg.iter_parts():
            if part.get_content_type() == 'text/plain':
                plain_text = part.get_payload(decode=True).decode(part.get_content_charset())
    else:
        plain_text = msg.get_payload(decode=True).decode(msg.get_content_charset())
    return msg, plain_text


def extract_email_headers(msg):
    """Returns Message-ID, In-Reply-To, References headers."""
    return (
        msg.get('Message-ID', '').strip(),
        msg.get('In-Reply-To', '').strip(),
        msg.get('References', '').strip()
    )


def extract_email_from_text(content: str):
    """Extracts first email address from text."""
    pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
    match = re.search(pattern, content)
    return match.group(0) if match else None
