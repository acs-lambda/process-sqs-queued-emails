# parser.py
from email import policy
from email.parser import BytesParser
import re
from typing import Tuple, Optional
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_email(email_content: bytes) -> Tuple[Optional[object], Optional[str]]:
    """
    Parses raw email bytes and returns the email message and plain text part.
    Handles HTML-only emails by converting to plain text.
    """
    try:
        msg = BytesParser(policy=policy.default).parsebytes(email_content)
        plain_text = None
        html_text = None

        if msg.is_multipart():
            for part in msg.iter_parts():
                content_type = part.get_content_type()
                try:
                    charset = part.get_content_charset() or 'utf-8'
                    if content_type == 'text/plain':
                        plain_text = part.get_payload(decode=True).decode(charset, errors='replace')
                    elif content_type == 'text/html':
                        html_text = part.get_payload(decode=True).decode(charset, errors='replace')
                except Exception as e:
                    logger.error(f"Error decoding part {content_type}: {str(e)}")
                    continue
        else:
            try:
                charset = msg.get_content_charset() or 'utf-8'
                if msg.get_content_type() == 'text/plain':
                    plain_text = msg.get_payload(decode=True).decode(charset, errors='replace')
                elif msg.get_content_type() == 'text/html':
                    html_text = msg.get_payload(decode=True).decode(charset, errors='replace')
            except Exception as e:
                logger.error(f"Error decoding message: {str(e)}")

        # If no plain text but HTML exists, convert HTML to plain text
        if not plain_text and html_text:
            # Simple HTML to text conversion
            plain_text = re.sub(r'<[^>]+>', ' ', html_text)
            plain_text = re.sub(r'\s+', ' ', plain_text).strip()

        return msg, plain_text
    except Exception as e:
        logger.error(f"Error parsing email: {str(e)}")
        return None, None

def extract_email_headers(msg) -> Tuple[str, str, str]:
    """
    Returns Message-ID, In-Reply-To, References headers.
    Normalizes Message-ID format to match RFC standard.
    """
    try:
        msg_id = msg.get('Message-ID', '').strip()
        in_reply_to = msg.get('In-Reply-To', '').strip()
        references = msg.get('References', '').strip()

        # Normalize Message-ID format
        if msg_id and not (msg_id.startswith('<') and msg_id.endswith('>')):
            msg_id = f'<{msg_id}>'

        # Combine References and In-Reply-To for better threading
        if in_reply_to and in_reply_to not in references:
            references = f"{references} {in_reply_to}".strip()

        return msg_id, in_reply_to, references
    except Exception as e:
        logger.error(f"Error extracting headers: {str(e)}")
        return '', '', ''

def extract_email_from_text(content: str) -> Optional[str]:
    """
    Extracts first email address from text.
    Validates email format more strictly.
    """
    if not content:
        return None
        
    try:
        # More strict email pattern
        pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[A-Za-z]{2,}'
        matches = re.findall(pattern, content)
        if matches:
            # Validate domain has at least one dot
            email = matches[0]
            if '.' in email.split('@')[1]:
                return email.lower()
        return None
    except Exception as e:
        logger.error(f"Error extracting email: {str(e)}")
        return None
