# parser.py
from email import policy
from email.parser import BytesParser
import re
from typing import Tuple, Optional
import logging
import email.utils

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def strip_quoted_reply(text: str) -> str:
    """
    Strips out quoted reply text from email body.
    Handles common email client quote formats.
    """
    if not text:
        return text

    # Common patterns for quoted replies
    patterns = [
        # Gmail style
        r'On.*wrote:.*$',
        # Outlook style
        r'From:.*Sent:.*To:.*Subject:.*$',
        # Generic quote markers
        r'^>.*$',
        # Common email client signatures
        r'--\s*$',
        r'_{2,}$',
        r'={2,}$'
    ]

    # Split by lines and filter out quoted content
    lines = text.split('\n')
    filtered_lines = []
    in_quote = False

    for line in lines:
        # Check if this line starts a quote block
        if any(re.match(pattern, line.strip()) for pattern in patterns):
            in_quote = True
            continue
        
        # If we're in a quote block, check if this line is part of it
        if in_quote:
            if line.strip().startswith('>'):
                continue
            if not line.strip():  # Empty line might end the quote
                in_quote = False
            continue

        filtered_lines.append(line)

    # Join the lines back together
    cleaned_text = '\n'.join(filtered_lines).strip()
    logger.info(f"Original text length: {len(text)}, Cleaned text length: {len(cleaned_text)}")
    return cleaned_text

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

        # Clean the text by removing quoted replies
        if plain_text:
            plain_text = strip_quoted_reply(plain_text)
            logger.info(f"Cleaned email body: {plain_text[:100]}...")  # Log first 100 chars

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

        logger.info(f"Extracted headers - Message-ID: {msg_id}, In-Reply-To: {in_reply_to}, References: {references}")
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

def extract_user_info_from_headers(msg) -> dict:
    """
    Extracts user information from standard email headers.
    Only extracts information that is explicitly present in the headers.
    """
    user_info = {}
    
    # Extract and parse From header which often contains name and email
    from_header = msg.get('From', '')
    if from_header:
        user_info['from_header'] = from_header
        # Try to parse name and email from From header
        # Common format: "John Doe <john.doe@example.com>"
        try:
            name, email_addr = email.utils.parseaddr(from_header)
            if name:
                user_info['sender_name'] = name
            if email_addr:
                user_info['sender_email'] = email_addr.lower()
        except Exception as e:
            logger.error(f"Error parsing From header: {str(e)}")

    # Extract Reply-To header if present
    reply_to = msg.get('Reply-To', '')
    if reply_to:
        user_info['reply_to'] = reply_to
        # Try to parse name and email from Reply-To header
        try:
            name, email_addr = email.utils.parseaddr(reply_to)
            if name:
                user_info['reply_to_name'] = name
            if email_addr:
                user_info['reply_to_email'] = email_addr.lower()
        except Exception as e:
            logger.error(f"Error parsing Reply-To header: {str(e)}")

    # Extract Organization header if present
    organization = msg.get('Organization', '')
    if organization:
        user_info['organization'] = organization

    # Extract X-Mailer header which might indicate email client
    mailer = msg.get('X-Mailer', '')
    if mailer:
        user_info['mailer'] = mailer

    # Extract X-Originating-IP header if present
    originating_ip = msg.get('X-Originating-IP', '')
    if originating_ip:
        user_info['originating_ip'] = originating_ip

    # Extract X-Forwarded-For header if present
    forwarded_for = msg.get('X-Forwarded-For', '')
    if forwarded_for:
        user_info['forwarded_for'] = forwarded_for

    return user_info
