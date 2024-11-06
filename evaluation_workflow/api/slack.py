import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import requests
import traceback

import traceback

def invoke_message_to_channel(
    message,
    url="https://hooks.slack.com/services/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"
):
    if isinstance(message, Exception):
        exception_message = str(message)
        traceback_details = traceback.format_exception(type(message), message, message.__traceback__)
    else:
        exception_message = message
        traceback_details = ["No traceback details available."]
    
    # Prepare the detailed message for Slack
    detailed_message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:* {exception_message}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "```\n" + "".join(traceback_details) + "\n```"
                }
            }
        ]
    }
    
    try:
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, json=detailed_message, headers=headers, timeout=30)
        response.raise_for_status()
        logger.info(f"Message sent to Slack with status - {response.status_code}")
    except Exception as invoke_e:
        logger.error(f"Some error occurred while sending message to Slack - {str(invoke_e)}")
