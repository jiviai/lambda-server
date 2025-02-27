import logging
import os

class CustomFormatter(logging.Formatter):
    """Custom Formatter to match the desired log format"""

    def format(self, record):
        # Construct the module and function name format
        module_path = record.pathname
        func_name = record.funcName
        # Convert module path to dot notation and remove the leading dot
        module_name = module_path.replace(os.path.sep, '.').replace('.py', '').lstrip('.')
        record.custom_module = f"{module_name}.{func_name}"
        return super().format(record)

def setup_logger(name):
    """Function to setup a logger with a custom formatter"""
    logger = logging.getLogger(name)
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    logger.setLevel(logging.DEBUG)

    # Create handlers
    console_handler = logging.StreamHandler()
    file_handler = logging.FileHandler('app.log')

    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.DEBUG)

    # Create custom formatter and add it to handlers
    formatter = CustomFormatter(
        '%(levelname)s:%(custom_module)s:%(message)s'
    )
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    # Prevent the logger from propagating to the root logger
    logger.propagate = False

    return logger