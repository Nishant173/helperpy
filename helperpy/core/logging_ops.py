import logging


def get_logger_object(
        logger_name: str,
        filepath: str,
    ) -> logging.Logger:
    """
    Gets `logging.Logger` object based on the given params. Each set of params must be unique together.
    Can be used to write neat-looking log messages for all possible log levels.

    >>> logger_ceo = get_logger_object(logger_name="ceo", filepath="ceo.log")
    >>> logger_emp = get_logger_object(logger_name="employee", filepath="employee.log")
    >>> logger_ceo.info(msg="This is a log in the CEO module")
    >>> logger_emp.info(msg="This is a log in the Employee module")
    
    ### References
        - https://docs.python.org/3/library/logging.html
        - https://docs.python.org/3/library/logging.html#logrecord-attributes
        - https://docs.python.org/3/howto/logging-cookbook.html
        - https://www.youtube.com/watch?v=jxmzY9soFXg
    """
    logger_obj = logging.getLogger(name=logger_name)
    logger_obj.setLevel(level=logging.DEBUG)
    file_handler = logging.FileHandler(
        filename=filepath,
        mode='a',
    )
    file_handler.setFormatter(
        fmt=logging.Formatter(
            fmt="\nDatetime: %(asctime)s | Level: %(levelname)s | Logger: %(name)s | File: %(filename)s | Function/method: %(funcName)s | Message: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger_obj.addHandler(hdlr=file_handler)
    return logger_obj