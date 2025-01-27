# Custom Logger
import logging

def get_custom_logger(v_Notebook_Name, v_Pipeline_RunId="None", v_Actvt_RunId="None", v_Log_Level="INFO"):
    
    if v_Log_Level.upper() == "INFO":
        v_Log_Level = logging.INFO
    elif v_Log_Level.upper() == "WARNING":
        v_Log_Level = logging.WARNING
    elif v_Log_Level.upper() == "DEBUG":
        v_Log_Level = logging.DEBUG
    elif v_Log_Level.upper() == "ERROR":
        v_Log_Level = logging.ERROR
    elif v_Log_Level.upper() == "CRITICAL":
        v_Log_Level = logging.CRITICAL
    else:
        v_Log_Level = logging.INFO

    FORMAT = (
        "%(asctime)s - [pipeline.runid="
        + v_Pipeline_RunId
        + ", pipeline.activity.runid="
        + v_Actvt_RunId
        + "] - %(name)s:%(lineno)d - %(levelname)s - %(message)s"
    )
    formatter = logging.Formatter(fmt=FORMAT)

    logger = logging.getLogger(v_Notebook_Name)
    logger.setLevel(v_Log_Level)

    if not logger.handlers:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    v_Logger_Response = (
        "Created a logger object with logging level="
        + str(v_Log_Level)
        + " [10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL]"
    )
    logger.info(v_Logger_Response)
    return logger
