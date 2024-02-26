import logging


class Logger:
    @staticmethod
    def sample_logger(filename="Logs/logs.log"):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(datefmt='%m-%d-%Y %I:%M:%S %p', fmt="%(asctime)s - %(module)s - %(levelname)s : "
                                                                          "%(message)s ")
        fh = logging.FileHandler(filename=filename,
                                 mode='a', encoding='utf-8')
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

