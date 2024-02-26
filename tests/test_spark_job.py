import os
import sys
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
from helpers.utils import csv_has_header
from src.spark_job import ingest_meta_data
from helpers.customLogger import Logger


class Test001:
    logger = Logger.sample_logger(filename="Logs/test_logs.log")

    def test_csv_has_header(self, spark_session):
        self.logger.info("********** Starting Test001 **********")
        self.logger.info("********** Starting test_csv_has_header **********")
        if csv_has_header("data/sample_data/books_info_3.csv"):
            self.logger.info("************** Test CSV_HAS_HEADER Passed *************")
            assert True
        else:
            self.logger.error("************** Test CSV_HAS_HEADER Failed !! *************")
            assert False

    def test_csv_has_no_header(self, spark_session):
        self.logger.info("********** Starting test_csv_has_no_header **********")
        if not csv_has_header("data/sample_data/movies_info_5.csv"):
            self.logger.info("************** Test CSV_HAS_NO_HEADER Passed *************")
            assert True
        else:
            self.logger.info("************** Test CSV_HAS_NO_HEADER Failed !! *************")
            assert False

    def test_ingest_meta_data(self, spark_session):
        self.logger.info("********** Starting test_ingest_meta_data **********")
        df = spark_session.createDataFrame([("Test", 1)], ["Name", "Value"])
        csv_file_path = "data/mock_test_data/mock_data.csv"
        metadata_catalog_path = "delta_tables/metadata_catalog"
        schema_id = ingest_meta_data(df, csv_file_path, metadata_catalog_path)
        metadata_df = spark_session.read.format("delta").load(metadata_catalog_path)
        if metadata_df.filter(metadata_df.schema_id == schema_id).count() == 1:
            self.logger.info("************** Test INGEST_METADATA Passed *************")
            assert True
        else:
            self.logger.error("************** Test INGEST_METADATA Failed !! *************")
            assert False
