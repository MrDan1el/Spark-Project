import os


DEFAULT_INPUT_PATH = "data/raw"
DEFAULT_OUTPUT_PATH = "data/output"

INPUT_PATH = os.getenv("INPUT_PATH", DEFAULT_INPUT_PATH)
OUTPUT_PATH = os.getenv("OUTPUT_PATH", DEFAULT_OUTPUT_PATH)

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true").lower() == "true"