# src/moo/utils/config.py
class PipelineConfig:
    def __init__(self):
        self.extract_config = {"batch_size": 1000, "max_retries": 3}
        self.transform_config = {"validation_rules": {...}}
        self.load_config = {"batch_size": 5000, "timeout": 300}
