import yaml
from typing import Dict, Any
from pathlib import Path


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        config_path (str): Path to the YAML configuration file

    Returns:
        Dict[str, Any]: Configuration dictionary with extract, transform, and load sections

    Raises:
        FileNotFoundError: If the config file doesn't exist
        yaml.YAMLError: If the config file is not valid YAML
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        # Validate required sections
        required_sections = ["extract", "transform", "load"]
        missing_sections = [
            section for section in required_sections if section not in config
        ]

        if missing_sections:
            raise ValueError(
                f"Missing required configuration sections: {', '.join(missing_sections)}"
            )

        return config

    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing configuration file: {e}")
