import pytest
import sys
from unittest.mock import patch


class TestMainArgs:
    """Tests for main.py argument parsing."""

    def test_parse_args_required(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
            "--config_s3_path", "s3://bucket/config.json"
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = _parse_args()
            
            assert args.tables == "all"
            assert args.config_s3_path == "s3://bucket/config.json"

    def test_parse_args_defaults(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
            "--config_s3_path", "s3://bucket/config.json"
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = _parse_args()
            
            assert args.mode == "incremental"
            assert args.raw_database == "db_raw"
            assert args.trusted_database == "db_trusted"
            assert args.log_level == "INFO"

    def test_parse_args_full_mode(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
            "--mode", "full",
            "--config_s3_path", "s3://bucket/config.json"
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = _parse_args()
            
            assert args.mode == "full"
            assert args.processing_date is None

    def test_parse_args_incremental_requires_date(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
            "--mode", "incremental",
            "--processing_date", "2024-01-15",
            "--config_s3_path", "s3://bucket/config.json"
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            args = _parse_args()
            
            assert args.mode == "incremental"
            assert args.processing_date == "2024-01-15"

    def test_parse_args_missing_required(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            with pytest.raises(SystemExit):
                _parse_args()

    def test_parse_args_invalid_mode(self):
        from main import _parse_args
        
        test_args = [
            "--tables", "all",
            "--mode", "invalid_mode",
            "--config_s3_path", "s3://bucket/config.json"
        ]
        
        with patch.object(sys, 'argv', ['main.py'] + test_args):
            with pytest.raises(SystemExit):
                _parse_args()