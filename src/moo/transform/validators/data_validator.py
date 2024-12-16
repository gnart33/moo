# src/moo/transform/validators/data_validator.py
class SwapDataValidator:
    def validate_schema(self, data: pd.DataFrame) -> bool:
        return self._check_required_columns(data) and self._validate_data_types(data)

    def validate_business_rules(self, data: pd.DataFrame) -> bool:
        return self._check_amount_ranges(data) and self._verify_timestamps(data)
