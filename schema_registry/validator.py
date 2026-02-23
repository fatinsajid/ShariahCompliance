from jsonschema import validate, ValidationError
from schema_registry.registry import load_schema


def validate_event(event_type: str, payload: dict, version: str = "v1"):
    """
    Validate payload against registered schema
    """
    schema = load_schema(event_type, version)

    try:
        validate(instance=payload, schema=schema)
    except ValidationError as e:
        raise ValueError(f"Schema validation failed: {e.message}")