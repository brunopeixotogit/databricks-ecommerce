"""Schema version constant.

Lives in its own module so pure-Python callers (the simulator, the
config loader, tests) can import it without dragging in PySpark via
``schemas.py``. Bump when the producer/consumer contract changes.
"""
SCHEMA_VERSION = "1.0.0"
