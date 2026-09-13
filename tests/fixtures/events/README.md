# Historical event fixtures (ADR-0009)

One file per `(event_type, schema_version)` **ever written to a production
log**, named `<Type>.v<N>.json`, containing the row exactly as a reader
receives it from the store (`_row_to_event` shape). Files are permanent: a
version that was ever written can be read from a backup in any future year.

`tests/unit/domain/test_upcasting.py` asserts that every fixture upcasts to
the current version and folds through the balance projection, and that
every version `1..CURRENT` of every type has a fixture. Adding a schema
version without adding its fixture fails the build.
