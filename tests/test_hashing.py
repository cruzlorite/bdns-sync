from bdns.sync.hashing import canonical_json, natural_key, row_hash


def test_canonical_json_is_key_order_independent():
    assert canonical_json({"a": 1, "b": 2}) == canonical_json({"b": 2, "a": 1})


def test_canonical_json_excludes_fields():
    assert canonical_json({"a": 1, "b": 2}, exclude_fields=["b"]) == canonical_json({"a": 1})


def test_row_hash_stable_for_equivalent_payloads():
    assert row_hash({"a": 1, "b": 2}) == row_hash({"b": 2, "a": 1})


def test_row_hash_changes_on_value_change():
    assert row_hash({"a": 1}) != row_hash({"a": 2})


def test_natural_key_simple():
    assert natural_key({"id": 42}, ("id",)) == "[42]"


def test_natural_key_composite_is_order_stable():
    payload = {"ambito": "M", "id": 7}
    assert natural_key(payload, ("ambito", "id")) == '["M",7]'
