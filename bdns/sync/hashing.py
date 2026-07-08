# -*- coding: utf-8 -*-
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
# You should have received a copy of the GNU General Public License along
# with this program. If not, see <https://www.gnu.org/licenses/>.

"""Canonical JSON, row hashing, and natural key derivation for SCD2 versioning."""

import hashlib
import json
from typing import Any, Dict, Iterable, Optional, Sequence


def _order_independent(value: Any) -> Any:
    """Recursively sort list elements so hashing doesn't care about array
    order. Confirmed live on `regiones`: the API returns the same tree
    `children` in a different element order across calls, with no field
    actually changed -- without this, every re-sync produced a spurious
    SCD2 version for any key with a reordered nested array.

    Dict key order is already handled by `json.dumps(sort_keys=True)`
    (recursively); only list element order needs normalizing here, by
    sorting on each element's own canonical JSON string.
    """
    if isinstance(value, dict):
        return {k: _order_independent(v) for k, v in value.items()}
    if isinstance(value, list):
        items = [_order_independent(v) for v in value]
        return sorted(items, key=lambda v: json.dumps(v, sort_keys=True, ensure_ascii=False, default=str))
    return value


def canonical_json(
    payload: Dict[str, Any], exclude_fields: Optional[Iterable[str]] = None
) -> str:
    if exclude_fields:
        excluded = set(exclude_fields)
        payload = {k: v for k, v in payload.items() if k not in excluded}
    return json.dumps(
        _order_independent(payload), sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )


def row_hash(payload: Dict[str, Any], exclude_fields: Optional[Iterable[str]] = None) -> str:
    digest = canonical_json(payload, exclude_fields).encode("utf-8")
    return hashlib.sha256(digest).hexdigest()


def natural_key(payload: Dict[str, Any], key_fields: Sequence[str]) -> str:
    """Build a stable string key from one or more fields (composite keys supported)."""
    values = [payload[field] for field in key_fields]
    return json.dumps(values, separators=(",", ":"), ensure_ascii=False, default=str)
