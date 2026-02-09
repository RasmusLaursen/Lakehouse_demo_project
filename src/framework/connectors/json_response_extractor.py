"""
Generic JSON Response Extractor for REST API responses.

Provides a two-phase extraction approach:
  Phase 1 - Auto-extract the first layer: scalars become columns,
            nested dicts/arrays are serialized as JSON strings.
  Phase 2 - Configurable explode: walk a dot-separated path to drill
            into nested structures, carrying parent context fields
            down to each leaf record.

Usage:
    # Flat response  (data_path="result")
    extractor = JSONResponseExtractor(data_path="result")
    records = extractor.extract(response_json)

    # Deeply nested  (data_path="result.Doc.TimeSeries.Period.Point")
    extractor = JSONResponseExtractor(
        data_path="result.Doc.TimeSeries.Period.Point",
        field_mapping={"out_Quantity.quantity": "quantity"},
    )
    records = extractor.extract(response_json)
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from src.framework.helper import logging_helper

logger = logging_helper.get_logger(__name__)


class JSONResponseExtractor:
    """Generic extractor for nested REST API JSON responses.

    Parameters
    ----------
    data_path : str
        Dot-separated navigation path into the response JSON.
        The **first segment** identifies the root array/object
        (e.g. ``"result"``).  Remaining segments define the
        *explode path* — each array encountered along the way is
        exploded so that every leaf element becomes its own record.
    field_mapping : dict, optional
        Rename keys in the final leaf records.
        ``{"out_Quantity.quantity": "quantity"}``
    store_raw : bool
        If ``True`` every record includes a ``_raw_json`` field with
        the serialized root-level item it originated from.  Only kept
        if ``_raw_json`` is also in *schema_fields* (when provided).
    parent_context_fields : list of str, optional
        Explicit list of parent scalar fields to carry into child
        records.  When ``None`` (default) **all** scalar fields at
        every intermediate level are carried forward automatically.
    schema_fields : list of str, optional
        Column names defined in the data-contract schema.  When
        provided, every extracted record is filtered to contain
        **only** these keys (applied after field mapping).  Keys
        not present in *schema_fields* are silently dropped.
    """

    def __init__(
        self,
        data_path: str = "result",
        field_mapping: Optional[Dict[str, str]] = None,
        store_raw: bool = True,
        parent_context_fields: Optional[List[str]] = None,
        schema_fields: Optional[List[str]] = None,
    ) -> None:
        segments = data_path.split(".")
        self.root_key = segments[0]              # e.g. "result"
        self.explode_segments = segments[1:]      # e.g. ["Doc","TimeSeries","Period","Point"]
        self.field_mapping = field_mapping or {}
        self.store_raw = store_raw
        self.parent_context_fields = parent_context_fields
        self.schema_fields: Optional[set] = set(schema_fields) if schema_fields else None

        logger.debug(
            f"JSONResponseExtractor: root_key={self.root_key}, "
            f"explode_segments={self.explode_segments}, "
            f"field_mapping={self.field_mapping}, "
            f"schema_fields={self.schema_fields}"
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, response_data: dict) -> List[Dict[str, Any]]:
        """Extract records from *response_data*.

        1. Navigate to ``root_key``.
        2. For each item in the root array, auto-extract the first layer.
        3. If explode segments are configured, recursively explode.
        4. Apply field mapping renames.

        Returns a flat list of dicts ready to be converted to ``Row``.
        """
        root_items = self._navigate_to_root(response_data)

        if not root_items:
            logger.warning("No data found at root_key=%s", self.root_key)
            return []

        all_records: List[Dict[str, Any]] = []

        for item in root_items:
            raw_json = json.dumps(item, default=str) if self.store_raw else None

            if self.explode_segments:
                # Phase 2: walk the explode path
                leaf_records = self._explode_path(item, self.explode_segments, {})
            else:
                # Phase 1 only: first-layer extraction
                leaf_records = [self._extract_first_layer(item)]

            # Attach _raw_json, apply field mapping, and filter to schema
            for rec in leaf_records:
                if raw_json is not None:
                    rec["_raw_json"] = raw_json
                rec = self._apply_field_mapping(rec)
                rec = self._filter_to_schema(rec)
                all_records.append(rec)

        logger.info(
            "Extracted %d records (root items=%d, explode_segments=%s)",
            len(all_records),
            len(root_items),
            self.explode_segments or "none",
        )
        return all_records

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _navigate_to_root(self, data: dict) -> List[dict]:
        """Return the items at ``self.root_key``.

        Handles the common case where the root is a list *or* a single
        dict.
        """
        root = data.get(self.root_key, [])
        if isinstance(root, dict):
            return [root]
        if isinstance(root, list):
            return root
        # Unexpected type – wrap it
        return [root] if root else []

    def _extract_first_layer(self, item: dict) -> Dict[str, Any]:
        """Auto-extract scalars; serialize nested values as JSON strings.

        Nested dicts and lists are stored as JSON strings so that the
        record can always be turned into a ``Row`` without requiring
        a complex schema.
        """
        record: Dict[str, Any] = {}
        for key, value in item.items():
            if isinstance(value, (dict, list)):
                # Serialize complex types as JSON strings
                record[key] = json.dumps(value, default=str)
            else:
                record[key] = value
        return record

    def _explode_path(
        self,
        data: Any,
        segments: List[str],
        parent_context: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Recursively navigate *segments*, exploding arrays along the way.

        At each level:
        - Scalar fields are collected as *parent context* (carried down).
        - The next segment is looked up; if it's a list, each element
          is processed recursively.  If it's a dict, a single recursion.
        - At the last segment the leaf items are returned with all
          accumulated parent context merged in.
        """
        if not segments:
            # Base case – we've arrived at a leaf
            if isinstance(data, dict):
                flat = self._extract_first_layer(data)
                flat.update(parent_context)
                return [flat]
            if isinstance(data, list):
                results = []
                for item in data:
                    if isinstance(item, dict):
                        flat = self._extract_first_layer(item)
                        flat.update(parent_context)
                        results.append(flat)
                    else:
                        results.append({**parent_context, "_value": item})
                return results
            # Scalar leaf
            return [{**parent_context, "_value": data}]

        current_segment = segments[0]
        remaining = segments[1:]

        # Collect scalar context from current level
        context = dict(parent_context)
        if isinstance(data, dict):
            context.update(self._collect_context(data, current_segment))

        # Navigate into the next segment
        if isinstance(data, dict):
            child = data.get(current_segment)
        else:
            # data is not a dict – cannot navigate further
            logger.warning(
                "Expected dict at segment '%s', got %s. Skipping.",
                current_segment,
                type(data).__name__,
            )
            return []

        if child is None:
            logger.debug("Segment '%s' not found in data, returning empty.", current_segment)
            return []

        # Normalise to list for uniform handling
        children = child if isinstance(child, list) else [child]

        # Recurse into each child
        results: List[Dict[str, Any]] = []
        for child_item in children:
            results.extend(self._explode_path(child_item, remaining, context))

        return results

    def _collect_context(self, data: dict, skip_key: str, prefix: str = "") -> Dict[str, Any]:
        """Collect scalar fields from *data* to use as parent context.

        *skip_key* is the segment we're about to descend into and is
        therefore excluded from context.

        Nested dicts are flattened with dot-separated keys so that
        field_mapping entries like ``"timeInterval.start": "period_start"``
        work correctly.  Lists are serialized as JSON strings.
        """
        context: Dict[str, Any] = {}
        for key, value in data.items():
            if key == skip_key and not prefix:
                continue
            full_key = f"{prefix}{key}" if prefix else key
            if self.parent_context_fields is not None:
                # Only include explicitly requested fields
                if full_key not in self.parent_context_fields:
                    # Still recurse into dicts to find nested matches
                    if isinstance(value, dict):
                        context.update(self._collect_context(value, skip_key="", prefix=f"{full_key}."))
                    continue
            if isinstance(value, dict):
                # Flatten nested dicts with dot-separated keys
                context.update(self._collect_context(value, skip_key="", prefix=f"{full_key}."))
            elif isinstance(value, list):
                context[full_key] = json.dumps(value, default=str)
            else:
                context[full_key] = value
        return context

    def _apply_field_mapping(self, record: dict) -> dict:
        """Rename keys in *record* according to ``self.field_mapping``."""
        if not self.field_mapping:
            return record

        mapped: Dict[str, Any] = {}
        for key, value in record.items():
            new_key = self.field_mapping.get(key, key) or key
            mapped[new_key] = value
        return mapped

    def _filter_to_schema(self, record: dict) -> dict:
        """Keep only keys present in ``self.schema_fields`` and pad
        missing columns with ``None``.

        If *schema_fields* was not provided, the record is returned
        unchanged.  This ensures the output always matches the
        data-contract schema exactly — no extra and no missing columns.
        """
        if self.schema_fields is None:
            return record

        # Build output with exactly the schema columns
        filtered = {col: record.get(col) for col in self.schema_fields}

        # Log diagnostics
        dropped = set(record.keys()) - self.schema_fields
        if dropped:
            logger.debug("Dropped keys not in schema: %s", dropped)

        missing = self.schema_fields - set(record.keys())
        if missing:
            logger.debug("Schema columns not found in record (set to None): %s", missing)

        return filtered
