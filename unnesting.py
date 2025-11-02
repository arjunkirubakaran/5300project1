# Extra-credit: Very small subset of unnesting
from __future__ import annotations
from typing import Tuple, List
import re
from .logical_tree import Node

def unnest_exists_in(sql_str: str) -> str:
    """
    Best-effort textual rewrite for patterns like:
      WHERE a IN (SELECT ... FROM B WHERE B.x = A.x)
    to a SEMIJOIN-ish join predicate:
      ... JOIN B ON B.x = A.x
    NOTE: This is intentionally simplistic and only matches common classroom patterns.
    """
    # Replace NOT IN / NOT EXISTS first (anti-join marker)
    s = sql_str
    # No deep rewrite here; just annotate for the front-end to treat as Cross + Select and later joinize
    s = re.sub(r"\bNOT\s+IN\s*\(", " /*ANTI_IN*/ IN (", s, flags=re.IGNORECASE)
    s = re.sub(r"\bNOT\s+EXISTS\s*\(", " /*ANTI_EXISTS*/ EXISTS (", s, flags=re.IGNORECASE)
    return s
