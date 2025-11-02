# Front-end: Parse SQL to canonical logical tree using sqlglot
from typing import List, Tuple, Dict, Any
import sqlglot
from sqlglot import exp
from .logical_tree import Node, leaf_scan, sigma, pi, join, group, having, order

def _col_attrs(expr: exp.Expression) -> List[str]:
    # return ["alias.col" or "col"]
    cols = []
    for e in expr.find_all(exp.Column):
        if e.table:
            cols.append(f"{e.table}.{e.name}")
        else:
            cols.append(e.name)
    return list(dict.fromkeys(cols))  # dedupe preserve order

def parse_sql_to_tree(sql: str) -> Node:
    q = sqlglot.parse_one(sql)
    assert isinstance(q, (exp.Select, exp.Subquery, exp.Union)), "Only SELECT supported"
    if isinstance(q, exp.Subquery):
        q = q.this
    assert isinstance(q, exp.Select), "Only single-block SELECT supported"

    # FROM with (possible) joins
    # Build a left-deep tree of scans and joins in canonical form: start with cross products, attach join predicates as selections (later joinize)
    from_expr = q.args.get("from")
    assert from_expr, "Query must have FROM"
    sources = []  # List[Node]
    join_preds: List[Tuple[str, List[str]]] = []
    if isinstance(from_expr, exp.From):
        # collect base tables and explicit joins
        base = None
        def scan_of(t: exp.Expression) -> Node:
            if isinstance(t, exp.Table):
                alias = t.alias_or_name
                return leaf_scan(t.name, alias=alias)
            if isinstance(t, exp.Subquery):
                # simplify: treat as scan over subselect alias
                alias = t.alias
                return leaf_scan(alias or "subq", alias=alias or "subq")
            raise NotImplementedError("Unsupported FROM term")

        # Walk joins
        first = from_expr.expressions[0]
        base = scan_of(first)
        cur = base
        for j in from_expr.find_all(exp.Join):
            right = scan_of(j.this)
            kind = j.side or "INNER"
            # capture join predicate as a selection above product; joinization will convert
            on = j.args.get("on")
            if on:
                pred_sql = on.sql()
                join_preds.append((pred_sql, []))  # attrs fill later
            # canonical: Cross product first
            cur = join("Cross", None, cur, right)
        sources.append(cur)
    else:
        raise NotImplementedError("Unsupported FROM clause")

    # WHERE: break into conjuncts, build cascade of selections
    where = q.args.get("where")
    where_preds: List[Tuple[str, List[str]]] = []
    if where:
        cond = where.this
        conjuncts = list(cond.flatten() if isinstance(cond, exp.And) else [cond])
        # naive split by AND
        def split_and(node):
            if isinstance(node, exp.And):
                return split_and(node.left) + split_and(node.right)
            return [node]
        conjuncts = split_and(cond)
        for c in conjuncts:
            where_preds.append((c.sql(), _col_attrs(c)))

    # SELECT list: split into plain attrs and aggregates
    select_attrs: List[str] = []
    aggs: Dict[str, str] = {}
    for p in q.expressions:
        if isinstance(p, exp.Alias):
            e = p.this
            alias = p.alias
        else:
            e = p
            alias = None
        if isinstance(e, (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)):
            aggs[alias or e.sql()] = e.sql()
        elif isinstance(e, exp.Star):
            select_attrs.append("*")
        else:
            # Column or expression
            select_attrs += _col_attrs(e) or [e.sql()]

    # GROUP BY / HAVING
    group_by_cols = []
    if gb := q.args.get("group"):
        for g in gb.expressions:
            group_by_cols += _col_attrs(g) or [g.sql()]
    having_pred = None
    having_attrs: List[str] = []
    if hv := q.args.get("having"):
        having_pred = hv.this.sql()
        having_attrs = _col_attrs(hv.this)

    # ORDER BY
    order_cols = []
    if ob := q.args.get("order"):
        for o in ob.expressions:
            order_cols += _col_attrs(o.this) or [o.this.sql()]

    # Build canonical tree:
    # FROM (cross-products) -> selections (WHERE + collected join on) -> group -> having -> project -> order
    root = sources[0]
    for pred, attrs in where_preds + [(jp, []) for jp, _ in join_preds]:
        root = Node("Select", children=[root], props={"pred": pred, "pred_attrs": attrs})
    if group_by_cols or aggs:
        root = Node("Group", children=[root], props={"group_by": group_by_cols, "aggs": aggs})
        if having_pred:
            root = Node("Having", children=[root], props={"pred": having_pred, "pred_attrs": having_attrs})
    # Projection (SELECT list). If SELECT *, leave empty to be resolved in projection pushdown
    root = Node("Project", children=[root], props={"attrs": select_attrs})
    if order_cols:
        root = Node("Order", children=[root], props={"order_by": order_cols})
    return root
