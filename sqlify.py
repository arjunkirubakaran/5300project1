# Convert a final logical tree back to SQL (best-effort)
from __future__ import annotations
from typing import List, Tuple
from .logical_tree import Node

def sql_from_tree(n: Node) -> str:
    # Very small emitter that expects: Order(Project(Group?(Select* (Join/Scan))))
    order_by = None
    if n.op == "Order":
        order_by = n.props.get("order_by",[])
        n = n.children[0]
    assert n.op == "Project"
    select_attrs = n.props.get("attrs",[])
    n = n.children[0]
    having = None
    if n.op == "Having":
        having = n.props.get("pred")
        n = n.children[0]
    group_by = None
    aggs = {}
    if n.op == "Group":
        group_by = n.props.get("group_by",[])
        aggs = n.props.get("aggs",{})
        n = n.children[0]

    # Pull up all selections into a WHERE
    where_preds = []
    def strip_selects(node: Node) -> Node:
        if node.op == "Select":
            where_preds.append(node.props.get("pred",""))
            return strip_selects(node.children[0])
        node.children = [strip_selects(c) for c in node.children]
        return node
    n = strip_selects(n)

    # Emit FROM with joins if present
    def emit_from(node: Node) -> str:
        if node.op == "Scan":
            alias = node.props.get("alias")
            rel = node.props.get("relation")
            return f"{rel} {alias}" if alias and alias != rel else rel
        if node.op in {"Join","Cross"}:
            left = emit_from(node.children[0])
            right = emit_from(node.children[1])
            if node.op == "Join" and node.props.get("on"):
                return f"{left} JOIN {right} ON {node.props['on']}"
            return f"{left}, {right}"
        return "(SUBQ)"
    from_sql = emit_from(n)

    # SELECT list (combine aggs)
    if not select_attrs or "*" in select_attrs:
        select_list = "*"
    else:
        select_list = ", ".join(select_attrs)

    if aggs:
        agg_list = ", ".join(f"{v} AS {k}" for k, v in aggs.items())
        if select_list != "*":
            select_list = ", ".join([select_list, agg_list])
        else:
            select_list = agg_list

    sql = f"SELECT {select_list}\nFROM {from_sql}"
    if where_preds:
        sql += "\nWHERE " + " AND ".join(where_preds)
    if group_by:
        sql += "\nGROUP BY " + ", ".join(group_by)
    if having:
        sql += "\nHAVING " + having
    if order_by:
        sql += "\nORDER BY " + ", ".join(order_by)
    sql += ";"
    return sql
