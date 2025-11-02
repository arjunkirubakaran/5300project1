# Heuristic rules and optimizer pipeline
from __future__ import annotations
from typing import List, Tuple, Dict, Any, Optional
from .logical_tree import Node, pi, sigma, join

def _collect_scans(n: Node) -> List[Node]:
    if n.op == "Scan":
        return [n]
    out = []
    for c in n.children:
        out += _collect_scans(c)
    return out

def breakup_conjuncts(n: Node) -> Node:
    # already done in front-end for WHERE; here we split Select nodes that have AND textuals (best-effort)
    import re
    def helper(node: Node) -> Node:
        if node.op == "Select" and " AND " in node.props.get("pred",""):
            parts = re.split(r"\s+AND\s+", node.props["pred"])
            cur = node.children[0]
            for p in parts[::-1]:
                cur = Node("Select", children=[cur], props={"pred": p, "pred_attrs": node.props.get("pred_attrs",[])})
            return cur
        node.children = [helper(c) for c in node.children]
        return node
    return helper(n)

def selection_pushdown(n: Node) -> Node:
    # push Select as low as possible when predicate touches only one relation alias
    def aliases(node: Node) -> set:
        if node.op == "Scan":
            return {node.props["alias"]}
        s = set()
        for c in node.children:
            s |= aliases(c)
        return s

    def helper(node: Node) -> Node:
        if node.op == "Select":
            child = helper(node.children[0])
            node.children[0] = child
            # find involved aliases by looking at pred_attrs "A.col" -> "A"
            involved = {a.split(".")[0] for a in node.props.get("pred_attrs",[]) if "." in a}
            if not involved:
                return node  # keep at current level (could be constant filter)
            # Try pushing into left or right if child is a join/cross and only one side referenced
            if child.op in {"Cross", "Join"}:
                left, right = child.children
                la, ra = aliases(left), aliases(right)
                if involved <= la:
                    node.children[0] = left
                    child.children[0] = helper(node)  # reattach
                    return child
                if involved <= ra:
                    node.children[0] = right
                    child.children[1] = helper(node)
                    return child
        else:
            node.children = [helper(c) for c in node.children]
        return node
    return helper(n)

def joinize(n: Node) -> Node:
    # Convert Select above Cross into Join when predicate is an equality between attrs from both sides
    import re
    def helper(node: Node) -> Node:
        node.children = [helper(c) for c in node.children]
        if node.op == "Select" and node.children[0].op == "Cross":
            pred = node.props.get("pred","")
            m = re.match(r"\s*([\w]+)\.([\w]+)\s*=\s*([\w]+)\.([\w]+)\s*$", pred)
            if m:
                # build Join
                left, right = node.children[0].children
                return Node("Join", children=[left, right],
                            props={"on": pred,
                                   "join_keys_left": [f"{m.group(1)}.{m.group(2)}"],
                                   "join_keys_right": [f"{m.group(3)}.{m.group(4)}"]})
        return node
    return helper(n)

def projection_pushdown(n: Node) -> Node:
    # compute needed attributes per subtree and insert Projects
    def helper(node: Node, needed: set) -> Node:
        if node.op == "Scan":
            # Limit to needed attrs of this relation if not SELECT *
            if "*" in needed or not needed:
                return node
            attrs = [a for a in needed if a.split(".")[0] == node.props["alias"]]
            if attrs:
                return Node("Project", children=[node], props={"attrs": sorted(attrs)})
            return node
        if node.op in {"Select","Having"}:
            pred_attrs = set(node.props.get("pred_attrs",[]))
            child = helper(node.children[0], needed | pred_attrs)
            node.children[0] = child
            return node
        if node.op in {"Join","Cross"}:
            left, right = node.children
            # split needed by side + join keys
            keys = set(node.props.get("join_keys_left",[])) | set(node.props.get("join_keys_right",[]))
            left_needed = {a for a in needed if a.split(".")[0] in _aliases(left)} | {k for k in keys if k.split(".")[0] in _aliases(left)}
            right_needed = {a for a in needed if a.split(".")[0] in _aliases(right)} | {k for k in keys if k.split(".")[0] in _aliases(right)}
            node.children[0] = helper(left, left_needed)
            node.children[1] = helper(right, right_needed)
            return node
        if node.op == "Group":
            gb = set(node.props.get("group_by",[]))
            # Collect attrs mentioned in aggs as well (simple column refs inside agg)
            agg_attrs = set()
            for v in node.props.get("aggs",{}).values():
                # very simple parse: look for A.B tokens
                for tok in v.replace("`","").replace("("," ").replace(")"," ").replace(","," ").split():
                    if "." in tok:
                        agg_attrs.add(tok)
            child_needed = gb | agg_attrs
            node.children[0] = helper(node.children[0], child_needed)
            return node
        if node.op == "Project":
            attrs = set(node.props.get("attrs",[]))
            # if SELECT * -> push needed from parent instead
            attrs = needed if ("*" in attrs or not attrs) else attrs
            node.children[0] = helper(node.children[0], attrs)
            return node
        if node.op == "Order":
            ob = set(node.props.get("order_by",[]))
            node.children[0] = helper(node.children[0], needed | ob)
            return node
        # default
        node.children = [helper(c, needed) for c in node.children]
        return node

    def _aliases(node: Node) -> set:
        if node.op == "Scan":
            return {node.props["alias"]}
        s = set()
        for c in node.children:
            s |= _aliases(c)
        return s

    # compute needed attrs from root outward: the project/ order define needs
    needs = set()
    if n.op == "Project":
        needs = set(n.props.get("attrs",[]))
    elif n.op == "Order":
        needs = set(n.props.get("order_by",[]))
    return helper(n, needs)

def reorder_joins(n: Node) -> Node:
    # Greedy: collect a list of base relations with attached selections, order by (#selections desc)
    def collect_factors(node: Node, acc: List[Node]) -> None:
        if node.op in {"Join","Cross"}:
            collect_factors(node.children[0], acc)
            collect_factors(node.children[1], acc)
        else:
            acc.append(node)

    def count_selections(node: Node) -> int:
        if node.op == "Select":
            return 1 + count_selections(node.children[0])
        total = 0
        for c in node.children:
            total += count_selections(c)
        return total

    def rebuild_chain(factors: List[Node]) -> Node:
        cur = factors[0]
        for nxt in factors[1:]:
            cur = Node("Join", children=[cur, nxt], props={"on": None, "join_keys_left": [], "join_keys_right": []})
        return cur

    def helper(node: Node) -> Node:
        if node.op in {"Join","Cross"}:
            factors: List[Node] = []
            collect_factors(node, factors)
            factors = [helper(f) for f in factors]
            factors.sort(key=count_selections, reverse=True)
            return rebuild_chain(factors)
        node.children = [helper(c) for c in node.children]
        return node

    return helper(n)

def having_to_where(n: Node) -> Node:
    # If Having predicate references no aggregated expressions, move as a Select below Group
    def helper(node: Node) -> Node:
        if node.op == "Having":
            pred = node.props.get("pred","")
            # naive check: contains "(" after an aggregate name => keep in Having
            import re
            if re.search(r"\b(SUM|COUNT|AVG|MIN|MAX)\s*\(", pred, re.IGNORECASE):
                node.children[0] = helper(node.children[0])
                return node
            child = helper(node.children[0])
            # insert as Select below Group
            g = child
            sel = Node("Select", children=[g.children[0]], props=node.props)
            g.children[0] = sel
            return g  # drop Having
        node.children = [helper(c) for c in node.children]
        return node
    return helper(n)

def dedup_selections(n: Node) -> Node:
    # Remove duplicate Select predicates in a chain
    def helper(node: Node, seen: set) -> Node:
        if node.op == "Select":
            pred = node.props.get("pred","")
            if pred in seen:
                return helper(node.children[0], seen)
            seen.add(pred)
            node.children[0] = helper(node.children[0], seen)
            return node
        node.children = [helper(c, seen) for c in node.children]
        return node
    return helper(n, set())

def optimize_pipeline(root: Node, trace: List[Tuple[str, Node]]) -> Node:
    def step(name: str, f, n: Node) -> Node:
        out = f(n)
        trace.append((name, out))
        return out

    trace.append(("Step 0 — Canonical", root))
    root = step("Step 1 — BreakUpConjuncts", breakup_conjuncts, root)
    root = step("Step 2 — SelectionPushdown", selection_pushdown, root)
    root = step("Step 3 — JoinizeSelections", joinize, root)
    root = step("Step 4 — ProjectionPushdown", projection_pushdown, root)
    root = step("Step 5 — ReorderJoins", reorder_joins, root)
    root = step("Step 6 — HavingToWhere", having_to_where, root)
    root = step("Step 7 — DedupSelections", dedup_selections, root)
    trace.append(("Step 8 — Final", root))
    return root
