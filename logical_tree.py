# Lightweight logical algebra tree and pretty printer

from dataclasses import dataclass, field
from typing import List, Optional, Any, Dict, Set

@dataclass
class Node:
    op: str
    children: List["Node"] = field(default_factory=list)
    props: Dict[str, Any] = field(default_factory=dict)

    def copy(self) -> "Node":
        import copy
        return copy.deepcopy(self)

    # preorder textual rendering
    def render(self, indent: int = 0) -> str:
        pad = "  " * indent
        head = f"{pad}{self.op}{(' ' + str(self.props)) if self.props else ''}"
        if not self.children:
            return head
        return head + "\n" + "\n".join(c.render(indent + 1) for c in self.children)

    # utility to collect needed attributes bottom-up
    def needed_attrs(self) -> Set[str]:
        # naive: union of projections, predicates, group/order cols
        need = set()
        if "attrs" in self.props:
            need |= set(self.props["attrs"])
        for k in ["pred_attrs", "group_by", "order_by", "join_keys_left", "join_keys_right"]:
            if k in self.props and self.props[k]:
                need |= set(self.props[k])
        for ch in self.children:
            need |= ch.needed_attrs()
        return need

def leaf_scan(relation: str, alias: Optional[str] = None) -> Node:
    return Node("Scan", props={"relation": relation, "alias": alias or relation})

def sigma(pred: str, attrs: List[str]) -> Node:
    return Node("Select", props={"pred": pred, "pred_attrs": attrs})

def pi(attrs: List[str]) -> Node:
    return Node("Project", props={"attrs": attrs})

def join(kind: str, on: Optional[str], left: Node, right: Node,
         keys_left: List[str] = None, keys_right: List[str] = None) -> Node:
    return Node(kind, children=[left, right], props={"on": on, "join_keys_left": keys_left or [], "join_keys_right": keys_right or []})

def group(group_by: List[str], aggs: Dict[str, str]) -> Node:
    return Node("Group", props={"group_by": group_by, "aggs": aggs})

def having(pred: str, attrs: List[str]) -> Node:
    return Node("Having", props={"pred": pred, "pred_attrs": attrs})

def order(order_by: List[str]) -> Node:
    return Node("Order", props={"order_by": order_by})
