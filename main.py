# CLI driver
import argparse, sys, os, textwrap
from .parser_front import parse_sql_to_tree
from .rules import optimize_pipeline
from .sqlify import sql_from_tree
from .unnesting import unnest_exists_in

def run(sql: str, trace: bool=True, unnest: bool=True, emit_sql: str=None):
    if unnest:
        sql = unnest_exists_in(sql)
    root = parse_sql_to_tree(sql)
    steps = []
    final = optimize_pipeline(root, steps)
    for name, tree in steps:
        print("="*80)
        print(name)
        print(tree.render())
    if emit_sql:
        out_sql = sql_from_tree(final)
        with open(emit_sql, "w") as f:
            f.write(out_sql)
        print("\\n[WROTE]", emit_sql)
    return 0

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--sql", required=True, help="Path to .sql file OR inline SQL")
    p.add_argument("--inline", action="store_true", help="Treat --sql as inline SQL text")
    p.add_argument("--no-unnest", action="store_true")
    p.add_argument("--no-sqlout", action="store_true")
    p.add_argument("--emit-sql", default="optimized.sql")
    args = p.parse_args(argv)

    if args.inline:
        sql_text = args.sql
    else:
        with open(args.sql, "r") as f:
            sql_text = f.read()

    return run(sql_text, trace=True, unnest=not args.no_unnest, emit_sql=None if args.no_sqlout else args.emit_sql)

if __name__ == "__main__":
    raise SystemExit(main())
