from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_map():
    flow = Dataflow("test")

    inp = [0, 1, 2]
    rows = op.input("inp", flow, TestingSource(inp))

    def add_one(item):
        return item + 1

    out = []
    processed = op.map("process", rows, add_one)

    op.output("out", processed, TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted([1, 2, 3])
