from complex_graph_splitter import split_complex_sub_graphs

def test_graph_1():
    v = sqlContext.createDataFrame([
        ("CH1", "CH"),
        ("CH2", "CH"),
        ("VAT1", "VAT"),
        ("VAT2", "VAT"),
        ("PAYE1", "PAYE"),
        ("PAYE2", "PAYE"),
    ], ["id", "type"])

    e = sqlContext.createDataFrame([
        ("CH1", "VAT1", "0.8"),
        ("CH1", "VAT2", "0.9"),
        ("CH2", "VAT2", "0.92"),
        ("VAT1", "PAYE1", "0.8"),
        ("VAT1", "PAYE2", "0.87"),
    ], ["src", "dst", 'weight'])

    assert split_complex_sub_graphs(v, e) == [['CH1', 'VAT1', 'PAYE1', 'PAYE2'], ['CH2', 'VAT2']]


def test_graph_2():
    v1 = sqlContext.createDataFrame([
        ("CH1", "CH"),
        ("CH2", "CH"),
        ("VAT1", "VAT"),
        ("VAT2", "VAT"),
    ], ["id", "type"])

    e1 = sqlContext.createDataFrame([
        ("CH1", "VAT1", "0.9"),
        ("VAT1", "VAT2", "0.8"),
        ("CH2", "VAT2", "0.92"),
    ], ["src", "dst", 'weight'])

    assert split_complex_sub_graphs(v1, e1) == [['CH1', 'VAT1'], ['CH2', 'VAT2']]


def test_graph_3():
    v2 = sqlContext.createDataFrame([
        ("CH1", "CH"),
        ("CH2", "CH"),
        ("VAT1", "VAT"),
        ("VAT2", "VAT"),
        ("PAYE1", "PAYE"),
    ], ["id", "type"])

    e2 = sqlContext.createDataFrame([
        ("CH1", "VAT1", "0.9"),
        ("CH2", "VAT1", "0.92"),
        ("CH2", "PAYE1", "0.92"),
        ("CH2", "VAT2", "0.92"),
    ], ["src", "dst", 'weight'])

    assert split_complex_sub_graphs(v2, e2) == [['CH1'], ['CH2', 'VAT1', 'VAT2', 'PAYE1']]


def test_graph_4():
    v3 = sqlContext.createDataFrame([
        ("CH1", "CH"),
        ("CH2", "CH"),
        ("VAT1", "VAT"),
        ("VAT2", "VAT"),
    ], ["id", "type"])

    e3 = sqlContext.createDataFrame([
        ("CH1", "VAT1", "0.9"),
        ("CH2", "VAT1", "0.92"),
        ("CH2", "VAT2", "0.92"),
        ("CH1", "VAT2", "0.93"),
    ], ["src", "dst", 'weight'])

    assert split_complex_sub_graphs(v3, e3) == [['CH1', 'VAT2'], ['CH2', 'VAT1']]

def test_graph_5():
    v4 = sqlContext.createDataFrame([
        ("CH1", "CH"),
        ("CH2", "CH"),
        ("VAT1", "VAT"),
        ("VAT2", "VAT"),
        ('PAYE3', 'PAYE')
    ], ["id", "type"])

    e4 = sqlContext.createDataFrame([
        ("CH1", "VAT1", "0.9"),
        ("VAT1", "VAT2", "0.7"),
        ("VAT2", "PAYE3", "0.8"),
        ("CH2", "PAYE3", "0.95"),
    ], ["src", "dst", 'weight'])

    assert split_complex_sub_graphs(v4, e4) == [['CH1', 'VAT1', 'VAT2'], ['CH2', 'PAYE3']]
