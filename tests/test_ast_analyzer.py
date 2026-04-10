"""Unit tests for spark_perf_lint.engine.ast_analyzer."""

from __future__ import annotations

import pytest

from spark_perf_lint.engine.ast_analyzer import (
    ASTAnalyzer,
    MethodCallInfo,
    ParseError,
)

# =============================================================================
# Helpers
# =============================================================================


def ana(code: str, filename: str = "test.py") -> ASTAnalyzer:
    """Shorthand: build an ASTAnalyzer from an inline code string."""
    return ASTAnalyzer.from_source(code, filename)


def method_names(calls: list[MethodCallInfo]) -> list[str]:
    return [c.method_name for c in calls]


def lines(items: list) -> list[int]:
    return [i.line for i in items]


# =============================================================================
# Parsing
# =============================================================================


class TestParsing:
    def test_valid_empty_file(self):
        a = ana("")
        assert a.tree is not None
        assert a.source_lines == []

    def test_valid_simple_python(self):
        a = ana("x = 1\ny = 2\n")
        assert len(a.source_lines) == 2

    def test_valid_pyspark_file(self):
        code = "from pyspark.sql import SparkSession\nspark = SparkSession.builder.getOrCreate()\n"
        a = ana(code)
        assert a.has_spark_imports()

    def test_from_file(self, tmp_path):
        f = tmp_path / "etl.py"
        f.write_text("import os\n", encoding="utf-8")
        a = ASTAnalyzer.from_file(f)
        assert a.filename == str(f)
        assert not a.has_spark_imports()

    def test_from_file_missing_raises(self, tmp_path):
        with pytest.raises(OSError):
            ASTAnalyzer.from_file(tmp_path / "nonexistent.py")

    def test_syntax_error_raises_parse_error(self):
        with pytest.raises(ParseError) as exc_info:
            ana("def foo(:\n    pass\n")
        assert exc_info.value.filename == "test.py"
        assert exc_info.value.lineno is not None

    def test_parse_error_message_includes_filename(self):
        with pytest.raises(ParseError, match="bad_file.py"):
            ana("x = (", filename="bad_file.py")

    def test_no_spark_imports(self):
        a = ana("import os\nimport json\n")
        assert not a.has_spark_imports()

    def test_pyspark_import_from(self):
        a = ana("from pyspark.sql import DataFrame\n")
        assert a.has_spark_imports()

    def test_pyspark_import_direct(self):
        a = ana("import pyspark\n")
        assert a.has_spark_imports()

    def test_repr_contains_filename(self):
        a = ana("x = 1\n", filename="my_job.py")
        assert "my_job.py" in repr(a)


# =============================================================================
# find_method_calls
# =============================================================================


class TestFindMethodCalls:
    def test_simple_call(self):
        a = ana("df.show()\n")
        calls = a.find_method_calls("show")
        assert len(calls) == 1
        assert calls[0].method_name == "show"
        assert calls[0].line == 1

    def test_no_match_returns_empty(self):
        a = ana("df.show()\n")
        assert a.find_method_calls("collect") == []

    def test_chained_calls_all_found(self):
        a = ana("df.filter('x > 0').select('a', 'b').show()\n")
        assert len(a.find_method_calls("filter")) == 1
        assert len(a.find_method_calls("select")) == 1
        assert len(a.find_method_calls("show")) == 1

    def test_chain_preserved(self):
        a = ana("df.filter('x > 0').join(other, 'id').show()\n")
        join_calls = a.find_method_calls("join")
        assert len(join_calls) == 1
        assert "filter" in join_calls[0].chain
        assert "join" in join_calls[0].chain

    def test_receiver_src_simple(self):
        a = ana("df.cache()\n")
        call = a.find_method_calls("cache")[0]
        assert call.receiver_src == "df"

    def test_receiver_src_chained(self):
        a = ana("df.filter('x > 1').cache()\n")
        call = a.find_method_calls("cache")[0]
        assert "filter" in call.receiver_src

    def test_args_captured(self):
        a = ana("df.repartition(200)\n")
        call = a.find_method_calls("repartition")[0]
        assert len(call.args) == 1

    def test_kwargs_captured(self):
        a = ana("spark.read.csv('/data', header=True)\n")
        calls = a.find_method_calls("csv")
        assert len(calls) == 1
        assert "header" in calls[0].kwargs

    def test_multiple_occurrences_sorted_by_line(self):
        code = "df1.cache()\ndf2.cache()\ndf3.cache()\n"
        calls = ana(code).find_method_calls("cache")
        assert [c.line for c in calls] == [1, 2, 3]

    def test_nested_call_args(self):
        # groupBy result passed as receiver
        a = ana("df.groupBy('key').agg({'val': 'sum'}).collect()\n")
        collect_calls = a.find_method_calls("collect")
        assert len(collect_calls) == 1
        assert "groupBy" in collect_calls[0].chain or "agg" in collect_calls[0].chain

    def test_find_all_method_calls(self):
        a = ana("df.filter('x').join(other, 'id').cache().show()\n")
        all_calls = a.find_all_method_calls()
        names = {c.method_name for c in all_calls}
        assert {"filter", "join", "cache", "show"}.issubset(names)

    def test_method_calls_in_loop(self):
        code = "for c in cols:\n    df = df.withColumn(c, df[c])\n"
        a = ana(code)
        loop_calls = a.find_method_calls_in_loops("withColumn")
        assert len(loop_calls) == 1
        loop, count = loop_calls[0]
        assert loop.loop_type == "for"
        assert count == 1


# =============================================================================
# find_attribute_accesses
# =============================================================================


class TestFindAttributeAccesses:
    def test_simple_attribute(self):
        a = ana("x = df.rdd\n")
        attrs = a.find_attribute_accesses("rdd")
        assert len(attrs) == 1
        assert attrs[0].attr_name == "rdd"
        assert attrs[0].receiver_src == "df"

    def test_no_match(self):
        a = ana("x = df.columns\n")
        assert a.find_attribute_accesses("schema") == []

    def test_multiple_accesses(self):
        code = "a = df.rdd\nb = df2.rdd\n"
        attrs = ana(code).find_attribute_accesses("rdd")
        assert len(attrs) == 2
        assert attrs[0].line < attrs[1].line


# =============================================================================
# find_spark_session_configs
# =============================================================================


class TestFindSparkSessionConfigs:
    def test_builder_config_single(self):
        code = (
            "spark = SparkSession.builder"
            '.config("spark.executor.memory", "8g")'
            ".getOrCreate()\n"
        )
        a = ana(code)
        configs = a.find_spark_session_configs()
        keys = [c.key for c in configs]
        assert "spark.executor.memory" in keys

    def test_builder_config_multiple(self):
        code = """\
spark = (
    SparkSession.builder
    .config("spark.executor.memory", "8g")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)
"""
        configs = ana(code).find_spark_session_configs()
        keys = [c.key for c in configs]
        assert "spark.executor.memory" in keys
        assert "spark.sql.adaptive.enabled" in keys
        assert "spark.sql.shuffle.partitions" in keys

    def test_builder_config_order(self):
        """Configs appear in source order (end_line sorted)."""
        code = """\
spark = (
    SparkSession.builder
    .config("first.key", "1")
    .config("second.key", "2")
    .config("third.key", "3")
    .getOrCreate()
)
"""
        configs = ana(code).find_spark_session_configs()
        keys = [c.key for c in configs]
        assert keys == ["first.key", "second.key", "third.key"]

    def test_conf_set_pattern(self):
        code = 'spark.conf.set("spark.sql.shuffle.partitions", "400")\n'
        configs = ana(code).find_spark_session_configs()
        assert len(configs) == 1
        assert configs[0].key == "spark.sql.shuffle.partitions"
        assert configs[0].value == "400"
        assert configs[0].method == "conf_set"

    def test_builder_config_method_label(self):
        code = 'SparkSession.builder.config("spark.executor.memory", "4g").getOrCreate()\n'
        configs = ana(code).find_spark_session_configs()
        assert configs[0].method == "builder_config"

    def test_non_string_value_is_none(self):
        code = "spark.conf.set('spark.executor.cores', executor_cores)\n"
        configs = ana(code).find_spark_session_configs()
        assert len(configs) == 1
        assert configs[0].key == "spark.executor.cores"
        assert configs[0].value is None

    def test_get_config_value(self):
        code = 'SparkSession.builder.config("spark.sql.adaptive.enabled", "true").getOrCreate()\n'
        a = ana(code)
        assert a.get_config_value("spark.sql.adaptive.enabled") == "true"

    def test_get_config_value_missing(self):
        a = ana("x = 1\n")
        assert a.get_config_value("spark.sql.adaptive.enabled") is None

    def test_get_config_value_last_wins(self):
        code = """\
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.shuffle.partitions", "400")
"""
        assert ana(code).get_config_value("spark.sql.shuffle.partitions") == "400"

    def test_no_configs_returns_empty(self):
        a = ana("df = spark.read.parquet('/data')\n")
        assert a.find_spark_session_configs() == []


# =============================================================================
# find_spark_read_writes
# =============================================================================


class TestFindSparkReadWrites:
    def test_read_parquet(self):
        a = ana("df = spark.read.parquet('/data/events')\n")
        reads = a.find_reads()
        assert len(reads) == 1
        assert reads[0].format == "parquet"
        assert reads[0].path == "/data/events"
        assert reads[0].operation == "read"

    def test_read_csv(self):
        a = ana("df = spark.read.csv('/data/file.csv')\n")
        reads = a.find_reads()
        assert len(reads) == 1
        assert reads[0].format == "csv"

    def test_read_json(self):
        reads = ana("df = spark.read.json('/data')\n").find_reads()
        assert reads[0].format == "json"

    def test_read_orc(self):
        reads = ana("df = spark.read.orc('/data')\n").find_reads()
        assert reads[0].format == "orc"

    def test_read_with_format_method(self):
        a = ana("df = spark.read.format('delta').load('/data/table')\n")
        reads = a.find_reads()
        assert len(reads) == 1
        assert reads[0].format == "delta"

    def test_read_with_option(self):
        a = ana("df = spark.read.option('mergeSchema', 'true').parquet('/data')\n")
        reads = a.find_reads()
        assert len(reads) == 1
        assert "mergeSchema" in reads[0].options

    def test_read_infer_schema_detected(self):
        a = ana("df = spark.read.option('inferSchema', 'true').csv('/data')\n")
        reads = a.find_reads()
        assert reads[0].infer_schema is True

    def test_read_infer_schema_false_when_absent(self):
        a = ana("df = spark.read.parquet('/data')\n")
        assert a.find_reads()[0].infer_schema is False

    def test_read_with_schema(self):
        a = ana("df = spark.read.schema(my_schema).parquet('/data')\n")
        assert a.find_reads()[0].has_schema is True

    def test_write_parquet(self):
        a = ana("df.write.parquet('/output')\n")
        writes = a.find_writes()
        assert len(writes) == 1
        assert writes[0].format == "parquet"
        assert writes[0].operation == "write"

    def test_write_with_mode(self):
        a = ana("df.write.mode('overwrite').parquet('/output')\n")
        writes = a.find_writes()
        assert writes[0].write_mode == "overwrite"

    def test_write_with_partition_by(self):
        a = ana("df.write.partitionBy('date', 'region').parquet('/output')\n")
        writes = a.find_writes()
        assert "date" in writes[0].partitioned_by
        assert "region" in writes[0].partitioned_by

    def test_write_with_bucket_by(self):
        a = ana("df.write.bucketBy(32, 'user_id').parquet('/output')\n")
        writes = a.find_writes()
        assert writes[0].bucketed_by is not None
        num, cols = writes[0].bucketed_by
        assert num == 32
        assert "user_id" in cols

    def test_write_save(self):
        a = ana("df.write.format('delta').save('/output')\n")
        writes = a.find_writes()
        assert len(writes) == 1
        assert writes[0].format == "delta"

    def test_no_spurious_io_from_intermediate_calls(self):
        """option/mode/partitionBy must NOT generate extra SparkIOInfo entries."""
        a = ana("df.write.mode('overwrite').option('compression', 'snappy').parquet('/out')\n")
        writes = a.find_writes()
        assert len(writes) == 1
        assert writes[0].options.get("compression") == "snappy"

    def test_read_and_write_separated(self):
        code = "df = spark.read.parquet('/in')\n" "df.write.mode('overwrite').parquet('/out')\n"
        a = ana(code)
        assert len(a.find_reads()) == 1
        assert len(a.find_writes()) == 1

    def test_find_spark_read_writes_combined(self):
        code = "df = spark.read.parquet('/in')\n" "df.write.parquet('/out')\n"
        io = ana(code).find_spark_read_writes()
        ops = [x.operation for x in io]
        assert "read" in ops
        assert "write" in ops


# =============================================================================
# find_function_definitions
# =============================================================================


class TestFindFunctionDefinitions:
    def test_plain_function(self):
        a = ana("def my_func(x):\n    return x\n")
        defs = a.find_function_definitions()
        assert len(defs) == 1
        assert defs[0].name == "my_func"
        assert defs[0].is_udf is False
        assert defs[0].udf_type is None

    def test_udf_decorator(self):
        code = "@udf(returnType='string')\ndef clean(val):\n    return val.strip()\n"
        defs = ana(code).find_function_definitions()
        assert defs[0].is_udf is True
        assert defs[0].udf_type == "udf"
        assert "udf" in defs[0].decorators

    def test_pandas_udf_decorator(self):
        code = "@pandas_udf('double')\ndef mean_fn(s):\n    return s.mean()\n"
        defs = ana(code).find_function_definitions()
        assert defs[0].is_udf is True
        assert defs[0].udf_type == "pandas_udf"

    def test_udf_from_pyspark(self):
        code = "from pyspark.sql.functions import udf\n" "@udf\n" "def my_udf(x):\n    return x\n"
        defs = ana(code).find_function_definitions()
        udf_defs = [d for d in defs if d.is_udf]
        assert len(udf_defs) == 1

    def test_multiple_decorators(self):
        code = "@staticmethod\n@udf\ndef f(x):\n    return x\n"
        defs = ana(code).find_function_definitions()
        assert "staticmethod" in defs[0].decorators
        assert "udf" in defs[0].decorators
        assert defs[0].is_udf is True

    def test_find_udf_definitions_filter(self):
        code = "def helper(x):\n    return x\n\n" "@udf\ndef transform(val):\n    return val\n"
        a = ana(code)
        all_defs = a.find_function_definitions()
        udf_only = a.find_udf_definitions()
        assert len(all_defs) == 2
        assert len(udf_only) == 1

    def test_complexity_simple(self):
        a = ana("def f(x):\n    return x\n")
        assert a.find_function_definitions()[0].complexity == 1

    def test_complexity_with_branches(self):
        code = """\
def f(x):
    if x > 0:
        return x
    elif x < 0:
        return -x
    else:
        return 0
"""
        defs = ana(code).find_function_definitions()
        # base 1 + if 1 + elif 1 = 3
        assert defs[0].complexity >= 3

    def test_complexity_with_loop(self):
        code = "def f(items):\n    for i in items:\n        pass\n"
        assert ana(code).find_function_definitions()[0].complexity == 2

    def test_no_functions_returns_empty(self):
        assert ana("x = 1\n").find_function_definitions() == []

    def test_line_number_correct(self):
        code = "\n\ndef my_func():\n    pass\n"
        defs = ana(code).find_function_definitions()
        assert defs[0].line == 3


# =============================================================================
# find_loop_patterns
# =============================================================================


class TestFindLoopPatterns:
    def test_for_loop_detected(self):
        code = "for col in cols:\n    df = df.withColumn(col, df[col])\n"
        loops = ana(code).find_loop_patterns()
        assert len(loops) == 1
        assert loops[0].loop_type == "for"

    def test_while_loop_detected(self):
        code = "while True:\n    df = df.filter('x > 0')\n    break\n"
        loops = ana(code).find_loop_patterns()
        assert len(loops) == 1
        assert loops[0].loop_type == "while"

    def test_loop_body_calls_captured(self):
        code = "for c in cols:\n    df = df.withColumn(c, f.col(c))\n"
        loops = ana(code).find_loop_patterns()
        assert "withColumn" in loops[0].body_calls

    def test_multiple_calls_in_loop(self):
        code = """\
for c in cols:
    df = df.withColumn(c, f.col(c))
    df = df.filter(f.col(c).isNotNull())
"""
        loops = ana(code).find_loop_patterns()
        assert "withColumn" in loops[0].body_calls
        assert "filter" in loops[0].body_calls

    def test_no_loops_returns_empty(self):
        assert ana("df.show()\n").find_loop_patterns() == []

    def test_method_calls_in_loops_count(self):
        code = """\
for c in cols:
    df = df.withColumn(c, f.col(c))
    df = df.withColumn(c + '_2', f.col(c))
"""
        result = ana(code).find_method_calls_in_loops("withColumn")
        assert len(result) == 1
        loop, count = result[0]
        assert count == 2

    def test_cache_in_loop_detected(self):
        code = "for i in range(10):\n    df.cache()\n"
        result = ana(code).find_method_calls_in_loops("cache")
        assert len(result) == 1

    def test_loop_line_number(self):
        code = "\nx = 1\nfor c in cols:\n    df.show()\n"
        loops = ana(code).find_loop_patterns()
        assert loops[0].line == 3


# =============================================================================
# find_variable_assignments
# =============================================================================


class TestFindVariableAssignments:
    def test_simple_assignment(self):
        a = ana("x = 1\n")
        assigns = a.find_variable_assignments()
        assert any(a.target == "x" for a in assigns)

    def test_dataframe_assignment(self):
        a = ana("df2 = df1.join(other, 'id')\n")
        assigns = a.find_variable_assignments()
        assert assigns[0].target == "df2"
        assert "join" in assigns[0].rhs_method_calls

    def test_chained_assignment_rhs_calls(self):
        a = ana("result = df.filter('x > 0').groupBy('key').count()\n")
        assigns = a.find_variable_assignments()
        assert "filter" in assigns[0].rhs_method_calls or "count" in assigns[0].rhs_method_calls

    def test_multiple_assignments(self):
        code = "df1 = spark.read.parquet('/a')\ndf2 = spark.read.parquet('/b')\n"
        assigns = ana(code).find_variable_assignments()
        targets = [a.target for a in assigns]
        assert "df1" in targets
        assert "df2" in targets

    def test_value_src_populated(self):
        a = ana("df = spark.read.parquet('/data')\n")
        assigns = a.find_variable_assignments()
        assert assigns[0].value_src != ""

    def test_line_number(self):
        code = "\ndf = spark.read.parquet('/data')\n"
        assigns = ana(code).find_variable_assignments()
        assert assigns[0].line == 2


# =============================================================================
# get_code_context
# =============================================================================


class TestGetCodeContext:
    def test_middle_of_file(self):
        lines = ["line1", "line2", "line3", "line4", "line5"]
        a = ana("\n".join(lines))
        ctx = a.get_code_context(3, context_lines=1)
        assert ">> " in ctx
        assert "line3" in ctx
        assert "line2" in ctx
        assert "line4" in ctx

    def test_focus_line_marked(self):
        a = ana("a = 1\nb = 2\nc = 3\n")
        ctx = a.get_code_context(2)
        assert ">>    2 |" in ctx or ">>  2 |" in ctx

    def test_start_of_file(self):
        a = ana("first = 1\nsecond = 2\nthird = 3\n")
        ctx = a.get_code_context(1, context_lines=3)
        assert "first" in ctx
        # No lines before line 1 should cause an error
        assert "second" in ctx

    def test_end_of_file(self):
        a = ana("a = 1\nb = 2\nc = 3\n")
        ctx = a.get_code_context(3, context_lines=5)
        assert "c = 3" in ctx

    def test_empty_file_returns_empty_string(self):
        assert ana("").get_code_context(1) == ""

    def test_zero_context_lines(self):
        a = ana("a = 1\nb = 2\nc = 3\n")
        ctx = a.get_code_context(2, context_lines=0)
        assert "b = 2" in ctx
        assert "a = 1" not in ctx
        assert "c = 3" not in ctx

    def test_context_lines_default(self):
        lines_code = "\n".join(f"x{i} = {i}" for i in range(20))
        a = ana(lines_code)
        ctx = a.get_code_context(10)  # default context_lines=3
        # Should include lines 7–13
        assert "x9 = 9" in ctx
        assert "x10 = 10" in ctx
        assert "x11 = 11" in ctx
