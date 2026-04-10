"""Unit tests for D06 Caching and persistence rules (SPL-D06-001 – SPL-D06-008).

Each rule has at least:
- A positive test (rule fires)
- A negative test (rule does not fire)

Tests call ``rule.check(analyzer, config)`` directly to avoid registry coupling.
"""

from __future__ import annotations

from spark_perf_lint.config import LintConfig
from spark_perf_lint.engine.ast_analyzer import ASTAnalyzer
from spark_perf_lint.rules.d06_caching import (
    CacheAfterRepartitionRule,
    CacheBeforeFilterRule,
    CacheInLoopRule,
    CacheUsedOnceRule,
    CacheWithoutUnpersistRule,
    CheckpointVsCacheMisuseRule,
    MemoryOnlyStorageLevelRule,
    ReusedDataFrameWithoutCacheRule,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARK_HDR = "from pyspark.sql import SparkSession\nfrom pyspark.sql.functions import *\n"


def _a(code: str) -> ASTAnalyzer:
    return ASTAnalyzer.from_source(code, filename="test.py")


def _cfg(**kw) -> LintConfig:
    return LintConfig.from_dict(kw) if kw else LintConfig.from_dict({})


def findings(rule, code: str, **kw):
    return rule.check(_a(code), _cfg(**kw))


# =============================================================================
# SPL-D06-001 — cache() without unpersist()
# =============================================================================


class TestCacheWithoutUnpersistRule:
    rule = CacheWithoutUnpersistRule()

    def test_fires_when_cache_no_unpersist(self):
        code = SPARK_HDR + "df_cached = df.join(other, 'id').cache()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-001"
        assert "unpersist" in fs[0].message

    def test_fires_when_persist_no_unpersist(self):
        code = SPARK_HDR + "df_cached = df.groupBy('id').agg(count('*')).persist()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_for_multiple_caches_no_unpersist(self):
        code = SPARK_HDR + "a = df.cache()\nb = df2.cache()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 2

    def test_no_finding_when_unpersist_present(self):
        code = SPARK_HDR + (
            "df_cached = df.cache()\n" "df_cached.count()\n" "df_cached.unpersist()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_cache(self):
        code = SPARK_HDR + "df.join(other, 'id').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-002 — cache() used only once
# =============================================================================


class TestCacheUsedOnceRule:
    rule = CacheUsedOnceRule()

    def test_fires_when_cached_used_once(self):
        code = SPARK_HDR + (
            "df_cached = df.join(other, 'id').cache()\n" "result = df_cached.count()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-002"
        assert "df_cached" in fs[0].message

    def test_fires_when_cached_never_used(self):
        code = SPARK_HDR + "df_cached = df.join(other, 'id').cache()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "0 time(s)" in fs[0].message

    def test_no_finding_when_cached_used_twice(self):
        code = SPARK_HDR + (
            "df_cached = df.join(other, 'id').cache()\n"
            "result1 = df_cached.count()\n"
            "result2 = df_cached.groupBy('x').agg(count('*'))\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_persist_used_twice(self):
        code = SPARK_HDR + (
            "df_p = df.groupBy('id').count().persist()\n"
            "df_p.join(other, 'id')\n"
            "df_p.count()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_cache_assignment(self):
        code = SPARK_HDR + "df2 = df.join(other, 'id')\ndf2.count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-003 — cache() inside loop
# =============================================================================


class TestCacheInLoopRule:
    rule = CacheInLoopRule()

    def test_fires_on_cache_in_for_loop(self):
        code = SPARK_HDR + ("for i in range(10):\n" "    df = df.filter(f'val > {i}').cache()\n")
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-003"
        assert "for" in fs[0].message

    def test_fires_on_persist_in_for_loop(self):
        code = SPARK_HDR + ("for epoch in range(100):\n" "    df = model_transform(df).persist()\n")
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "persist" in fs[0].message

    def test_fires_on_cache_in_while_loop(self):
        code = SPARK_HDR + ("while not converged:\n" "    df = df.join(updates, 'id').cache()\n")
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "while" in fs[0].message

    def test_no_finding_when_cache_outside_loop(self):
        code = SPARK_HDR + (
            "df_cached = df.cache()\n"
            "for i in range(10):\n"
            "    df_cached.filter(f'val > {i}').count()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_loop(self):
        code = SPARK_HDR + "df.cache()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-004 — cache() before filter
# =============================================================================


class TestCacheBeforeFilterRule:
    rule = CacheBeforeFilterRule()

    def test_fires_when_filter_after_cache(self):
        code = SPARK_HDR + "df.cache().filter('active = true').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-004"
        assert "filter" in fs[0].message

    def test_fires_when_where_after_cache(self):
        code = SPARK_HDR + "df.cache().where('status = \"active\"').groupBy('x').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert "where" in fs[0].message

    def test_fires_when_filter_after_persist(self):
        code = SPARK_HDR + "df.persist().filter('val > 0').count()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_filter_before_cache(self):
        code = SPARK_HDR + "df.filter('active = true').cache().count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_filter_without_cache(self):
        code = SPARK_HDR + "df.filter('active = true').count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-005 — MEMORY_ONLY storage level
# =============================================================================


class TestMemoryOnlyStorageLevelRule:
    rule = MemoryOnlyStorageLevelRule()

    def test_fires_on_memory_only_attribute(self):
        code = SPARK_HDR + (
            "from pyspark import StorageLevel\n" "df.persist(StorageLevel.MEMORY_ONLY)\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-005"
        assert "MEMORY_ONLY" in fs[0].message

    def test_fires_on_memory_only_string(self):
        code = SPARK_HDR + 'df.persist("MEMORY_ONLY")\n'
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_memory_and_disk(self):
        code = SPARK_HDR + (
            "from pyspark import StorageLevel\n" "df.persist(StorageLevel.MEMORY_AND_DISK)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_persist_without_args(self):
        # persist() with no args uses default level
        code = SPARK_HDR + "df.persist()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_cache(self):
        # cache() uses MEMORY_AND_DISK by default for DataFrames
        code = SPARK_HDR + "df.cache()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-006 — Reused DataFrame without cache
# =============================================================================


class TestReusedDataFrameWithoutCacheRule:
    rule = ReusedDataFrameWithoutCacheRule()

    def test_fires_on_join_result_used_twice(self):
        code = SPARK_HDR + (
            "df_joined = df.join(other, 'id')\n"
            "result1 = df_joined.count()\n"
            "result2 = df_joined.groupBy('x').agg(count('*'))\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-006"
        assert "df_joined" in fs[0].message
        assert "times" in fs[0].message

    def test_fires_on_groupby_result_used_twice(self):
        code = SPARK_HDR + (
            "df_agg = df.groupBy('id').agg(sum('v'))\n"
            "df_agg.join(lookup, 'id')\n"
            "df_agg.count()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_when_cached(self):
        code = SPARK_HDR + (
            "df_joined = df.join(other, 'id').cache()\n"
            "result1 = df_joined.count()\n"
            "result2 = df_joined.groupBy('x').agg(count('*'))\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_used_only_once(self):
        code = SPARK_HDR + ("df_joined = df.join(other, 'id')\n" "result = df_joined.count()\n")
        assert findings(self.rule, code) == []

    def test_no_finding_for_cheap_operation(self):
        # select/filter are cheap — no need to cache
        code = SPARK_HDR + (
            "df2 = df.select('id', 'val')\n"
            "df2.filter('val > 0').count()\n"
            "df2.filter('val < 10').count()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-007 — cache() after repartition
# =============================================================================


class TestCacheAfterRepartitionRule:
    rule = CacheAfterRepartitionRule()

    def test_fires_on_cache_after_repartition(self):
        code = SPARK_HDR + "df2 = df.repartition(200).cache()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-007"
        assert "repartition" in fs[0].message

    def test_fires_on_persist_after_repartition(self):
        code = SPARK_HDR + "df2 = df.repartition(200).persist()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_fires_on_cache_after_coalesce(self):
        code = SPARK_HDR + "df2 = df.coalesce(1).cache()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1

    def test_no_finding_for_cache_without_repartition(self):
        code = SPARK_HDR + "df2 = df.join(other, 'id').cache()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_repartition_without_cache(self):
        code = SPARK_HDR + "df2 = df.repartition(200).count()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []


# =============================================================================
# SPL-D06-008 — checkpoint vs cache misuse
# =============================================================================


class TestCheckpointVsCacheMisuseRule:
    rule = CheckpointVsCacheMisuseRule()

    def test_fires_on_checkpoint_without_loop(self):
        code = SPARK_HDR + "df_cp = df.join(other, 'id').checkpoint()\n"
        fs = findings(self.rule, code)
        assert len(fs) == 1
        assert fs[0].rule_id == "SPL-D06-008"
        assert "checkpoint" in fs[0].message

    def test_fires_on_multiple_checkpoints_without_loop(self):
        code = SPARK_HDR + (
            "df1 = df.join(other, 'id').checkpoint()\n"
            "df2 = df1.groupBy('x').count().checkpoint()\n"
        )
        fs = findings(self.rule, code)
        assert len(fs) == 2

    def test_no_finding_when_checkpoint_inside_loop(self):
        # Iterative algorithm — checkpoint() is appropriate here
        code = SPARK_HDR + (
            "for epoch in range(10):\n"
            "    prev = df\n"
            "    df = df.join(updates, 'id').checkpoint()\n"
            "    prev.unpersist()\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_when_loop_present_anywhere(self):
        # If the file contains any loop, we assume iterative context
        code = SPARK_HDR + (
            "df_cp = df.join(other, 'id').checkpoint()\n"
            "for item in items:\n"
            "    process(item)\n"
        )
        assert findings(self.rule, code) == []

    def test_no_finding_for_no_checkpoint(self):
        code = SPARK_HDR + "df.join(other, 'id').cache()\n"
        assert findings(self.rule, code) == []

    def test_no_finding_for_non_spark_file(self):
        assert findings(self.rule, "x = 1\n") == []
