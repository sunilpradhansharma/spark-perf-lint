"""AST-based analyzer for PySpark source files.

The module has three public layers:

1. **Data classes** — lightweight containers for the structured facts that
   the single-pass visitor extracts from the AST.  Rule modules consume
   these; they never touch raw ``ast`` nodes.

2. **SparkASTVisitor** — walks the full AST once and populates every
   container.  Rules call the query methods on ``ASTAnalyzer``; they never
   drive the visitor directly.

3. **ASTAnalyzer** — the main public class.  Accepts source code (string)
   or a file path, runs the visitor, and exposes typed query methods that
   rule modules call.

Design constraints:
- Zero external dependencies: stdlib ``ast``, ``dataclasses``, ``pathlib``
  only.
- Single AST traversal regardless of how many rules run.
- All query methods return *new* lists (safe for callers to mutate).
"""

from __future__ import annotations

import ast
import textwrap
from dataclasses import dataclass
from pathlib import Path

# =============================================================================
# Data classes
# =============================================================================


@dataclass
class MethodCallInfo:
    """A single method-call node extracted from the AST.

    Attributes:
        node: The raw ``ast.Call`` node (for advanced consumers).
        line: 1-based source line number.
        col: 0-based column offset.
        method_name: The attribute name that was called, e.g. ``"join"``.
        receiver_src: Best-effort string representation of the object the
            method was called on, e.g. ``"df1"`` or ``"df.filter(...)``.
        args: Positional argument nodes (``ast.expr`` subclasses).
        kwargs: Keyword argument nodes keyed by name.
        chain: Ordered list of method names in the chain leading up to
            (and including) this call, e.g. ``["read", "parquet"]``.
    """

    node: ast.Call
    line: int
    col: int
    method_name: str
    receiver_src: str
    args: list[ast.expr]
    kwargs: dict[str, ast.expr]
    chain: list[str]


@dataclass
class AttributeInfo:
    """An attribute-access expression, e.g. ``df.rdd`` or ``spark.conf``.

    Attributes:
        node: The raw ``ast.Attribute`` node.
        line: 1-based source line number.
        col: 0-based column offset.
        attr_name: The attribute being accessed, e.g. ``"rdd"``.
        receiver_src: String representation of the object.
    """

    node: ast.Attribute
    line: int
    col: int
    attr_name: str
    receiver_src: str


@dataclass
class SparkConfigInfo:
    """A single Spark configuration key/value pair detected in source.

    Covers both ``SparkSession.builder.config("k", "v")`` and
    ``spark.conf.set("k", "v")`` patterns.

    Attributes:
        key: The Spark config key string, e.g.
            ``"spark.sql.adaptive.enabled"``.
        value: The config value as a string, or ``None`` when the value
            could not be statically resolved (e.g. a variable reference).
        line: 1-based source line number of the *start* of the call
            expression.  For chained builder calls Python 3.8+ assigns the
            same ``lineno`` to every call in the chain (the chain's first
            token), so use ``end_line`` to determine source position within
            a chain.
        end_line: 1-based line number of the *end* of this specific call
            node.  Reliably distinguishes adjacent ``.config(...)`` calls
            in a builder chain.
        method: How the config was set: ``"builder_config"`` or
            ``"conf_set"``.
    """

    key: str
    value: str | None
    line: int
    end_line: int
    method: str  # "builder_config" | "conf_set"


@dataclass
class SparkIOInfo:
    """A Spark I/O operation (read or write) detected in source.

    Attributes:
        operation: ``"read"`` or ``"write"``.
        format: Format string if determinable (e.g. ``"parquet"``), else
            ``None``.
        options: Dict of ``option()`` / ``options()`` kwargs found in the
            call chain.
        path: First positional path argument if present, else ``None``.
        line: 1-based source line number.
        partitioned_by: Columns passed to ``partitionBy()``, if any.
        bucketed_by: ``(num_buckets, columns)`` from ``bucketBy()``, or
            ``None``.
        has_schema: Whether ``.schema(...)`` appears in the read chain.
        infer_schema: Whether ``inferSchema`` option is ``"true"``
            (case-insensitive).
        write_mode: Value passed to ``.mode()`` in a write chain, or
            ``None``.
    """

    operation: str  # "read" | "write"
    format: str | None
    options: dict[str, str]
    path: str | None
    line: int
    partitioned_by: list[str]
    bucketed_by: tuple[int, list[str]] | None
    has_schema: bool
    infer_schema: bool
    write_mode: str | None


@dataclass
class FuncDefInfo:
    """A function or method definition.

    Attributes:
        name: Function name.
        line: 1-based source line number of the ``def`` keyword.
        decorators: List of decorator name strings
            (e.g. ``["udf", "staticmethod"]``).
        is_udf: ``True`` when a ``@udf`` or ``@pandas_udf`` decorator is
            present.
        udf_type: ``"udf"``, ``"pandas_udf"``, or ``None``.
        node: The raw ``ast.FunctionDef`` / ``ast.AsyncFunctionDef`` node.
        complexity: McCabe cyclomatic complexity of the function body
            (populated by ``ASTAnalyzer``; ``1`` until computed).
    """

    name: str
    line: int
    decorators: list[str]
    is_udf: bool
    udf_type: str | None
    node: ast.FunctionDef | ast.AsyncFunctionDef
    complexity: int = 1


@dataclass
class LoopInfo:
    """A for-loop or while-loop with the method calls made inside it.

    Attributes:
        loop_type: ``"for"`` or ``"while"``.
        line: 1-based source line of the loop keyword.
        col: 0-based column offset.
        body_calls: Method names called anywhere inside the loop body
            (not deduplicated; preserves order of appearance).
        node: The raw ``ast.For`` / ``ast.While`` node.
    """

    loop_type: str  # "for" | "while"
    line: int
    col: int
    body_calls: list[str]
    node: ast.For | ast.While


@dataclass
class AssignmentInfo:
    """A variable assignment, used for lightweight dataflow tracing.

    Attributes:
        target: The assignment target name(s) as a string,
            e.g. ``"df2"`` or ``"result, count"``.
        line: 1-based source line number.
        col: 0-based column offset.
        value_src: String representation of the right-hand side.
        rhs_method_calls: Method names called on the RHS
            (e.g. ``["join"]`` for ``df1.join(df2, "id")``).
        node: The raw ``ast.Assign`` / ``ast.AnnAssign`` node.
    """

    target: str
    line: int
    col: int
    value_src: str
    rhs_method_calls: list[str]
    node: ast.Assign | ast.AnnAssign


# =============================================================================
# Internal helpers
# =============================================================================


def _node_to_src(node: ast.expr) -> str:
    """Best-effort conversion of an AST expression to source text.

    Uses ``ast.unparse`` (Python 3.9+) with a fallback for older nodes.
    """
    try:
        return ast.unparse(node)
    except Exception:  # noqa: BLE001
        return "<expr>"


def _literal_str(node: ast.expr) -> str | None:
    """Return the string value of a constant node, or ``None``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _method_chain(node: ast.expr) -> list[str]:
    """Walk a chained attribute-call expression and collect method names.

    Given ``a.b().c().d()`` returns ``["b", "c", "d"]`` (innermost first).
    """
    chain: list[str] = []
    current: ast.expr = node
    while True:
        if isinstance(current, ast.Call) and isinstance(current.func, ast.Attribute):
            chain.append(current.func.attr)
            current = current.func.value
        elif isinstance(current, ast.Attribute):
            chain.append(current.attr)
            current = current.value
        else:
            break
    chain.reverse()
    return chain


def _mccabe_complexity(node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    """Compute the McCabe cyclomatic complexity of a function body.

    Counts decision points: ``if``, ``elif``, ``for``, ``while``,
    ``except``, ``with``, ``assert``, boolean ``and``/``or``.
    Starts at 1 (the function itself).
    """
    complexity = 1
    for child in ast.walk(node):
        if isinstance(child, (ast.If, ast.For, ast.While, ast.ExceptHandler, ast.With)):
            complexity += 1
        elif isinstance(child, ast.BoolOp):
            # Each extra operand adds a branch: A and B and C → +2
            complexity += len(child.values) - 1
        elif isinstance(child, ast.Assert):
            complexity += 1
    return complexity


# =============================================================================
# SparkASTVisitor — single-pass collector
# =============================================================================


class SparkASTVisitor(ast.NodeVisitor):
    """Walks a parsed AST once and collects all Spark-relevant facts.

    Results are stored in public lists that ``ASTAnalyzer`` exposes through
    typed query methods.  Callers should not use this class directly.

    Attributes:
        method_calls: All method-call sites.
        attribute_accesses: All attribute accesses.
        spark_configs: Detected Spark config assignments.
        spark_io: Detected read/write operations.
        func_defs: Function/method definitions (with UDF detection).
        loops: For/while loops with their inner method calls.
        assignments: Variable assignments.
    """

    def __init__(self) -> None:
        self.method_calls: list[MethodCallInfo] = []
        self.attribute_accesses: list[AttributeInfo] = []
        self.spark_configs: list[SparkConfigInfo] = []
        self.spark_io: list[SparkIOInfo] = []
        self.func_defs: list[FuncDefInfo] = []
        self.loops: list[LoopInfo] = []
        self.assignments: list[AssignmentInfo] = []

        # Internal state
        self._inside_loop_stack: list[list[str]] = []  # stack of body_calls lists

    # ------------------------------------------------------------------
    # Visitor callbacks
    # ------------------------------------------------------------------

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        """Record every function/method call."""
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            receiver = node.func.value
            receiver_src = _node_to_src(receiver)
            chain = _method_chain(node)

            kwargs: dict[str, ast.expr] = {kw.arg: kw.value for kw in node.keywords if kw.arg}

            info = MethodCallInfo(
                node=node,
                line=node.lineno,
                col=node.col_offset,
                method_name=method_name,
                receiver_src=receiver_src,
                args=list(node.args),
                kwargs=kwargs,
                chain=chain,
            )
            self.method_calls.append(info)

            # Propagate into enclosing loops
            for body_calls in self._inside_loop_stack:
                body_calls.append(method_name)

            # Specialised extractors
            self._maybe_extract_spark_config(node, method_name, chain)
            self._maybe_extract_spark_io(node, chain)

        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:  # noqa: N802
        """Record every attribute access."""
        self.attribute_accesses.append(
            AttributeInfo(
                node=node,
                line=node.lineno,
                col=node.col_offset,
                attr_name=node.attr,
                receiver_src=_node_to_src(node.value),
            )
        )
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._record_func_def(node)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._record_func_def(node)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:  # noqa: N802
        self._visit_loop("for", node)

    def visit_While(self, node: ast.While) -> None:  # noqa: N802
        self._visit_loop("while", node)

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        self._record_assignment(node)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802
        if node.value is not None:
            self._record_ann_assignment(node)
        self.generic_visit(node)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _record_func_def(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        decorator_names: list[str] = []
        for dec in node.decorator_list:
            if isinstance(dec, ast.Name):
                decorator_names.append(dec.id)
            elif isinstance(dec, ast.Attribute):
                decorator_names.append(dec.attr)
            elif isinstance(dec, ast.Call):
                if isinstance(dec.func, ast.Name):
                    decorator_names.append(dec.func.id)
                elif isinstance(dec.func, ast.Attribute):
                    decorator_names.append(dec.func.attr)

        is_udf = any(d in {"udf", "pandas_udf"} for d in decorator_names)
        udf_type: str | None = None
        if "pandas_udf" in decorator_names:
            udf_type = "pandas_udf"
        elif "udf" in decorator_names:
            udf_type = "udf"

        self.func_defs.append(
            FuncDefInfo(
                name=node.name,
                line=node.lineno,
                decorators=decorator_names,
                is_udf=is_udf,
                udf_type=udf_type,
                node=node,
                complexity=_mccabe_complexity(node),
            )
        )

    def _visit_loop(self, loop_type: str, node: ast.For | ast.While) -> None:
        body_calls: list[str] = []
        self._inside_loop_stack.append(body_calls)
        self.generic_visit(node)
        self._inside_loop_stack.pop()
        self.loops.append(
            LoopInfo(
                loop_type=loop_type,
                line=node.lineno,
                col=node.col_offset,
                body_calls=body_calls,
                node=node,
            )
        )

    def _record_assignment(self, node: ast.Assign) -> None:
        targets_src = ", ".join(_node_to_src(t) for t in node.targets)
        rhs_calls: list[str] = []
        for child in ast.walk(node.value):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                rhs_calls.append(child.func.attr)
        self.assignments.append(
            AssignmentInfo(
                target=targets_src,
                line=node.lineno,
                col=node.col_offset,
                value_src=_node_to_src(node.value),
                rhs_method_calls=rhs_calls,
                node=node,
            )
        )

    def _record_ann_assignment(self, node: ast.AnnAssign) -> None:
        assert node.value is not None  # guarded by caller
        rhs_calls: list[str] = []
        for child in ast.walk(node.value):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                rhs_calls.append(child.func.attr)
        self.assignments.append(
            AssignmentInfo(
                target=_node_to_src(node.target),
                line=node.lineno,
                col=node.col_offset,
                value_src=_node_to_src(node.value),
                rhs_method_calls=rhs_calls,
                node=node,
            )
        )

    # ------------------------------------------------------------------
    # Spark config extractor
    # ------------------------------------------------------------------

    # Patterns recognised:
    #   SparkSession.builder.config("k", "v")   → builder_config
    #   spark.conf.set("k", "v")                → conf_set
    #   spark.conf.set("k", v)                  → conf_set (value=None)
    _CONF_SET_RECEIVERS = {"conf"}

    def _maybe_extract_spark_config(
        self,
        node: ast.Call,
        method_name: str,
        chain: list[str],
    ) -> None:
        # .config("key", "value") anywhere in a builder chain
        if method_name == "config" and len(node.args) >= 2:
            key = _literal_str(node.args[0])
            if key is not None:
                value = _literal_str(node.args[1])
                self.spark_configs.append(
                    SparkConfigInfo(
                        key=key,
                        value=value,
                        line=node.lineno,
                        end_line=getattr(node, "end_lineno", node.lineno),
                        method="builder_config",
                    )
                )
            return

        # spark.conf.set("key", "value")
        if method_name == "set" and isinstance(node.func, ast.Attribute):
            receiver = node.func.value
            if (
                isinstance(receiver, ast.Attribute)
                and receiver.attr in self._CONF_SET_RECEIVERS
                and len(node.args) >= 2
            ):
                key = _literal_str(node.args[0])
                if key is not None:
                    value = _literal_str(node.args[1]) if len(node.args) >= 2 else None
                    self.spark_configs.append(
                        SparkConfigInfo(
                            key=key,
                            value=value,
                            line=node.lineno,
                            end_line=getattr(node, "end_lineno", node.lineno),
                            method="conf_set",
                        )
                    )

    # ------------------------------------------------------------------
    # Spark I/O extractor
    # ------------------------------------------------------------------

    # Format method names that appear directly in the chain
    _READ_FORMATS = {"csv", "json", "parquet", "orc", "jdbc", "text", "avro"}
    _WRITE_FORMATS = {"csv", "json", "parquet", "orc", "jdbc", "text", "avro", "delta"}
    # Terminal methods that signal the end of a read/write chain.
    # Only these trigger IO extraction — intermediate helpers like .option(),
    # .mode(), .partitionBy() share the same chain but must not produce
    # separate SparkIOInfo entries.
    _READ_TERMINALS = _READ_FORMATS | {"load"}
    _WRITE_TERMINALS = _WRITE_FORMATS | {"save", "saveAsTable", "insertInto"}

    def _maybe_extract_spark_io(self, node: ast.Call, chain: list[str]) -> None:  # noqa: C901
        chain_set = set(chain)

        is_read = "read" in chain_set or "readStream" in chain_set
        is_write = "write" in chain_set or "writeStream" in chain_set

        if not (is_read or is_write):
            return

        # Only process terminal call in the chain — the method that actually
        # triggers execution (e.g. .parquet(), .save()).  Intermediate helpers
        # (.option(), .mode(), .partitionBy()) share the chain prefix but must
        # not generate their own SparkIOInfo entry.
        method_name_local = chain[-1] if chain else ""
        if is_read and method_name_local not in self._READ_TERMINALS:
            return
        if is_write and method_name_local not in self._WRITE_TERMINALS:
            return

        # Determine operation and format
        operation = "read" if is_read else "write"
        fmt: str | None = None

        if is_read and method_name_local in self._READ_FORMATS:
            fmt = method_name_local
        elif is_write and method_name_local in self._WRITE_FORMATS:
            fmt = method_name_local

        # Walk the full chain of the call to pick up .format(), .option(),
        # .options(), .schema(), .mode(), .partitionBy(), .bucketBy()
        options: dict[str, str] = {}
        partitioned_by: list[str] = []
        bucketed_by: tuple[int, list[str]] | None = None
        has_schema = False
        infer_schema = False
        write_mode: str | None = None
        path: str | None = None

        # Extract path from the terminal positional argument if present
        if node.args:
            path = _literal_str(node.args[0])

        # Walk the entire chained expression to collect all sub-calls
        current: ast.expr = node
        while isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Attribute):
                m = func.attr
                if m == "format" and current.args:
                    maybe_fmt = _literal_str(current.args[0])
                    if maybe_fmt:
                        fmt = maybe_fmt
                elif m == "option" and len(current.args) >= 2:
                    k = _literal_str(current.args[0])
                    v = _literal_str(current.args[1])
                    if k is not None:
                        options[k] = v or ""
                        if k.lower() == "inferschema" and (v or "").lower() == "true":
                            infer_schema = True
                elif m == "options":
                    for kw in current.keywords:
                        if kw.arg:
                            v = _literal_str(kw.value)
                            options[kw.arg] = v or ""
                            if kw.arg.lower() == "inferschema" and (v or "").lower() == "true":
                                infer_schema = True
                elif m == "schema":
                    has_schema = True
                elif m == "mode" and current.args:
                    write_mode = _literal_str(current.args[0])
                elif m == "partitionBy":
                    partitioned_by = [
                        s for arg in current.args if (s := _literal_str(arg)) is not None
                    ]
                elif m == "bucketBy" and current.args:
                    num_raw = current.args[0]
                    num: int | None = None
                    if isinstance(num_raw, ast.Constant) and isinstance(num_raw.value, int):
                        num = num_raw.value
                    bucket_cols = [
                        s for arg in current.args[1:] if (s := _literal_str(arg)) is not None
                    ]
                    if num is not None:
                        bucketed_by = (num, bucket_cols)
                # Recurse into the receiver
                current = func.value
            else:
                break

        self.spark_io.append(
            SparkIOInfo(
                operation=operation,
                format=fmt,
                options=options,
                path=path,
                line=node.lineno,
                partitioned_by=partitioned_by,
                bucketed_by=bucketed_by,
                has_schema=has_schema,
                infer_schema=infer_schema,
                write_mode=write_mode,
            )
        )


# =============================================================================
# ASTAnalyzer — public façade
# =============================================================================


class ParseError(ValueError):
    """Raised when a source file cannot be parsed as valid Python.

    Attributes:
        filename: The file that failed to parse.
        lineno: The line number reported by the parser, or ``None``.
        msg: The underlying syntax error message.
    """

    def __init__(self, filename: str, lineno: int | None, msg: str) -> None:
        self.filename = filename
        self.lineno = lineno
        self.msg = msg
        location = f" (line {lineno})" if lineno else ""
        super().__init__(f"Syntax error in {filename!r}{location}: {msg}")


class ASTAnalyzer:
    """Parses a Python source file and exposes Spark-aware query methods.

    All expensive work (parsing + AST traversal) happens once in
    ``__init__``.  Query methods are pure reads over the pre-built result
    sets; they never re-traverse the tree.

    Example::

        analyzer = ASTAnalyzer.from_file("jobs/etl.py")
        for call in analyzer.find_method_calls("join"):
            print(call.line, call.receiver_src)

    Attributes:
        filename: Source path (for error messages and Finding.file_path).
        source: Raw source text.
        tree: Parsed ``ast.Module``.
        source_lines: Source split into individual lines (for
            ``get_code_context``).
    """

    def __init__(self, source: str, filename: str = "<string>") -> None:
        """Parse *source* and run the single-pass visitor.

        Args:
            source: Python source code to analyse.
            filename: Label used in error messages and ``Finding.file_path``.

        Raises:
            ParseError: If *source* is not valid Python syntax.
        """
        self.filename = filename
        self.source = source
        self.source_lines = source.splitlines()

        try:
            self.tree = ast.parse(source, filename=filename, type_comments=False)
        except SyntaxError as exc:
            raise ParseError(filename, exc.lineno, exc.msg) from exc

        self._visitor = SparkASTVisitor()
        self._visitor.visit(self.tree)

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_file(cls, path: str | Path) -> ASTAnalyzer:
        """Read *path* from disk and return an ``ASTAnalyzer``.

        Args:
            path: Path to a ``.py`` file.

        Returns:
            A fully initialised ``ASTAnalyzer``.

        Raises:
            OSError: If the file cannot be read.
            ParseError: If the file is not valid Python.
        """
        p = Path(path)
        source = p.read_text(encoding="utf-8", errors="replace")
        return cls(source, filename=str(p))

    @classmethod
    def from_source(cls, source: str, filename: str = "<string>") -> ASTAnalyzer:
        """Construct directly from a source string (alias for ``__init__``).

        Useful in tests where code is generated programmatically.

        Args:
            source: Python source code.
            filename: Label for error messages.

        Returns:
            A fully initialised ``ASTAnalyzer``.
        """
        return cls(source, filename=filename)

    # ------------------------------------------------------------------
    # Query methods
    # ------------------------------------------------------------------

    def find_method_calls(self, method_name: str) -> list[MethodCallInfo]:
        """Return all calls to *method_name* anywhere in the file.

        Matches the attribute name only (not the receiver type), so
        ``find_method_calls("join")`` returns DataFrame joins, string
        ``join`` calls, and any other ``.join(...)`` in the file.
        Rule modules are responsible for disambiguation.

        Args:
            method_name: The method name to search for (case-sensitive).

        Returns:
            A new list of ``MethodCallInfo`` sorted by line number.
        """
        return sorted(
            [c for c in self._visitor.method_calls if c.method_name == method_name],
            key=lambda c: (c.line, c.col),
        )

    def find_attribute_accesses(self, attr_name: str) -> list[AttributeInfo]:
        """Return all attribute accesses for *attr_name*.

        Args:
            attr_name: Attribute name to search for (case-sensitive).

        Returns:
            A new list of ``AttributeInfo`` sorted by line number.
        """
        return sorted(
            [a for a in self._visitor.attribute_accesses if a.attr_name == attr_name],
            key=lambda a: (a.line, a.col),
        )

    def find_spark_session_configs(self) -> list[SparkConfigInfo]:
        """Return all detected Spark config assignments.

        Covers both ``SparkSession.builder.config("k", "v")`` and
        ``spark.conf.set("k", "v")`` call patterns.

        Returns:
            A new list of ``SparkConfigInfo`` sorted by line number.
        """
        # Sort by end_line so that adjacent .config() calls within a builder
        # chain appear in source order.  Python 3.8+ assigns the same lineno
        # to every call in a chain (the chain's first token); end_line is the
        # reliable discriminator.
        return sorted(self._visitor.spark_configs, key=lambda c: (c.end_line, c.line))

    def find_spark_read_writes(self) -> list[SparkIOInfo]:
        """Return all detected Spark read/write operations.

        Returns:
            A new list of ``SparkIOInfo`` sorted by line number.
        """
        return sorted(self._visitor.spark_io, key=lambda io: io.line)

    def find_function_definitions(self) -> list[FuncDefInfo]:
        """Return all function/method definitions, including UDFs.

        Returns:
            A new list of ``FuncDefInfo`` sorted by line number.
        """
        return sorted(self._visitor.func_defs, key=lambda f: f.line)

    def find_loop_patterns(self) -> list[LoopInfo]:
        """Return all for/while loops with the methods called inside them.

        Returns:
            A new list of ``LoopInfo`` sorted by line number.
        """
        return sorted(self._visitor.loops, key=lambda lo: lo.line)

    def find_variable_assignments(self) -> list[AssignmentInfo]:
        """Return all variable assignments for lightweight dataflow tracing.

        Returns:
            A new list of ``AssignmentInfo`` sorted by line number.
        """
        return sorted(self._visitor.assignments, key=lambda a: a.line)

    def get_code_context(self, line: int, context_lines: int = 3) -> str:
        """Return the source lines surrounding *line*.

        The focus line is marked with ``>>`` in the output; surrounding
        lines are indented with spaces.  Useful for populating
        ``Finding.before_code``.

        Args:
            line: 1-based target line number.
            context_lines: Number of lines of context *above and below*
                the focus line.

        Returns:
            A dedented multi-line string with the context window.
        """
        if not self.source_lines:
            return ""

        total = len(self.source_lines)
        start = max(0, line - 1 - context_lines)
        end = min(total, line - 1 + context_lines + 1)

        parts: list[str] = []
        for i in range(start, end):
            lineno = i + 1
            prefix = ">>" if lineno == line else "  "
            parts.append(f"{prefix} {lineno:4d} | {self.source_lines[i]}")

        return textwrap.dedent("\n".join(parts))

    # ------------------------------------------------------------------
    # Convenience / aggregate queries
    # ------------------------------------------------------------------

    def has_spark_imports(self) -> bool:
        """Return ``True`` if the file imports anything from ``pyspark``.

        Uses a fast scan of ``Import`` / ``ImportFrom`` nodes rather than
        relying on the visitor's collected method calls.

        Returns:
            ``True`` when at least one ``pyspark`` import is present.
        """
        for node in ast.walk(self.tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("pyspark"):
                    return True
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith("pyspark"):
                        return True
        return False

    def find_all_method_calls(self) -> list[MethodCallInfo]:
        """Return every method call in the file, sorted by line.

        Returns:
            All ``MethodCallInfo`` instances sorted by ``(line, col)``.
        """
        return sorted(self._visitor.method_calls, key=lambda c: (c.line, c.col))

    def find_method_calls_in_loops(self, method_name: str) -> list[tuple[LoopInfo, int]]:
        """Return loops that call *method_name* inside their body.

        Args:
            method_name: The method name to look for inside loops.

        Returns:
            List of ``(loop, count)`` tuples where ``count`` is how many
            times *method_name* appears inside that loop.
        """
        result: list[tuple[LoopInfo, int]] = []
        for loop in self._visitor.loops:
            count = loop.body_calls.count(method_name)
            if count:
                result.append((loop, count))
        return result

    def find_udf_definitions(self) -> list[FuncDefInfo]:
        """Return only UDF function definitions.

        Returns:
            Functions decorated with ``@udf`` or ``@pandas_udf``.
        """
        return [f for f in self._visitor.func_defs if f.is_udf]

    def get_config_value(self, key: str) -> str | None:
        """Return the last statically-resolvable value set for *key*.

        When a key appears multiple times (overriding), the last occurrence
        wins — matching Spark's own semantics for builder chaining.

        Args:
            key: Spark config key, e.g. ``"spark.sql.adaptive.enabled"``.

        Returns:
            The value string, or ``None`` if the key was not set or its
            value could not be statically resolved.
        """
        result: str | None = None
        for cfg in self._visitor.spark_configs:
            if cfg.key == key:
                result = cfg.value
        return result

    def find_reads(self) -> list[SparkIOInfo]:
        """Return only read operations.

        Returns:
            ``SparkIOInfo`` instances where ``operation == "read"``.
        """
        return [io for io in self._visitor.spark_io if io.operation == "read"]

    def find_writes(self) -> list[SparkIOInfo]:
        """Return only write operations.

        Returns:
            ``SparkIOInfo`` instances where ``operation == "write"``.
        """
        return [io for io in self._visitor.spark_io if io.operation == "write"]

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"ASTAnalyzer(filename={self.filename!r}, "
            f"lines={len(self.source_lines)}, "
            f"method_calls={len(self._visitor.method_calls)}, "
            f"configs={len(self._visitor.spark_configs)})"
        )
