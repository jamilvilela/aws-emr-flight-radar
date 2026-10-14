from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
import json
import boto3
from botocore.exceptions import ClientError


@dataclass
class ColumnSpec:
    name: str
    type: str
    nullable: bool = True
    comment: Optional[str] = None


@dataclass
class PartitionSpec:
    name: str
    type: str
    source_column: Optional[str] = None


@dataclass
class SurrogateKeySpec:
    name: str
    col: str


@dataclass
class TableConfig:
    target_table: str
    source_tables: List[str]
    primary_key: List[str]
    surrogate_key: Optional[SurrogateKeySpec] = None
    merge_fallback_keys: Optional[List[str]] = None
    partition_keys: List[PartitionSpec] = field(default_factory=list)
    transform_type: str = "spark_sql"
    transform_query: Optional[str] = None
    transform_module: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    order: int = 0
    enable_zorder: bool = False
    zorder_cols: List[str] = field(default_factory=list)
    enum_columns: Dict[str, List[str]] = field(default_factory=dict)

    def __post_init__(self):
        if self.surrogate_key and isinstance(self.surrogate_key, dict):
            self.surrogate_key = SurrogateKeySpec(**self.surrogate_key)
        self.partition_keys = [
            PartitionSpec(**pk) if isinstance(pk, dict) else pk
            for pk in self.partition_keys
        ]

    def validate(self) -> List[str]:
        errors = []
        if not self.target_table:
            errors.append("target_table is required")
        if not self.primary_key:
            errors.append("primary_key is required")
        if self.transform_type not in ("spark_sql", "pyspark"):
            errors.append(f"transform_type must be 'spark_sql' or 'pyspark', got '{self.transform_type}'")
        if self.transform_type == "spark_sql" and not self.transform_query:
            errors.append("transform_query required for spark_sql transform_type")
        if self.transform_type == "pyspark" and not self.transform_module:
            errors.append("transform_module required for pyspark transform_type")
        if self.enable_zorder and not self.zorder_cols:
            errors.append("zorder_cols required when enable_zorder is true")
        return errors


@dataclass
class TablesConfig:
    _tables: List[TableConfig] = field(default_factory=list, repr=False)

    @property
    def tables(self) -> List[TableConfig]:
        return sorted(self._tables, key=lambda t: t.order)

    def get_table(self, name: str) -> Optional[TableConfig]:
        for table in self._tables:
            if table.target_table == name:
                return table
        return None

    def get_table_names(self) -> List[str]:
        return [t.target_table for t in self.tables]

    @classmethod
    def from_s3(cls, s3_path: str) -> "TablesConfig":
        if not s3_path.startswith("s3://"):
            raise ValueError(f"Expected s3:// path, got: {s3_path}")

        bucket = s3_path.split("/")[2]
        key = "/".join(s3_path.split("/")[3:])

        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            data = json.loads(content)
        except ClientError as e:
            raise RuntimeError(f"Failed to load config from {s3_path}: {e}")

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: List[Dict[str, Any]]) -> "TablesConfig":
        tables = []
        seen_names = set()
        seen_orders = set()

        for idx, item in enumerate(data):
            table = TableConfig(**item)
            errors = table.validate()
            if errors:
                raise ValueError(f"Validation failed for table '{table.target_table}': {errors}")

            if table.target_table in seen_names:
                raise ValueError(f"Duplicate target_table: {table.target_table}")
            seen_names.add(table.target_table)

            if table.order in seen_orders:
                raise ValueError(f"Duplicate order: {table.order}")
            seen_orders.add(table.order)

            tables.append(table)

        config = cls(_tables=tables)

        table_names = config.get_table_names()
        for table in tables:
            for dep in table.dependencies:
                if dep not in table_names:
                    raise ValueError(f"Table '{table.target_table}' depends on unknown table '{dep}'")

        return config

    def resolve_tables(self, tables_arg: str) -> List[TableConfig]:
        if tables_arg == "all":
            return self.tables

        requested = [t.strip() for t in tables_arg.split(",")]
        result = []
        visited = set()

        def add_table(name: str):
            if name in visited:
                return
            table = self.get_table(name)
            if not table:
                raise ValueError(f"Unknown table: {name}")
            for dep in table.dependencies:
                add_table(dep)
            visited.add(name)
            result.append(table)

        for name in requested:
            add_table(name)

        result.sort(key=lambda t: t.order)
        return result

    def to_dict(self) -> List[Dict[str, Any]]:
        result = []
        for table in self._tables:
            d = {
                "target_table": table.target_table,
                "source_tables": table.source_tables,
                "primary_key": table.primary_key,
                "partition_keys": [
                    {"name": pk.name, "type": pk.type, "source_column": pk.source_column}
                    for pk in table.partition_keys
                ],
                "transform_type": table.transform_type,
                "dependencies": table.dependencies,
                "order": table.order,
                "enable_zorder": table.enable_zorder,
                "zorder_cols": table.zorder_cols,
                "enum_columns": table.enum_columns,
            }
            if table.surrogate_key:
                d["surrogate_key"] = {"name": table.surrogate_key.name, "col": table.surrogate_key.col}
            if table.merge_fallback_keys:
                d["merge_fallback_keys"] = table.merge_fallback_keys
            if table.transform_query:
                d["transform_query"] = table.transform_query
            if table.transform_module:
                d["transform_module"] = table.transform_module
            result.append(d)
        return result