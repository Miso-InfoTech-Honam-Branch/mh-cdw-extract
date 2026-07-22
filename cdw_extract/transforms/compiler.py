from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from ..duck import quote_ident
from .schema import ColumnSchema, common_type, derived_column, normalize_type, numeric_result_type, relabel, schema_hash


MAX_STEPS = 50
MAX_OUTPUT_COLUMNS = 500
MAX_PIVOT_VALUES = 100
MAX_PIVOT_COLUMNS = 200


@dataclass(frozen=True)
class CompiledPipeline:
    sql: str
    parameters: list[Any]
    output_schema: list[ColumnSchema]
    step_schemas: list[dict]
    warnings: list[dict]
    pipeline_hash: str
    resolved_pipeline: dict[str, Any]

    def json(self, include_sql: bool = False, include_resolved_pipeline: bool = False) -> dict:
        value = {
            "valid": True,
            "pipelineHash": self.pipeline_hash,
            "sourceSchemaHash": schema_hash(self.step_schemas[0]["_schema"] if self.step_schemas else self.output_schema),
            "outputSchema": [item.json() for item in self.output_schema],
            "stepResults": [{key: value for key, value in step.items() if key != "_schema"} for step in self.step_schemas],
            "warnings": self.warnings,
        }
        if include_resolved_pipeline:
            value["resolvedPipeline"] = copy.deepcopy(self.resolved_pipeline)
        if include_sql:
            value["sql"] = self.sql
        return value


def canonical_hash(value: object) -> str:
    canonical = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def inspect_source_schema(description: list[tuple], declared: list[dict] | None = None) -> list[ColumnSchema]:
    declared_by_name = {str(item.get("physicalName") or item.get("name")): item for item in declared or []}
    columns = []
    for index, item in enumerate(description or []):
        name, duck_type = str(item[0]), str(item[1])
        metadata = declared_by_name.get(name, {})
        column_id = str(metadata.get("columnId") or f"source:{index}:{name}")
        columns.append(ColumnSchema(
            column_id=column_id,
            physical_name=name,
            label=str(metadata.get("label") or name),
        # DuckDB describes the current physical source. Declared metadata supplies identity and labels,
        # but must never override the worker's actual type inference.
        data_type=normalize_type(duck_type),
            nullable=bool(metadata.get("nullable", True)),
            source_column_ids=(column_id,),
        ))
    return columns


class PipelineCompiler:
    def __init__(self, source_sql: str, source_schema: list[ColumnSchema], pipeline: dict):
        self.source_sql = source_sql
        self.schema = list(source_schema)
        # Compile an isolated snapshot. This same snapshot is exposed as the
        # resolved validation contract and is the sole input to pipelineHash.
        self.pipeline = copy.deepcopy(pipeline or {})
        self.parameters: list[Any] = []
        self.warnings: list[dict] = []
        self.ctes = [f"{quote_ident('__source')} AS ({source_sql})"]
        self.relation = quote_ident("__source")
        self.step_schemas: list[dict] = [{"stepId": "SOURCE", "status": "VALID", "outputSchema": [item.json() for item in self.schema], "warnings": [], "_schema": list(self.schema)}]

    def column(self, column_id: str) -> ColumnSchema:
        for item in self.schema:
            if item.column_id == column_id:
                return item
        raise ValueError(f"PIPELINE_COLUMN_NOT_FOUND: {column_id}")

    def expression(self, column_id: str) -> str:
        return quote_ident(self.column(column_id).physical_name)

    def output(self, step_id: str, select: list[str], schema: list[ColumnSchema], warning_start: int) -> None:
        if len(schema) > MAX_OUTPUT_COLUMNS:
            raise ValueError(f"PIPELINE_SCHEMA_TOO_WIDE: maximum is {MAX_OUTPUT_COLUMNS}")
        relation_name = f"__step_{len(self.step_schemas):03d}"
        self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT {', '.join(select)} FROM {self.relation})")
        self.relation = quote_ident(relation_name)
        self.schema = schema
        warnings = self.warnings[warning_start:]
        self.step_schemas.append({"stepId": step_id, "status": "VALID", "outputSchema": [item.json() for item in schema], "warnings": warnings, "_schema": list(schema)})

    def passthrough(self) -> list[str]:
        return [quote_ident(item.physical_name) for item in self.schema]

    def compile(self) -> CompiledPipeline:
        steps = self.pipeline.get("steps") or []
        if len(steps) > MAX_STEPS:
            raise ValueError(f"PIPELINE_STEP_LIMIT_EXCEEDED: maximum is {MAX_STEPS}")
        ids: set[str] = set()
        active = [step for step in steps if step.get("enabled", True)]
        for index, step in enumerate(active):
            step_id = str(step.get("stepId") or "").strip()
            if not step_id or step_id in ids:
                raise ValueError("PIPELINE_DUPLICATE_STEP_ID")
            ids.add(step_id)
            step_type = str(step.get("type") or "").upper()
            if step_type == "OUTPUT" and index != len(active) - 1:
                raise ValueError("OUTPUT must be the last active step")
            getattr(self, f"step_{step_type.lower()}", self.unsupported)(step_id, step.get("config") or {})
        if not active or str(active[-1].get("type") or "").upper() != "OUTPUT":
            raise ValueError("the final active step must be OUTPUT")
        sql = f"WITH {', '.join(self.ctes)} SELECT * FROM {self.relation}"
        resolved_pipeline = copy.deepcopy(self.pipeline)
        return CompiledPipeline(
            sql,
            self.parameters,
            self.schema,
            self.step_schemas,
            self.warnings,
            canonical_hash(resolved_pipeline),
            resolved_pipeline,
        )

    def unsupported(self, step_id: str, config: dict) -> None:
        raise ValueError(f"unsupported pipeline step: {step_id}")

    def step_select_columns(self, step_id: str, config: dict) -> None:
        warning_start = len(self.warnings)
        outputs, schema = [], []
        for item in config.get("columns") or []:
            column = relabel(self.column(item.get("columnId")), item.get("label"))
            outputs.append(f"{self.expression(column.column_id)} AS {quote_ident(column.physical_name)}")
            schema.append(column)
        if not schema:
            raise ValueError("SELECT_COLUMNS requires columns")
        self.output(step_id, outputs, schema, warning_start)

    def _condition(self, item: dict) -> str:
        column = self.column(item.get("columnId"))
        expr = quote_ident(column.physical_name)
        parameter_type = column.data_type.replace("STRING", "VARCHAR").replace("INT64", "BIGINT").replace("TIMESTAMP_TZ", "TIMESTAMPTZ").replace("BINARY", "BLOB")
        placeholder = f"CAST(? AS {parameter_type})"
        op = str(item.get("operator") or "").upper()
        values = item.get("values") or []
        negative = op in {"NE", "NOT_IN", "NOT_CONTAINS", "NOT_STARTS_WITH", "NOT_ENDS_WITH"}
        if op == "IS_NULL": return f"{expr} IS NULL"
        if op == "IS_NOT_NULL": return f"{expr} IS NOT NULL"
        if op in {"IN", "NOT_IN"}:
            if not values: raise ValueError(f"{op} requires values")
            self.parameters.extend(values)
            clause = f"{expr} {'NOT IN' if op == 'NOT_IN' else 'IN'} ({', '.join(placeholder for _ in values)})"
        elif op == "BETWEEN":
            if len(values) != 2: raise ValueError("BETWEEN requires two values")
            self.parameters.extend(values); clause = f"{expr} BETWEEN {placeholder} AND {placeholder}"
        elif op in {"CONTAINS", "NOT_CONTAINS", "STARTS_WITH", "NOT_STARTS_WITH", "ENDS_WITH", "NOT_ENDS_WITH"}:
            if len(values) != 1: raise ValueError(f"{op} requires one value")
            value = str(values[0]).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            if "CONTAINS" in op: value = f"%{value}%"
            elif "STARTS" in op: value = f"{value}%"
            else: value = f"%{value}"
            self.parameters.append(value)
            clause = f"CAST({expr} AS VARCHAR) {'NOT LIKE' if negative else 'LIKE'} ? ESCAPE '\\'"
        else:
            if len(values) != 1: raise ValueError(f"{op} requires one value")
            symbols = {"EQ": "=", "NE": "<>", "GT": ">", "GTE": ">=", "LT": "<", "LTE": "<="}
            if op not in symbols: raise ValueError(f"unsupported filter operator: {op}")
            self.parameters.append(values[0]); clause = f"{expr} {symbols[op]} {placeholder}"
        return f"({clause} OR {expr} IS NULL)" if negative else clause

    def step_filter(self, step_id: str, config: dict) -> None:
        conditions = config.get("conditions") or []
        if not conditions: raise ValueError("FILTER requires conditions")
        default_logic = "OR" if str(config.get("logic") or "AND").upper() == "OR" else "AND"
        predicate = self._condition(conditions[0])
        for condition in conditions[1:]:
            condition_logic = "OR" if str(condition.get("logic") or default_logic).upper() == "OR" else "AND"
            predicate = f"({predicate} {condition_logic} {self._condition(condition)})"
        warning_start = len(self.warnings)
        relation_name = f"__step_{len(self.step_schemas):03d}"
        self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT * FROM {self.relation} WHERE {predicate})")
        self.relation = quote_ident(relation_name)
        self.step_schemas.append({"stepId": step_id, "status": "VALID", "outputSchema": [item.json() for item in self.schema], "warnings": self.warnings[warning_start:], "_schema": list(self.schema)})

    def step_cast(self, step_id: str, config: dict) -> None:
        source = self.column(config.get("columnId")); target = normalize_type(config.get("targetType")); output_id = config.get("outputId") or "cast"
        duck_type = target.replace("STRING", "VARCHAR").replace("INT64", "BIGINT").replace("TIMESTAMP_TZ", "TIMESTAMPTZ").replace("BINARY", "BLOB")
        policy = str(config.get("onError") or "FAIL").upper()
        input_format = str(config.get("inputFormat") or "").upper()
        date_formats = {
            "YYYYMMDD": "%Y%m%d",
            "YYMMDD": "%y%m%d",
            "YYYY-MM-DD": "%Y-%m-%d",
            "YYYY/MM/DD": "%Y/%m/%d",
            "YYYYMMDDHH24MISS": "%Y%m%d%H%M%S",
            "YYYY-MM-DD HH24:MI:SS": "%Y-%m-%d %H:%M:%S",
            "ISO8601_TZ": "%Y-%m-%dT%H:%M:%S%z",
        }
        if input_format and target in {"DATE", "TIMESTAMP", "TIMESTAMP_TZ"}:
            if input_format not in date_formats:
                raise ValueError(f"unsupported CAST inputFormat: {input_format}")
            parser = "strptime" if policy == "FAIL" else "try_strptime"
            parsed = f"{parser}(CAST({quote_ident(source.physical_name)} AS VARCHAR), '{date_formats[input_format]}')"
            cast = f"CAST({parsed} AS {duck_type})"
        else:
            cast = f"CAST({quote_ident(source.physical_name)} AS {duck_type})" if policy == "FAIL" else f"TRY_CAST({quote_ident(source.physical_name)} AS {duck_type})"
        output = derived_column(step_id, output_id, config.get("label") or source.label, target, [source], "CAST", nullable=source.nullable or policy != "FAIL")
        select = self.passthrough() if config.get("keepInput", False) else [quote_ident(item.physical_name) for item in self.schema if item.column_id != source.column_id]
        schema = list(self.schema) if config.get("keepInput", False) else [item for item in self.schema if item.column_id != source.column_id]
        select.append(f"{cast} AS {quote_ident(output.physical_name)}"); schema.append(output)
        warning_start = len(self.warnings)
        if policy == "DROP_ROW":
            relation_name = f"__cast_source_{len(self.step_schemas):03d}"
            self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT * FROM {self.relation} WHERE {quote_ident(source.physical_name)} IS NULL OR {cast} IS NOT NULL)")
            self.relation = quote_ident(relation_name)
            self.warnings.append({"code": "CAST_DROPS_INVALID_ROWS", "stepId": step_id})
        self.output(step_id, select, schema, warning_start)

    def step_fill_null(self, step_id: str, config: dict) -> None:
        source = self.column(config.get("columnId")); self.parameters.append(config.get("value"))
        output = derived_column(step_id, config.get("outputId") or "filled", config.get("label") or source.label, source.data_type, [source], "FILL_NULL", nullable=False)
        self.output(step_id, self.passthrough() + [f"COALESCE({quote_ident(source.physical_name)}, ?) AS {quote_ident(output.physical_name)}"], self.schema + [output], len(self.warnings))

    def step_trim(self, step_id: str, config: dict) -> None:
        source = self.column(config.get("columnId")); mode = str(config.get("mode") or "BOTH").upper(); fn = {"LEFT":"LTRIM","RIGHT":"RTRIM","BOTH":"TRIM"}.get(mode)
        if source.data_type != "STRING" or not fn: raise ValueError("TRIM requires a STRING column and valid mode")
        output = derived_column(step_id, config.get("outputId") or "trimmed", config.get("label") or source.label, "STRING", [source], "TRIM", nullable=source.nullable)
        self.output(step_id, self.passthrough()+[f"{fn}({quote_ident(source.physical_name)}) AS {quote_ident(output.physical_name)}"], self.schema+[output], len(self.warnings))

    def step_change_case(self, step_id: str, config: dict) -> None:
        source=self.column(config.get("columnId")); mode=str(config.get("mode") or "UPPER").upper()
        if source.data_type != "STRING" or mode not in {"UPPER","LOWER"}: raise ValueError("CHANGE_CASE requires STRING and UPPER/LOWER")
        output=derived_column(step_id, config.get("outputId") or "case", config.get("label") or source.label, "STRING", [source], "CHANGE_CASE", nullable=source.nullable)
        self.output(step_id,self.passthrough()+[f"{mode}({quote_ident(source.physical_name)}) AS {quote_ident(output.physical_name)}"],self.schema+[output],len(self.warnings))

    def step_merge_columns(self, step_id: str, config: dict) -> None:
        sources=[self.column(item) for item in config.get("inputColumnIds") or []]
        if len(sources)<2: raise ValueError("MERGE_COLUMNS requires at least two columns")
        delimiter=str(config.get("delimiter") or ""); policy=str(config.get("nullPolicy") or "SKIP").upper(); self.parameters.append(delimiter)
        casts=[f"CAST({quote_ident(item.physical_name)} AS VARCHAR)" for item in sources]
        if policy=="EMPTY": casts=[f"COALESCE({item}, '')" for item in casts]
        if policy=="SKIP": expression=f"array_to_string(list_filter([{', '.join(casts)}], x -> x IS NOT NULL), ?)"
        elif policy=="NULL": expression=f"CASE WHEN {' OR '.join(quote_ident(item.physical_name)+' IS NULL' for item in sources)} THEN NULL ELSE array_to_string([{', '.join(casts)}], ?) END"
        elif policy=="EMPTY": expression=f"array_to_string([{', '.join(casts)}], ?)"
        else: raise ValueError("invalid MERGE_COLUMNS nullPolicy")
        output=derived_column(step_id,config.get("output",{}).get("outputId") or "merged",config.get("output",{}).get("label") or "합친 값","STRING",sources,"MERGE_COLUMNS")
        keep=config.get("keepInputs",True); schema=list(self.schema) if keep else [item for item in self.schema if item not in sources]; select=self.passthrough() if keep else [quote_ident(item.physical_name) for item in schema]
        self.output(step_id,select+[f"{expression} AS {quote_ident(output.physical_name)}"],schema+[output],len(self.warnings))

    def step_split_column(self, step_id: str, config: dict) -> None:
        source=self.column(config.get("inputColumnId")); outputs=config.get("outputs") or []
        mode=str(config.get("mode") or "DELIMITER").upper()
        minimum_outputs=1 if mode=="SLICE" else 2
        if source.data_type=="BINARY": raise ValueError("SPLIT_COLUMN does not support BINARY input columns")
        if len(outputs)<minimum_outputs or len(outputs)>20: raise ValueError(f"SPLIT_COLUMN {mode} requires {minimum_outputs}-20 output definitions")
        source_expression=quote_ident(source.physical_name) if source.data_type=="STRING" else f"CAST({quote_ident(source.physical_name)} AS VARCHAR)"
        selects=self.passthrough(); schema=list(self.schema)
        if mode=="DELIMITER":
            for index,item in enumerate(outputs,1):
                self.parameters.append(str(config.get("delimiter") or ""))
                output=derived_column(step_id,item.get("outputId") or f"part-{index}",item.get("label") or f"나눈 값 {index}","STRING",[source],"SPLIT_COLUMN")
                selects.append(f"NULLIF(split_part({source_expression}, ?, {index}), '') AS {quote_ident(output.physical_name)}"); schema.append(output)
        elif mode=="POSITION":
            positions=[int(value) for value in config.get("positions") or []]
            if len(positions)!=len(outputs)-1 or positions!=sorted(set(positions)): raise ValueError("SPLIT_COLUMN positions are invalid")
            starts=[1]+[value+1 for value in positions]; ends=positions+[None]
            for index,item in enumerate(outputs):
                output=derived_column(step_id,item.get("outputId") or f"part-{index+1}",item.get("label") or f"나눈 값 {index+1}","STRING",[source],"SPLIT_COLUMN")
                expr=f"substr({source_expression}, {starts[index]})" if ends[index] is None else f"substr({source_expression}, {starts[index]}, {ends[index]-starts[index]+1})"
                selects.append(f"NULLIF({expr}, '') AS {quote_ident(output.physical_name)}"); schema.append(output)
        elif mode=="FIXED_LENGTH":
            lengths=[int(value) for value in config.get("lengths") or []]
            if len(lengths)!=len(outputs) or any(value<1 for value in lengths): raise ValueError("SPLIT_COLUMN fixed lengths must match outputs and be positive")
            start=1
            for index,(item,length) in enumerate(zip(outputs,lengths),1):
                output=derived_column(step_id,item.get("outputId") or f"part-{index}",item.get("label") or f"나눈 값 {index}","STRING",[source],"SPLIT_COLUMN")
                expr=f"substr({source_expression}, {start}, {length})"
                selects.append(f"NULLIF({expr}, '') AS {quote_ident(output.physical_name)}"); schema.append(output); start+=length
        elif mode=="SLICE":
            start=int(config.get("startAt") or 0); length=int(config.get("length") or 0)
            if start<1 or length<1 or len(outputs)!=1: raise ValueError("SPLIT_COLUMN slice requires one output, positive startAt and length")
            item=outputs[0]
            output=derived_column(step_id,item.get("outputId") or "slice",item.get("label") or "추출한 값","STRING",[source],"SPLIT_COLUMN")
            expr=f"substr({source_expression}, {start}, {length})"
            selects.append(f"NULLIF({expr}, '') AS {quote_ident(output.physical_name)}"); schema.append(output)
        else: raise ValueError("unsupported SPLIT_COLUMN mode")
        self.output(step_id,selects,schema,len(self.warnings))

    def step_replace_value(self, step_id: str, config: dict) -> None:
        source=self.column(config.get("columnId")); mappings=config.get("mappings") or []
        if not mappings: raise ValueError("REPLACE_VALUE requires mappings")
        match_mode=str(config.get("matchMode") or "EXACT").upper()
        if match_mode not in {"EXACT", "CONTAINS"}: raise ValueError("unsupported REPLACE_VALUE matchMode")
        parts=[]
        for item in mappings:
            source_value=item.get("from")
            if match_mode == "CONTAINS":
                escaped=str(source_value or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                self.parameters.extend([f"%{escaped}%",item.get("to")])
                parts.append(f"WHEN CAST({quote_ident(source.physical_name)} AS VARCHAR) LIKE ? ESCAPE '\\' THEN ?")
            else:
                self.parameters.extend([source_value,item.get("to")]); parts.append(f"WHEN {quote_ident(source.physical_name)} = ? THEN ?")
        unmatched=str(config.get("unmatchedPolicy") or "KEEP").upper()
        otherwise="NULL" if unmatched=="NULL" else quote_ident(source.physical_name)
        output=derived_column(step_id,config.get("outputId") or "replaced",config.get("label") or source.label,source.data_type,[source],"REPLACE_VALUE")
        self.output(step_id,self.passthrough()+[f"CASE {' '.join(parts)} ELSE {otherwise} END AS {quote_ident(output.physical_name)}"],self.schema+[output],len(self.warnings))

    def _calc(self, node: dict, depth: int = 0) -> tuple[str, list[ColumnSchema], str]:
        if depth>8: raise ValueError("calculation depth exceeds 8")
        op=str(node.get("op") or "").upper()
        if op=="COLUMN":
            column=self.column(node.get("columnId")); return quote_ident(column.physical_name),[column],column.data_type
        if op=="LITERAL":
            value=node.get("value"); self.parameters.append(value)
            return "?",[],normalize_type(node.get("dataType") or ("NULL" if value is None else "STRING"))
        args=[self._calc(item,depth+1) for item in node.get("args") or []]; sql=[item[0] for item in args]; sources=[col for item in args for col in item[1]]
        if op in {"ADD","SUBTRACT","MULTIPLY","DIVIDE"} and len(sql)==2:
            symbol={"ADD":"+","SUBTRACT":"-","MULTIPLY":"*","DIVIDE":"/"}[op]
            typ, warning = numeric_result_type(args[0][2], args[1][2], op)
            if warning: self.warnings.append({"code":warning,"message":"정수부를 보존하기 위해 소수 자릿수를 줄였습니다."})
            return f"({sql[0]} {symbol} {sql[1]})",sources,typ
        if op=="COALESCE" and sql: return f"COALESCE({', '.join(sql)})",sources,common_type([item[2] for item in args])
        if op=="CONCAT" and sql: return f"concat({', '.join('CAST('+item+' AS VARCHAR)' for item in sql)})",sources,"STRING"
        raise ValueError(f"unsupported calculation op: {op}")

    def step_calculate(self, step_id: str, config: dict) -> None:
        expression,sources,inferred=self._calc(config.get("expression") or {}); target=normalize_type(config.get("targetType") or inferred)
        output=derived_column(step_id,config.get("outputId") or "calculated",config.get("label") or "계산 결과",target,sources,"CALCULATE")
        self.output(step_id,self.passthrough()+[f"{expression} AS {quote_ident(output.physical_name)}"],self.schema+[output],len(self.warnings))

    def step_code_lookup(self, step_id: str, config: dict) -> None:
        source=self.column(config.get("columnId")); mappings=config.get("values") or []
        if not mappings: raise ValueError("CODE_LOOKUP requires resolved values")
        seen=set(); parts=[]
        for item in mappings:
            code=str(item.get("code"))
            if code in seen: raise ValueError("CODE_LOOKUP_DUPLICATE_KEY")
            seen.add(code); self.parameters.extend([code,item.get("name")]); parts.append(f"WHEN CAST({quote_ident(source.physical_name)} AS VARCHAR) = ? THEN ?")
        output=derived_column(step_id,config.get("outputId") or "code-name",config.get("label") or f"{source.label} 이름","STRING",[source],"CODE_LOOKUP")
        self.output(step_id,self.passthrough()+[f"CASE {' '.join(parts)} ELSE NULL END AS {quote_ident(output.physical_name)}"],self.schema+[output],len(self.warnings))

    def step_row_number(self, step_id: str, config: dict) -> None:
        order=config.get("orderBy") or []
        if not order: raise ValueError("ROW_NUMBER_ORDER_REQUIRED")
        partition=", ".join(self.expression(item) for item in config.get("partitionBy") or [])
        orders=", ".join(f"{self.expression(item.get('columnId'))} {'DESC' if str(item.get('direction')).upper()=='DESC' else 'ASC'} NULLS {'FIRST' if str(item.get('nulls')).upper()=='FIRST' else 'LAST'}" for item in order)
        over=(f"PARTITION BY {partition} " if partition else "")+f"ORDER BY {orders}"
        out_cfg=config.get("output") or {}; output=derived_column(step_id,out_cfg.get("outputId") or "row-number",out_cfg.get("label") or "행 번호","INT64",[self.column(item.get("columnId")) for item in order],"ROW_NUMBER",nullable=False)
        start=max(1,int(config.get("startAt") or 1)); expr=f"row_number() OVER ({over})"+(f" + {start-1}" if start>1 else "")
        self.output(step_id,self.passthrough()+[f"{expr} AS {quote_ident(output.physical_name)}"],self.schema+[output],len(self.warnings))

    def _aggregate(self, item: dict, step_id: str) -> tuple[str, ColumnSchema]:
        op=str(item.get("op") or "").upper(); source=self.column(item.get("columnId")) if item.get("columnId") else None
        if op=="COUNT_ROWS": expr="count(*)"; typ="INT64"; sources=[]
        elif op in {"COUNT","COUNT_DISTINCT"} and source:
            expr=f"count({'DISTINCT ' if op=='COUNT_DISTINCT' else ''}{quote_ident(source.physical_name)})"; typ="INT64"; sources=[source]
        elif op in {"SUM","AVG","MIN","MAX","MEDIAN"} and source:
            if op in {"SUM","AVG","MEDIAN"} and not (source.data_type=="INT64" or source.data_type.startswith("DECIMAL")): raise ValueError(f"{op} requires numeric column")
            expr=f"{op.lower()}({quote_ident(source.physical_name)})"; typ=source.data_type
            if op=="SUM" and source.data_type.startswith("DECIMAL"): typ=f"DECIMAL(38,{source.data_type.split(',')[1].rstrip(')')})"
            if op=="AVG": typ="DECIMAL(38,6)"
            sources=[source]
        else: raise ValueError(f"unsupported aggregate: {op}")
        output=derived_column(step_id,item.get("aggregateId") or op.lower(),item.get("label") or op,typ,sources,op,nullable=op not in {"COUNT","COUNT_ROWS","COUNT_DISTINCT"})
        return f"{expr} AS {quote_ident(output.physical_name)}",output

    def step_group_aggregate(self, step_id: str, config: dict) -> None:
        selects=[]; schema=[]; groups=config.get("groups") or []
        for item in groups:
            source=self.column(item.get("columnId")); output=derived_column(step_id,item.get("outputId") or source.column_id,item.get("label") or source.label,source.data_type,[source],"GROUP",nullable=source.nullable)
            selects.append(f"{quote_ident(source.physical_name)} AS {quote_ident(output.physical_name)}"); schema.append(output)
        for item in config.get("aggregates") or []:
            expression,output=self._aggregate(item,step_id); selects.append(expression); schema.append(output)
        if not selects: raise ValueError("GROUP_AGGREGATE requires groups or aggregates")
        group_sql=f" GROUP BY {', '.join(str(i) for i in range(1,len(groups)+1))}" if groups else ""
        relation_name=f"__step_{len(self.step_schemas):03d}"; self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT {', '.join(selects)} FROM {self.relation}{group_sql})"); self.relation=quote_ident(relation_name); self.schema=schema
        self.step_schemas.append({"stepId":step_id,"status":"VALID","outputSchema":[item.json() for item in schema],"warnings":[],"_schema":list(schema)})

    def step_unpivot(self, step_id: str, config: dict) -> None:
        ids=[self.column(item) for item in config.get("idColumnIds") or []]; values=[(self.column(item.get("columnId")),item) for item in config.get("valueColumns") or []]
        if not values: raise ValueError("UNPIVOT requires valueColumns")
        target=common_type([item[0].data_type for item in values],(config.get("valueOutput") or {}).get("targetType")); name_cfg=config.get("nameOutput") or {}; value_cfg=config.get("valueOutput") or {}
        name_col=derived_column(step_id,name_cfg.get("outputId") or "name",name_cfg.get("label") or "항목","STRING",[item[0] for item in values],"UNPIVOT",nullable=False)
        value_col=derived_column(step_id,value_cfg.get("outputId") or "value",value_cfg.get("label") or "값",target,[item[0] for item in values],"UNPIVOT")
        branches=[]
        for source,item in values:
            self.parameters.append(item.get("labelValue") or source.label)
            id_sql=", ".join(quote_ident(col.physical_name) for col in ids); prefix=(id_sql+", ") if id_sql else ""
            branches.append(f"SELECT {prefix}? AS {quote_ident(name_col.physical_name)}, CAST({quote_ident(source.physical_name)} AS {target.replace('STRING','VARCHAR').replace('INT64','BIGINT')}) AS {quote_ident(value_col.physical_name)} FROM {self.relation}"+(" WHERE "+quote_ident(source.physical_name)+" IS NOT NULL" if not config.get("includeNulls",False) else ""))
        relation_name=f"__step_{len(self.step_schemas):03d}"; self.ctes.append(f"{quote_ident(relation_name)} AS ({' UNION ALL '.join(branches)})"); self.relation=quote_ident(relation_name); self.schema=ids+[name_col,value_col]
        self.step_schemas.append({"stepId":step_id,"status":"VALID","outputSchema":[item.json() for item in self.schema],"warnings":[],"_schema":list(self.schema)})

    def step_pivot(self, step_id: str, config: dict) -> None:
        groups=[self.column(item) for item in config.get("groupColumnIds") or []]; pivot=self.column(config.get("pivotColumnId")); values=config.get("values") or []; aggs=config.get("aggregates") or []
        if not values or len(values)>MAX_PIVOT_VALUES: raise ValueError("PIVOT_VALUES_REQUIRED")
        unknown_policy=str(config.get("unknownValuePolicy") or "IGNORE").upper()
        if unknown_policy not in {"IGNORE","FAIL","OTHER"}: raise ValueError("PIVOT_UNKNOWN_VALUE_POLICY")
        if unknown_policy=="OTHER" and not any(str(item.get("valueId"))=="__other__" for item in values): raise ValueError("PIVOT_OTHER_VALUE_REQUIRED")
        if len(values)*len(aggs)>MAX_PIVOT_COLUMNS: raise ValueError("PIPELINE_SCHEMA_TOO_WIDE")
        selects=[quote_ident(item.physical_name) for item in groups]; schema=list(groups)
        for value in values:
            for agg in aggs:
                op=str(agg.get("op") or "").upper(); source=self.column(agg.get("columnId")) if agg.get("columnId") else None
                if str(value.get("valueId"))=="__other__":
                    known=[item.get("value") for item in values if str(item.get("valueId"))!="__other__"]
                    self.parameters.extend(known)
                    condition=f"{quote_ident(pivot.physical_name)} NOT IN ({', '.join('?' for _ in known)})" if known else "TRUE"
                else:
                    self.parameters.append(value.get("value")); condition=f"{quote_ident(pivot.physical_name)} = ?"
                input_expr=quote_ident(source.physical_name) if source else "1"
                if op in {"SUM","AVG","MEDIAN"} and source and not (source.data_type=="INT64" or source.data_type.startswith("DECIMAL")):
                    raise ValueError(f"PIVOT_NUMERIC_AGGREGATE_REQUIRED: {op} 계산은 숫자 항목에서만 사용할 수 있습니다.")
                if op in {"COUNT","COUNT_ROWS"}: expr=f"count(CASE WHEN {condition} THEN {input_expr} END)"; typ="INT64"; nullable=False
                elif op=="COUNT_DISTINCT": expr=f"count(DISTINCT CASE WHEN {condition} THEN {input_expr} END)"; typ="INT64"; nullable=False
                elif op in {"FIRST","LAST"} and source: expr=f"{op.lower()}({input_expr}) FILTER (WHERE {condition})"; typ=source.data_type; nullable=True
                elif op in {"SUM","AVG","MIN","MAX","MEDIAN"} and source: expr=f"{op.lower()}(CASE WHEN {condition} THEN {input_expr} END)"; typ=source.data_type; nullable=True
                else: raise ValueError(f"unsupported pivot aggregate: {op}")
                output_id=f"{value.get('valueId')}:{agg.get('aggregateId')}"
                output_label=str(value.get("label") or value.get("value"))
                if len(aggs)>1 and agg.get("label"):
                    output_label=f"{output_label} {agg.get('label')}"
                output=derived_column(step_id,output_id,output_label,typ,[pivot]+([source] if source else []),"PIVOT",pivot=True,nullable=nullable)
                selects.append(f"{expr} AS {quote_ident(output.physical_name)}"); schema.append(output)
        group_sql=f" GROUP BY {', '.join(str(i) for i in range(1,len(groups)+1))}" if groups else ""; relation_name=f"__step_{len(self.step_schemas):03d}"
        self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT {', '.join(selects)} FROM {self.relation}{group_sql})"); self.relation=quote_ident(relation_name); self.schema=schema
        self.step_schemas.append({"stepId":step_id,"status":"VALID","outputSchema":[item.json() for item in schema],"warnings":[],"_schema":list(schema)})

    def step_sort(self, step_id: str, config: dict) -> None:
        items=config.get("items") or []
        if not items: raise ValueError("SORT requires items")
        clause=", ".join(f"{self.expression(item.get('columnId'))} {'DESC' if str(item.get('direction')).upper()=='DESC' else 'ASC'} NULLS {'FIRST' if str(item.get('nulls')).upper()=='FIRST' else 'LAST'}" for item in items)
        relation_name=f"__step_{len(self.step_schemas):03d}"; self.ctes.append(f"{quote_ident(relation_name)} AS (SELECT * FROM {self.relation} ORDER BY {clause})"); self.relation=quote_ident(relation_name)
        self.step_schemas.append({"stepId":step_id,"status":"VALID","outputSchema":[item.json() for item in self.schema],"warnings":[],"_schema":list(self.schema)})

    def step_deduplicate(self, step_id: str, config: dict) -> None:
        keys=config.get("keyColumnIds") or []
        if not keys:
            sql=f"SELECT DISTINCT * FROM {self.relation}"
        else:
            order=config.get("orderBy") or []
            partition=", ".join(self.expression(item) for item in keys)
            ordering=", ".join(f"{self.expression(item.get('columnId'))} {'DESC' if str(item.get('direction')).upper()=='DESC' else 'ASC'}" for item in order)
            order_clause=f" ORDER BY {ordering}" if ordering else ""
            sql=f"SELECT * EXCLUDE (__dedup_rn) FROM (SELECT *, row_number() OVER (PARTITION BY {partition}{order_clause}) __dedup_rn FROM {self.relation}) WHERE __dedup_rn=1"
        relation_name=f"__step_{len(self.step_schemas):03d}"; self.ctes.append(f"{quote_ident(relation_name)} AS ({sql})"); self.relation=quote_ident(relation_name)
        self.step_schemas.append({"stepId":step_id,"status":"VALID","outputSchema":[item.json() for item in self.schema],"warnings":[],"_schema":list(self.schema)})

    def step_output(self, step_id: str, config: dict) -> None:
        columns=config.get("columns") or [{"columnId":item.column_id} for item in self.schema]
        self.step_select_columns(step_id,{"columns":columns})


def compile_pipeline(source_sql: str, source_schema: list[ColumnSchema], pipeline: dict) -> CompiledPipeline:
    return PipelineCompiler(source_sql, source_schema, pipeline).compile()


def validate_pipeline(source_sql: str, source_schema: list[ColumnSchema], pipeline: dict) -> dict:
    return compile_pipeline(source_sql, source_schema, pipeline).json(
        include_sql=False,
        include_resolved_pipeline=True,
    )
