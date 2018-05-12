# coding: utf-8

import re
import copy

# from monsql.libs.jinja2 import j2_render


class SQLTable(object):

    def __init__(self, name, columns, pk, **extra):
        self.name = name
        self.columns = columns
        self.extra_definitions = extra
        self.pk = pk

    @classmethod
    def from_mongodb(cls, t, config):
        trans = Transformer(
            common=config["common"],
            customize=config.get("customize", {}).get(t.name, {}),
        )
        return cls(**trans(t))

    @classmethod
    def from_meta_file(cls, p, config, suffix="_all.json"):

        from monsql.models.mongodb import MongoTable

        t = MongoTable(p, suffix)
        return SQLTable.from_mongodb(t, config)

    def gen_schema(self):
        return j2_render("schema.j2", table=self)


class Transformer(object):
    first_cap_re = re.compile("(.)([A-Z][a-z]+)")
    all_cap_re = re.compile("([a-z0-9])([A-Z])")

    def snake_case(self, name):
        s1 = self.first_cap_re.sub(r"\1_\2", name)
        return self.all_cap_re.sub(r"\1_\2", s1).lower()

    @staticmethod
    def merge_config(config, customize):
        """ref: https://gist.github.com/angstwad/bf22d1822c38a92ec0a9"""
        for k, v in customize.items():
            if k in config and isinstance(config[k], dict):
                Transformer.merge_config(config[k], customize[k])
            else:
                config[k] = customize[k]

    def __init__(self, common, customize):
        self.config = copy.deepcopy(common)
        self.merge_config(self.config, customize)

    def __call__(self, table):
        name = self.config.get("table_name", self.snake_case(table.name) + "s")
        columns = self.get_columns(table)
        pk = self.get_pk(table)
        return {"name": name, "columns": columns, "pk": pk}

    def get_pk(self, table):
        pk = self.config["pk"]
        common_args = self.config.get("common_column_args", {}).get("default",[])
        pk["extra_args"] = common_args + pk.get("extra", [])
        return pk

    def get_columns(self, table):

        _config = self.config.get("columns", {})
        common_column_args = self.config.get("common_column_args", [])

        def _get_default_name(ci):
            name = self.snake_case(ci["name"])
            return name + "_oid" if ci["type"] == "Pointer" else name

        def _get_default_dtype(ci):

            dtype = self.config.get("type_mapping", {}).get(ci["type"])
            dtype = self.config.get("type_mapping_by_name", {}).get(ci["name"]) or dtype

            if dtype is None:
                raise Exception(
                    "Unspecific sql type of %s.%s(%s)"
                    % (ci["table_name"], ci["name"], ci["type"])
                )

            return dtype

        def _get_default_args(ci):
            args = self.config.get("common_column_args", {}).get("default", [])
            return self.config.get("common_column_args", {}).get(ci["type"], args)

        def _trans_single_col(col_name, col_info):

            if col_name in self.config["ignore_columns"].get("name", []):
                return

            if col_info["type"] in self.config["ignore_columns"].get("type", []):
                return

            __config = _config.get(col_name, {})
            col_info["name"] = col_name
            col_info["table_name"] = table.name
            return {
                "origin": col_name,
                "raw": col_info,
                "name": __config.get("name") or _get_default_name(col_info),
                "dtype": __config.get("dtype") or _get_default_dtype(col_info),
                "extra_args": __config.get(
                    "rewrite_extra",
                    _get_default_args(col_info) + __config.get("extra", []),
                ),
            }

        return list(filter(None, table.map_columns(_trans_single_col)))
