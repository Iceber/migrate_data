class TableInfo(object):
    def __init__(self,table_name,ignore_columns,columns):
        self.table_name = table_name
        self.ignore_columns = ignore_columns
        self.columns = columns
    
    @classmethod
    def from_mongodb(cls, t, config):
        trans = Transformer(
            common=config["common"],
            customize=config.get("customize",{}).get(t.name, {}),
        )
        return cls(**trans(t))



class Transformer(object):
    first_cap_re = re.compile("(.)([A-Z][a-z]+)")
    all_cap_re = re.compile("([a-z0-9])([A-Z])")
    default_value_by_type = {
        "int" = 0
        "float" = 0.0
        "varchar" = ""

    }
    def _snake_case(self, name):
        s1 = self.first_cap_re.sub(r"\1_\2", name)
        return self.all_cap_re.sub(r"\1_\2", s1).lower()

    @staticmethod
    def merge_config(config,customize):
        for k in customize.keys():
            if k in config and instance(config[k],dict):
                Columns_info.merge_config(config[k],customize)
            else:
                config[k] = customize[k]

    def __init__(self, common, customize):
        self.config = copy.deepcopy(common)
        self.megre_config(common, customize)

    def __call__(self,table):
        name = self.config.get("table_name",self._snake_case(table.name))
        self.ignore_columns = self.config["ignore_columns"].get("name",[])
        columns = self.get_columns(table)
        return {"table_name":name,"ignore_columns":self.ignore_columns,"columns":columns}

    def get_columns(self, table):
        _config = self.config.get("columns",{})

        def _get_default_name(ci):
            name = self._snake_case(ci["name"])
            return name + "_oid" if ci["type"] == "Pointer" else name
        def _get_defaut_dtype(ci):
            dtype = self.config.get("type_mapping",{}).get(ci["type"])
            dtype = self.config.get("type_mapping_by_name",{}).get(ci["name"]) or dtype
            return dtype

        def _get_default_value(ci,ctype):
            value = ci.get("default") 
            if value:
                return value if not isinstance(value,dict) else value.get("iso") or value.get("objectId")
            else:
                return self.default_value_by_type.get(ctype)

        def _get_nullable(ci,ctype):
            return ctype in self.config.get("rewrite_extra_by_type",{}).get(col_info["type"])

        def _trans_single_col(col_name, col_info):

            if col_name in self.ingore_columns:
                return
            if col_info["type"] in self.config["ignore_columns"].get("type",[]):
                self.ignore_columns.append(col_name)
                return
            
            __config = _config.get(col_name, {})
            ctype = __config.get("dtype") or _get_defaut_dtype(col_info)
            
            col = {
                "name":__config.get("name") or _get_default_name(col_info)
                "type": ctype
                "default": _get_default_value(col_info,dtype)
                "nullable": __config.get("rewrite_extra") or _get_nullable(col_info,ctype)
            }
            return (col_name, col)
        return dict(filter(None,table.map_columns(_trans_single_col)))



