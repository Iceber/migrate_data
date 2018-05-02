import re
import yaml

yaml_path = ""

with open(yaml_path) as f:
    config = yaml.load(f.read())
common = config["common"]
customize = config["customize"]

first_cap_re = re.compile("(.)([A-Z][a-z]+)")
all_cap_re = re.compile("([a-z0-9])([A-Z])")


def _snake_case(name):
    s1 = first_cap_re.sub(r"\1_\2", name)
    return all_cap_re.sub(r"\1_\2", s1).lower()


def translate_table_name(file_name):
    table_name = customize.get(file_name, {}).get(
        "table_name", _snake_case(file_name) + "s"
    )
    return table_name