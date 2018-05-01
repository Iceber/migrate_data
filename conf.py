import re
import yaml

yaml_path = ""

with open(yaml_path) as f:
    config = yaml.load(f.read())

first_cap_re = re.compile("(.)([A-Z][a-z]+)")
all_cap_re = re.compile("([a-z0-9])([A-Z])")


def _snake_case(name):
    s1 = first_cap_re.sub(r"\1_\2", name)
    return all_cap_re.sub(r"\1_\2", s1).lower()


common = config["common"]
customize = config["customize"]
