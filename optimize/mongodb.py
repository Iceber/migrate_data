# coding: utf-8

import os
import glob
import json


class MongoTable(object):

    def __init__(self, meta_file, suffix=None):
        self._meta_file = meta_file
        self._suffix = suffix

        with open(meta_file) as f:
            self._meta = json.loads(f.read())

        self.schema = self._meta["schema"]
        self.permissions = self._meta["permissions"]
        self.indexes = self._meta["indexes"]

    @property
    def name(self):
        fname = os.path.basename(self._meta_file)
        return fname.split(self._suffix)[0]

    @classmethod
    def iter_tables(cls, dir, suffix="_all.json"):
        pattern = os.path.join(dir, "*" + suffix)
        for p in glob.glob(pattern):
            yield cls(p, suffix)

    def map_columns(self, f):
        return map(lambda kv: f(*kv), self.schema.items())
