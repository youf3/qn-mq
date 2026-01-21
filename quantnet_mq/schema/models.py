import os
import sys
import json
from referencing import Registry, Resource
import yaml
import python_jsonschema_objects as pjs
import importlib
import pathlib
import quantnet_mq

default_ns = sys.modules[__name__]
module_path = os.path.dirname(quantnet_mq.__file__)

object_dir = os.path.join(module_path, "schema/objects")
core_dir = os.path.join(module_path, "schema/rpc/core")
qnrpc_server_dir = os.path.join(module_path, "schema/rpc/qn-server")
qnrpc_agent_dir = os.path.join(module_path, "schema/rpc/qn-agent")
schema_dirs = [
    os.path.join(module_path, "schema/rpc"),
    os.path.join(module_path, "schema/messages")
]


class Schema:
    _SCHEMA = {}
    _SCHEMA_CACHE = {}
    _REGISTRY = Registry()
    _SCHEMA_DIR = os.path.join(module_path, "schema")
    _URI_PREFIX = "qn-schema:"
    _BASE_URI = "uri:quant-net:mq"
    _SCHEMA_DRAFT = "http://json-schema.org/draft-04/schema#"
    _cpath = None

    def __str__(self):
        ret = f"{'NAME':<20}{'NAMESPACE':<20}SCHEMA\n"
        ret += f"{''.ljust(60,'-')}\n"
        for s, v in self._SCHEMA.items():
            name = s
            ns = v.get("ns", "default")
            path = v.get("path")
            ret += f"{name:<20}{ns:<20}{path}\n"
        return ret.strip()

    @staticmethod
    def get_entry(name):
        return Schema._SCHEMA.get(name)

    @staticmethod
    def set_entry(name, path, ns=None, classes=None, json=None):
        entry = {
            "path": path,
            "classes": classes if classes else [],
            "json": json
        }
        if ns:
            entry["ns"] = ns
        Schema._SCHEMA[name] = entry

    @staticmethod
    def _get_file_json(f):
        with open(f, "r") as file:
            str = file.read()
            while len(str) == 0:
                file.seek(0)
                str = file.read()
            data = json.loads(str)
        return data

    @staticmethod
    def _get_file_yaml(f):
        path = pathlib.Path(f)
        contents = yaml.safe_load(path.read_text())
        return contents

    @staticmethod
    def _add_schema_id(sdata: dict, name: str):
        if sdata.get("$schema") is None:
            sdata["$schema"] = Schema._SCHEMA_DRAFT
        if sdata.get("id") is None:
            sdata["id"] = f"{Schema._BASE_URI}:{name}"

    @staticmethod
    def _get_resource_yaml(uri: str):
        schema = Schema._SCHEMA_CACHE.get(uri)
        if schema:
            return Resource.from_contents(schema)
        if uri.startswith(Schema._URI_PREFIX):
            path = Schema._SCHEMA_DIR / pathlib.Path(uri.removeprefix(Schema._URI_PREFIX))
        else:
            path = Schema._cpath / pathlib.Path(uri)
        contents = yaml.safe_load(path.read_text())
        Schema._add_schema_id(contents, uri)
        Schema._SCHEMA_CACHE[uri] = contents
        return Resource.from_contents(contents)

    @staticmethod
    def _convert_yaml(f):
        with open(f, "r") as file:
            data = yaml.safe_load(file)
        with open(f.with_suffix('.json'), "w") as jfile:
            dat = json.dumps(data).replace('yaml', 'json')
            dat = dat.replace('objects.json', os.path.join(object_dir, "objects.json"))
            jfile.write(dat)
        return data

    @staticmethod
    def _load_schema(name: str, fpath: pathlib.PosixPath, namespace: str = None, classes: list = []):
        # We define "default" as a reserved namespace
        if namespace and namespace != "default":
            # Create an empty ns and attach to the default ns
            spec = importlib.machinery.ModuleSpec(namespace, None)
            module = importlib.util.module_from_spec(spec)
            setattr(default_ns, namespace, module)
            module = getattr(default_ns, namespace)
        else:
            module = default_ns

        Schema._cpath = fpath.parent.absolute()
        sdata = Schema._get_file_yaml(fpath)
        for k, v in sdata["components"]["schemas"].items():
            Schema._add_schema_id(v, k)
            builder = pjs.ObjectBuilder(v, resolver=Schema._get_resource_yaml)
            builder.basedir = "/"
            ns = builder.build_classes(named_only=True, standardize_names=False)
            for cls in dir(ns):
                setattr(module, cls, ns[cls])

        # Update Schema entry as needed
        if not Schema.get_entry(name):
            Schema.set_entry(name, str(fpath), namespace, classes, sdata)

    @staticmethod
    def load_schema(fname: str, ns: str = None, classes: list = []):
        if isinstance(fname, str):
            fname = [fname]
        for f in fname:
            p = pathlib.Path(f)
            if p.is_dir():
                for fp in pathlib.Path(p).glob('*.yaml'):
                    namespace = fp.stem if not ns else ns
                    Schema._load_schema(fp.stem, fp, namespace)
            else:
                namespace = p.stem if not ns else ns
                Schema._load_schema(p.stem, p, namespace)


Schema.load_schema(object_dir, ns="default")
Schema.load_schema(core_dir, ns="default")
Schema.load_schema(qnrpc_server_dir, ns="default")
Schema.load_schema(qnrpc_agent_dir, ns="default")
Schema.load_schema(schema_dirs)
