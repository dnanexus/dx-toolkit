
from collections import defaultdict
import copy

from .utils import merge
from .compat import basestring
from .exceptions import (err_exit, DXError, DXCLIError)

'''
System Requirements
+++++++++++++++++++

A module containing utility methods useful for packing and unpacking
system requirements.
'''

class SystemRequirementsDict(object):
    """
    A class representing system requirements that can be passed as
    "systemRequirements" to the class-xxxx/run API call (after converting
    it to a dictionary with as_dict()).
    """

    def __init__(self, entrypoints):
        """
        Example of the entrypoints input:
        {"main":
            {"instanceType": "mem2_hdd2_x2"},
         "other_function":
            {"instanceType": "mem2_hdd2_x1",
             "clusterSpec": {"type": "spark",
                             "version": "2.4.0",
                             "initialInstanceCount": 2}}}
        """
        if entrypoints is not None and not isinstance(entrypoints, dict):
            raise DXError("Expected entrypoints to be a dict or None")
        self.entrypoints = copy.deepcopy(entrypoints)

    @classmethod
    def from_instance_count(cls, instance_count_arg, entrypoint="*"):
        """
        Returns a SystemRequirementsDict that can be passed as a
        "systemRequirements" input to job/new or run/ API calls.
        The instance_count_arg should be either a:
        * string or int eg. "6" or 8
        * dictionary, eg. {"main": 4, "other_function": 2}
        """
        try:
            if instance_count_arg is None:
                return cls(None)
            if isinstance(instance_count_arg, basestring) or isinstance(instance_count_arg, int):
                return cls({entrypoint: {"clusterSpec": {"initialInstanceCount": int(instance_count_arg)}}})
            if isinstance(instance_count_arg, dict):
                return cls({k: {"clusterSpec": {"initialInstanceCount": int(v)}} for k, v in instance_count_arg.items()})
            raise ValueError
        except ValueError:
            DXError('Expected instance_count field to be either an int, string or a dict')

    @classmethod
    def from_instance_type(cls, instance_type_arg, entrypoint="*"):
        """
        Returns SystemRequirementsDict that can be passed as a
        "systemRequirements" input to job/new or run/ API calls.
        The instance_type_arg should be either a:
        * string, eg. mem1_ssd1_x2
        * dictionary, eg. {"main": "mem2_hdd2_x2", "other_function": "mem2_hdd2_x1"}
        """
        if instance_type_arg is None:
            return cls(None)
        if isinstance(instance_type_arg, basestring):
            # By default, all entry points ("*") should use this instance type
            return cls({entrypoint: {"instanceType": instance_type_arg}})
        if isinstance(instance_type_arg, dict):
            # instance_type is a map of entry point to instance type
            return cls({fn: {"instanceType": fn_inst} for fn, fn_inst in instance_type_arg.items()})
        raise DXError('Expected instance_type field to be either a string or a dict')

    @classmethod
    def from_sys_requirements(cls, system_requirements, _type='all'):
        """
        Returns SystemRequirementsDict encapsulating system requirements.
        It can extract only entrypoints with specific fields ('clusterSpec',
        'instanceType', etc), depending on the value of _type.
        """
        if _type not in ('all', 'clusterSpec', 'instanceType'):
            raise DXError("Expected '_type' to be either 'all', 'clusterSpec', or 'instanceType'")

        if _type == 'all':
            return cls(system_requirements)

        extracted = defaultdict(dict)
        for entrypoint, req in system_requirements.items():
            if _type in req:
                extracted[entrypoint][_type] = req[_type]
        return cls(dict(extracted))

    def override_cluster_spec(self, srd):
        """
        Returns SystemRequirementsDict can be passed in a "systemRequirements"
        input to app-xxx/run, e.g. {'fn': {'clusterSpec': {initialInstanceCount: 3, version: "2.4.0", ..}}}
        Since full clusterSpec must be passed to the API server, we need to retrieve the cluster
        spec defined in app doc's systemRequirements and overwrite the field initialInstanceCount
        with the value the user passed to dx run for each entrypoint.
        initialInstanceCount is currently the only clusterSpec's field the user is allowed to change
        at runtime.
        A few scenarios when requesting instance count for different entrypoints with dx run 
        and the resulting merged systemRequirements (merged_cluster_spec). The bootstapScript
        field here is only one of many (version, ports, etc) that should be copied from app
        spec to merged_cluster_spec:

        Requested: {"*": 5}
        App doc: {"main": "clusterSpec": {"initialInstanceCount": 7, bootstrapScript: "x.sh"},
                "other": "clusterSpec": {"initialInstanceCount": 9, bootstrapScript: "y.sh"}}
        Merged: {"main": "clusterSpec": {"initialInstanceCount": 5, bootstrapScript: "x.sh"},
                "other": "clusterSpec": {"initialInstanceCount": 5, bootstrapScript: "y.sh"}}
        
        Requested: {"*": 15}
        App doc: {"main": "clusterSpec": {"initialInstanceCount": 7, bootstrapScript: "x.sh"},
                  "other": "clusterSpec": {"initialInstanceCount": 9, bootstrapScript: "y.sh"},
                  "*": "clusterSpec": {"initialInstanceCount": 11, bootstrapScript: "y.sh"}}
        Merged: {"main": "clusterSpec": {"initialInstanceCount": 15, bootstrapScript: "x.sh"},
                 "other": "clusterSpec": {"initialInstanceCount": 15, bootstrapScript: "y.sh"},
                 "*": "clusterSpec": {"initialInstanceCount": 15, bootstrapScript: "y.sh"}}

        Requested: {"main": 12}
        App doc: {"main": "clusterSpec": {"initialInstanceCount": 7, bootstrapScript: "x.sh"},
                  "other": "clusterSpec": {"initialInstanceCount": 9, bootstrapScript: "y.sh"}}
        Merged: {"main": "clusterSpec": {"initialInstanceCount": 12, bootstrapScript: "x.sh"}}

        Requested: {"main": 33}
        App doc: {"*": "clusterSpec": {"initialInstanceCount": 2, bootstrapScript: "z.sh"}}
        Merged: {"main": "clusterSpec": {"initialInstanceCount": 33, bootstrapScript: "z.sh"}}

        Requested: {"main": 22, "*": 11}
        App doc: {"*": "clusterSpec": {"initialInstanceCount": 2, bootstrapScript: "t.sh"}}
        Merged: {"main": "clusterSpec": {"initialInstanceCount": 22, bootstrapScript: "t.sh"},
                 "*": "clusterSpec": {"initialInstanceCount": 11, bootstrapScript: "t.sh"}}
        """

        merged_cluster_spec = copy.deepcopy(self.entrypoints)

        # Remove entrypoints without "clusterSpec"
        merged_cluster_spec = dict([(k, v) for k, v in merged_cluster_spec.items() if v.get("clusterSpec") is not None])

        # Remove entrypoints not provided in requested instance counts
        merged_cluster_spec = dict([(k, v) for k, v in merged_cluster_spec.items() if \
            k in srd.entrypoints or "*" in srd.entrypoints])

        # Overwrite values of self.entrypoints.clusterSpec with the ones from srd
        # Named entrypoint takes precedence over the wildcard
        for entry_pt, req in merged_cluster_spec.items():
            merged_cluster_spec[entry_pt]["clusterSpec"].update(
                srd.entrypoints.get(entry_pt, srd.entrypoints.get("*"))["clusterSpec"])

        # Check if all entrypoints in srd are included in merged_cluster_spec
        # (if a named entrypoint was used in srd and such an entrypoint doesn't exist
        #  in app sys req, we need to take the cluster spec from the app's "*", if it exists)
        for entry_pt, req in srd.entrypoints.items():
            if entry_pt not in merged_cluster_spec and "*" in self.entrypoints and "clusterSpec" in self.entrypoints["*"]:
                merged_cluster_spec[entry_pt] = {"clusterSpec": copy.deepcopy(self.entrypoints["*"]["clusterSpec"])}
                merged_cluster_spec[entry_pt]["clusterSpec"].update(req["clusterSpec"])

        return SystemRequirementsDict(merged_cluster_spec)

    def _add_dict_values(self, d1, d2):
        """
        Merges the values of two dictionaries, which are expected to be dictionaries, e.g
        d1 = {'a': {'x': pqr}}
        d2 = {'a': {'y': lmn}, 'b': {'y': rst}}
        will return: {'a': {'x': pqr, 'y': lmn}, 'b': {'y': rst}}.
        Collisions of the keys of the sub-dictionaries are not checked.
        """

        if d1 is None and d2 is None:
            return None

        d1 = d1 or {}
        d2 = d2 or {}

        added = {}
        for key in set(list(d1.keys()) + list(d2.keys())):
            added[key] = dict(d1.get(key, {}), **(d2.get(key, {})))
        return added

    def __add__(self, other):
        if not isinstance(other, SystemRequirementsDict):
            raise DXError("Developer error: SystemRequirementsDict expected")
        added_entrypoints = self._add_dict_values(self.entrypoints, other.entrypoints)
        return SystemRequirementsDict(added_entrypoints)

    def as_dict(self):
        return self.entrypoints
