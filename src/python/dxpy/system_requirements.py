
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
    def __init__(self, instance_type=None, cluster_spec=None):
        self.instance_type = instance_type
        self.cluster_spec = cluster_spec

    @classmethod
    def entrypoint2instcount(cls, instance_count, entrypoint="*"):
        """
        Returns a dictionary {entrypoint: instance_count}. The instance_count should
        be either a:
        * string, eg. 6
        * dictionary, eg. {"main": 4, "other_function": 2}
        """
        if isinstance(instance_count, basestring):
            # By default, all entry points ("*") should use this instance type
            print("instance_count", instance_count)
            return {entrypoint: int(instance_count)}
        elif isinstance(instance_count, dict):
            # instance_type is a map of entry point to instance count
            return {k: int(v) for k, v in instance_count.items()}
        else:
            raise DXError('Expected instance_count field to be either a string or a dict')

    @classmethod
    def from_instance_type(cls, instance_type_arg, entrypoint="*"):
        """
        Returns SystemRequirementsDict with instance_type that can be passed as a
        "systemRequirements" input to job/new or run/ API calls. The instance_type_arg
        should be either a:
        * string, eg. mem1_ssd1_x2
        * dictionary, eg. {"main": "mem2_hdd2_x2", "other_function": "mem2_hdd2_x1"}
        """
        if instance_type_arg is None:
            return cls(instance_type=None)
        elif isinstance(instance_type_arg, basestring):
            # By default, all entry points ("*") should use this instance type
            return cls(instance_type={entrypoint: {"instanceType": instance_type_arg}})
        elif isinstance(instance_type_arg, dict):
            # instance_type is a map of entry point to instance type
            return cls(instance_type={fn: {"instanceType": fn_inst} for fn, fn_inst in instance_type_arg.items()})
        else:
            raise DXError('Expected instance_type field to be either a string or a dict')

    @classmethod
    def from_cluster_spec(cls, app_sys_reqs, instance_count_arg):
        """
        Returns SystemRequirementsDict with cluster_spec that can be passed as a "systemRequirements"
        input to app-xxx/run, e.g. {'fn': {'clusterSpec': {initialInstanceCount: 3, version: "2.4.0", ..}}}
        Since full clusterSpec must be passed to the API server, we need to retrieve the cluster
        spec defined in app doc's systemRequirements and overwrite the field initialInstanceCount
        with the value the user passed to dx run for each entrypoint.
        Instance count is currently the only clusterSpec's field the user is allowed to change
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

        requested_counts = cls.entrypoint2instcount(instance_count_arg)
        merged_cluster_spec = copy.deepcopy(app_sys_reqs)
        print("requested_counts", requested_counts)

        # Merge known entry points
        for entry_pt, req in app_sys_reqs.items():
            print("entry_pt", entry_pt)
            if "clusterSpec" not in req and entry_pt in requested_counts and entry_pt != "*":
                    raise DXCLIError(
                         '--instance-count is not supported for entrypoint ' + entry_pt + ' since the app' \
                         ' does not have "clusterSpec" defined for this entrypoint in its systemRequirements')

            if entry_pt not in requested_counts and "*" not in requested_counts or "clusterSpec" not in req:
                print("DELETING entrypoint", entry_pt)
                del merged_cluster_spec[entry_pt]
            else:
                # overwrite initialInstanceCount with the requested count
                # named entrypoint used in requested_counts takes precedense over the wildcard
                merged_cluster_spec[entry_pt]["clusterSpec"]["initialInstanceCount"] = requested_counts.get(entry_pt, requested_counts.get("*"))

        print("AAA", merged_cluster_spec)
        # Check if all elements in requested_counts were passed to merged_cluster_spec
        # (if a named entry_point was used in requested instance count and such an entrypoint
        # doesn't exist in app sys req, we need to take the cluster spec from the app's "*", if it exists)
        for entry_pt, inst_count in requested_counts.items():
            if entry_pt not in merged_cluster_spec and "*" in app_sys_reqs and "clusterSpec" in app_sys_reqs["*"]:
                merged_cluster_spec[entry_pt] = {"clusterSpec": copy.deepcopy(app_sys_reqs["*"]["clusterSpec"])}
                merged_cluster_spec[entry_pt]["clusterSpec"]["initialInstanceCount"] = inst_count
            # else:
            #     # Error out when user requested instance count for entrypoints
            #     # that don't exist in app systemRequirements
            #     if not merged_cluster_spec and requested_counts:
            #         requested_entry_pts = ",".join(requested_counts.keys())
            #         mesg = '--instance-count is not supported for entrypoints that are not' \
            #                ' specified in the app system requirements or entrypoints without clusterSpec: ' + requested_entry_pts
            #         raise DXCLIError(mesg)
        return cls(cluster_spec=merged_cluster_spec)

    def _add_dictionaries(self, one_dict, other_dict):
        if one_dict is None and other_dict is None:
            return None

        one_dict = one_dict or {}
        other_dict = other_dict or {}

        if len(set(one_dict.keys()).intersection(set(other_dict.keys()))) > 0:
            raise ValueError("Entrypoint collisions are not accepted when adding system requirements dictionaries")

        one_dict.update(other_dict)
        return one_dict

    def __add__(self, other):
        """Add only, raise on collisions"""
        if not isinstance(other, SystemRequirementsDict):
            raise DXError("Developer error: SystemRequirementsDict expected")

        added_instance_types = self._add_dictionaries(self.instance_type, other.instance_type)
        added_cluster_specs = self._add_dictionaries(self.cluster_spec, other.cluster_spec)

        return SystemRequirementsDict(instance_type=added_instance_types,
                                      cluster_spec=added_cluster_specs)

    def as_dict(self):
        """
        Returns a dictionary that can be passed as a "systemRequirements"
        input to app-xxx/run, e.g. 
        {'fn': {"clusterSpec": {initialInstanceCount: 3, version: "2.4.0", ..},
                "instanceType": "ssd1_mem1_x4"
               }
        }
        """
        entrypoints = defaultdict(dict)
        if self.instance_type is not None:
            for entrypoint, req in self.instance_type.items():
                entrypoints[entrypoint]['instanceType'] =  req["instanceType"]
        if self.cluster_spec is not None:
            for entrypoint, req in self.cluster_spec.items():
                entrypoints[entrypoint]['clusterSpec'] =  req["clusterSpec"]
        return dict(entrypoints)
