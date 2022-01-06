from enum import Enum
from typing import Union, Sequence, List

from . import processing


class Command:
    """
    Helper for building subprocess commands.
    """

    OrderType = Enum("OrderType", ["PARAMS_FLAGS_OPTIONS", "FLAGS_OPTIONS_PARAMS"])
    BuildType = Enum("BuildType", ["STRING", "LIST"])

    def __init__(
        self, executables: str, order_type: OrderType = OrderType.FLAGS_OPTIONS_PARAMS
    ):
        self._executables = executables.split(" ")
        self._params = []
        self._options = []
        self._flags = []
        self._order_type = self._process_order_type(order_type)

    def with_flag(self, flag: str, predicate: bool = True):
        if predicate:
            self._flags.append(flag)
        return self

    def with_option(
        self, name: str, value: Union[str, Sequence[str]], predicate: bool = True
    ):
        if predicate:
            self._options.append({name: name, value: value})
        return self

    def with_order(self, order_type: OrderType):
        self._order_type = self._process_order_type(order_type)

    def with_param(self, value: str, predicate: bool = True):
        if predicate:
            self._params.append(value)
        return self

    def with_params(self, *values: Union[str, List[str]], predicate: bool = True):
        if not predicate:
            return self

        for param in values:
            if isinstance(param, str):
                self._params.append(param)
            elif isinstance(param, list):
                self._params.extend(param)
            else:
                raise ValueError(
                    f"Parameter type is neither string nor list: {type(param)}"
                )

        return self

    def _process_order_type(self, order_type: OrderType):
        if order_type is None:
            order_type = self.OrderType.FLAGS_OPTIONS_PARAMS
        if not isinstance(order_type, self.OrderType):
            raise ValueError(f"Invalid order type: {order_type}")
        return order_type

    @staticmethod
    def _process_options(options) -> [str]:
        processed_options = []
        for op_name, op_value in options:
            processed_options.append(op_name)
            if isinstance(op_value, list):
                processed_options.extend(op_value)
            else:
                processed_options.append(op_value)
        return processed_options

    def build(self, result_type: BuildType = None) -> Union[str, Sequence[str]]:
        if result_type is None or not isinstance(result_type, self.BuildType):
            result_type = self.BuildType.LIST

        self._options = self._process_options(self._options)

        command = self._executables
        order = self._order_type.name.lower().split("_")
        for part in order:
            command.extend(getattr(self, f"_{part}"))

        if result_type == self.BuildType.LIST:
            return command
        elif result_type == self.BuildType.STRING:
            return " ".join(command)
        else:
            raise NotImplementedError(
                f"Unsupported result type requested: {result_type}"
            )

    def build_string(self):
        return self.build(result_type=self.BuildType.STRING)

    def build_list(self):
        return self.build(result_type=self.BuildType.LIST)


class DockerImage:
    """
    Abstraction over local docker commands for easier handling and debugging.
    """

    def __init__(
        self,
        image_path: str,
        image_name: str,
        mounts: [str] = None,
        working_dir: str = None,
        debug: bool = False,
    ):
        if mounts is None:
            mounts = ["/home/dnanexus:/home/dnanexus"]
        if working_dir is None:
            working_dir = "/home/dnanexus"

        self.image_path = image_path
        self.image_name = image_name
        self.mounts = mounts
        self.working_dir = working_dir
        self._output = None
        self._debug = debug
        self._last_command = None
        self._last_process = None
        self._load_process = None

    def load(self, block=True) -> None:
        self._load_process = processing.run(
            f"docker load -i {self.image_path}", block=block
        )

    def load_async(self) -> None:
        self.load(block=False)

    @property
    def output(self):
        if self._last_process is None or not self._last_process.done:
            raise ValueError(
                "No command or command not finished yet, output not available."
            )
        return self._last_process.output

    @property
    def last_command(self):
        if self._last_command is None:
            raise ValueError("No command has been run yet.")
        return self._last_command

    @property
    def loaded(self) -> bool:
        return self._load_process is not None and self._load_process.ok

    def wait_for_load(self) -> None:
        if self._load_process is None:
            raise ValueError("No image load requested, nothing to wait for.")

        while not self._load_process.done:
            pass
        if not self._load_process.ok:
            raise RuntimeError(
                f"DockerImage load subprocess did not finish successfully:\n{self}"
            )

    def run(
        self, command: Union[str, Sequence[str]], force_bash_string=False, block=True
    ):
        if not self.loaded:
            raise DockerError(f"Image {self.image_name} not loaded but run attempted")

        docker_cmd = ["docker", "run", "-e", "PYTHONUNBUFFERED='s'"]
        if self.mounts is not None and len(self.mounts) > 0:
            for mount in self.mounts:
                docker_cmd.extend(["-v", mount])
        if self.working_dir is not None and len(self.working_dir) > 0:
            docker_cmd.extend(["-w", self.working_dir])
        docker_cmd.append(self.image_name)

        if isinstance(command, list):
            if force_bash_string:
                string_command = " ".join(command)
                command = ["bash", "-c", string_command]
            docker_cmd.extend(command)
        elif isinstance(command, str):
            docker_cmd.extend(["bash", "-c", f'"{command}"'])
        else:
            raise DockerError(
                f"Unknown command type {type(command)}. Expected list or string."
            )

        self._last_command = docker_cmd

        if self._debug:
            print(self)

        self._last_process = processing.run(
            " ".join(docker_cmd), block=block, stdout=processing.StdType.PIPE
        )

    def wait_for_done(self) -> None:
        if self._last_process is None:
            raise ValueError("No command run requested, nothing to wait for.")

        while not self._last_command.done:
            pass
        if not self._last_command.ok:
            raise RuntimeError(f"The run command did not finish successfully:\n{self}")

    def __str__(self):
        return (
            f"Image name: {self.image_name}\nImage path: {self.image_path}\nLoaded: {self.loaded}\n"
            f"Mounts: {self.mounts}\nWorking dir: {self.working_dir}\nLast command: {self._last_command}"
        )


class DockerError(Exception):
    pass
