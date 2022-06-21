import os
import sys

from py4j.java_gateway import JavaGateway


class IceBridgeClient:
    def __init__(self) -> None:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        jarpath = os.path.join(dir_path, "app/build/libs/app-uber.jar")

        if not os.path.exists(jarpath):
            raise ImportError(f"We could not find a jar at {jarpath}. Have you ran './gradlew uberJar' in the `icebridge` directory?")

        self.gateway = JavaGateway.launch_gateway(
            jarpath=jarpath,
            redirect_stdout=sys.stdout,
            redirect_stderr=sys.stderr,
            die_on_exit=True
        )