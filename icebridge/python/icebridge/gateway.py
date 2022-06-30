import os
import sys

from py4j.java_gateway import JavaGateway, java_import


def launch_gateway() -> JavaGateway:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    jarpath = os.path.join(dir_path, "jars/app-uber.jar")

    if not os.path.exists(jarpath):
        raise ImportError(
            f"We could not find a jar at {jarpath}. Have you ran './gradlew uberJar' in the `icebridge` directory?"
        )

    gateway = JavaGateway.launch_gateway(
        jarpath=jarpath, redirect_stdout=sys.stdout, redirect_stderr=sys.stderr, die_on_exit=True
    )
    java_import(gateway.jvm, "org.apache.iceberg.*")

    return gateway
