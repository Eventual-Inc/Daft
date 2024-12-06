"""Given a comma-separated string of environment variables, parse them into a dictionary.

Example:
    env_str = "a=1,b=2"
    result = parse_env_var_str(env_str)
    # returns {"a":1,"b":2}
"""

import argparse
import json


def parse_env_var_str(env_var_str: str) -> dict:
    iter = map(
        lambda s: s.strip().split("="),
        filter(lambda s: s, env_var_str.split(",")),
    )
    return {k: v for k, v in iter}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--enable-ray-tracing", action="store_true")
    parser.add_argument("--env-vars", required=True)
    args = parser.parse_args()

    env_vars = parse_env_var_str(args.env_vars)
    if args.enable_ray_tracing:
        env_vars["DAFT_ENABLE_RAY_TRACING"] = "1"
    ray_env_vars = {
        "env_vars": env_vars,
    }
    print(json.dumps(ray_env_vars))


if __name__ == "__main__":
    main()
