import argparse
import json


def parse_env_var_str(env_var_str: str) -> dict:
    iter = map(
        lambda s: s.strip().split("="),
        filter(lambda s: s, env_var_str.split(",")),
    )
    return {k: v for k, v in iter}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-vars", required=True)
    args = parser.parse_args()

    env_vars = parse_env_var_str(args.env_vars)
    ray_env_vars = {
        "env_vars": {
            "DAFT_ENABLE_RAY_TRACING": "1",
            **env_vars,
        },
    }
    print(json.dumps(ray_env_vars))
