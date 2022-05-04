# Welcome to Tilt!
#   To get you started as quickly as possible, we have created a
#   starter Tiltfile for you.
#
#   Uncomment, modify, and delete any commands as needed for your
#   project's configuration.


# Output diagnostic messages
#   You can print log messages, warnings, and fatal errors, which will
#   appear in the (Tiltfile) resource in the web UI. Tiltfiles support
#   multiline strings and common string operations such as formatting.
#
#   More info: https://docs.tilt.dev/api.html#api.warn


SRC_CODE_DIRS = ['cmd', 'codegen', 'pkg', 'build.makefile', 'go.mod', 'go.sum', '.env', '.env.example']
AWS_KEY_ID_NAME="AWS_ACCESS_KEY_ID"
AWS_ACCESS_KEY_NAME="AWS_SECRET_ACCESS_KEY"
DAFT_WEB_ENDPOINT_PORT="DAFT_WEB_ENDPOINT_PORT"
IMAGE_REGISTRY="IMAGE_REGISTRY"

load('ext://dotenv', 'dotenv')

def load_env():
    # Look for .env file otherwise use .env.example file for variables
    if os.path.exists(".env"):
        dotenv(fn=".env")
    elif os.path.exists(".env.example"):
        dotenv(fn=".env.example")
    else:
        fail("Could not find '.env' or '.env.example' file in local dir")

load_env()

def get_var(name):
    if name not in os.environ:
        fail("could not find {}".format(name))
    return os.environ[name]

print("""
-----------------------------------------------------------------
✨ Hello Tilt! This appears in the (Tiltfile) pane whenever Tilt
   evaluates this file.
-----------------------------------------------------------------
""".strip())
warn('ℹ️ Open {tiltfile_path} in your favorite editor to get started.'.format(
    tiltfile_path=config.main_path))

load('ext://secret', 'secret_from_dict')

def deploy_aws_secret():
    aws_id = get_var(AWS_KEY_ID_NAME)
    aws_key = get_var(AWS_ACCESS_KEY_NAME)

    if aws_id == "" or aws_key == "":
        fail("aws creds not populated in .env or .env.example")

    aws_secrets = {
        AWS_KEY_ID_NAME: aws_id,
        AWS_ACCESS_KEY_NAME: aws_key
    }

    aws_secret_yaml = secret_from_dict("aws", inputs=aws_secrets)
    k8s_yaml(aws_secret_yaml)

deploy_aws_secret()


# Build Docker image
#   Tilt will automatically associate image builds with the resource(s)
#   that reference them (e.g. via Kubernetes or Docker Compose YAML).
#
#   More info: https://docs.tilt.dev/api.html#api.docker_build
#

local_resource(
    name="generate-flatbuffers",
    cmd="make gen-fbs",
    deps="./fbs",
)
registry=get_var(IMAGE_REGISTRY).strip().strip('"')
IMAGES = ['daftlet', 'reader', 'web']
update_settings(suppress_unused_image_warnings=["{}/reader".format(registry)])
for image in IMAGES:
    docker_build('{}/{}'.format(registry, image),
                context='.',
                dockerfile='Dockerfile',
                target='{}'.format(image),
                only=SRC_CODE_DIRS
    )

# Apply Kubernetes manifests
#   Tilt will build & push any necessary images, re-deploying your
#   resources as they change.
#
#   More info: https://docs.tilt.dev/api.html#api.k8s_yaml
#

k8s_yaml(['k8s/daftlet.yaml'])
k8s_yaml(['k8s/daft_web.yaml'])

k8s_resource(workload='daft-web-singlepod', port_forwards=port_forward(int(get_var(DAFT_WEB_ENDPOINT_PORT)), container_port=8080))

# Customize a Kubernetes resource
#   By default, Kubernetes resource names are automatically assigned
#   based on objects in the YAML manifests, e.g. Deployment name.
#
#   Tilt strives for sane defaults, so calling k8s_resource is
#   optional, and you only need to pass the arguments you want to
#   override.
#
#   More info: https://docs.tilt.dev/api.html#api.k8s_resource
#
# k8s_resource('my-deployment',
#              # map one or more local ports to ports on your Pod
#              port_forwards=['5000:8080'],
#              # change whether the resource is started by default
#              auto_init=False,
#              # control whether the resource automatically updates
#              trigger_mode=TRIGGER_MODE_MANUAL
# )


# Run local commands
#   Local commands can be helpful for one-time tasks like installing
#   project prerequisites. They can also manage long-lived processes
#   for non-containerized services or dependencies.
#
#   More info: https://docs.tilt.dev/local_resource.html
#
# local_resource('install-helm',
#                cmd='which helm > /dev/null || brew install helm',
#                # `cmd_bat`, when present, is used instead of `cmd` on Windows.
#                cmd_bat=[
#                    'powershell.exe',
#                    '-Noninteractive',
#                    '-Command',
#                    '& {if (!(Get-Command helm -ErrorAction SilentlyContinue)) {scoop install helm}}'
#                ]
# )


# Extensions are open-source, pre-packaged functions that extend Tilt
#
#   More info: https://github.com/tilt-dev/tilt-extensions
#
# load('ext://git_resource', 'git_checkout')


# Organize logic into functions
#   Tiltfiles are written in Starlark, a Python-inspired language, so
#   you can use functions, conditionals, loops, and more.
#
#   More info: https://docs.tilt.dev/tiltfile_concepts.html
#
def tilt_demo():
    # Tilt provides many useful portable built-ins
    # https://docs.tilt.dev/api.html#modules.os.path.exists
    if os.path.exists('tilt-avatars/Tiltfile'):
        # It's possible to load other Tiltfiles to further organize
        # your logic in large projects
        # https://docs.tilt.dev/multiple_repos.html
        load_dynamic('tilt-avatars/Tiltfile')
    watch_file('tilt-avatars/Tiltfile')
    # git_checkout('https://github.com/tilt-dev/tilt-avatars.git',
                #  checkout_dir='tilt-avatars')


# Edit your Tiltfile without restarting Tilt
#   While running `tilt up`, Tilt watches the Tiltfile on disk and
#   automatically re-evaluates it on change.
#
#   To see it in action, try uncommenting the following line with
#   Tilt running.
# tilt_demo()
