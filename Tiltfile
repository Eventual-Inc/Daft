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
print("""
-----------------------------------------------------------------
✨ Hello Tilt! This appears in the (Tiltfile) pane whenever Tilt
   evaluates this file.
-----------------------------------------------------------------
""".strip())
warn('ℹ️ Open {tiltfile_path} in your favorite editor to get started.'.format(
    tiltfile_path=config.main_path))

# Build Docker image
#   Tilt will automatically associate image builds with the resource(s)
#   that reference them (e.g. via Kubernetes or Docker Compose YAML).
#
#   More info: https://docs.tilt.dev/api.html#api.docker_build
#
docker_build('localhost:5001/eventual-hub-jupyterhub:latest',
             context='eventual-hub/',
             # (Optional) Use a custom Dockerfile path
             dockerfile='./Dockerfile.jupyterhub',
             platform="linux/amd64",
)

docker_build('localhost:5001/eventual-hub-backend:latest',
             context='eventual-hub/',
             # (Optional) Use a custom Dockerfile path
             dockerfile='./Dockerfile.backend',
             # (Recommended) Updating a running container in-place
             # https://docs.tilt.dev/live_update_reference.html
             live_update=[
                # Sync files from host to container
                sync('./eventual-hub/backend', '/app'),
             ],
             platform="linux/amd64",
)


# Apply Kubernetes manifests
#   Tilt will build & push any necessary images, re-deploying your
#   resources as they change.
#
#   More info: https://docs.tilt.dev/api.html#api.k8s_yaml
#

k8s_yaml(kustomize("kubernetes-ops/eventual-hub/_pre"))
k8s_yaml(kustomize("kubernetes-ops/eventual-hub/installs/local_tilt_dev"))

# Tilt doesn't recognize these as a resource, but these CRDs need to be created first
CERT_CRD = [
   "cert-manager:namespace",
   "certificaterequests.cert-manager.io:customresourcedefinition",
   "certificates.cert-manager.io:customresourcedefinition",
   "challenges.acme.cert-manager.io:customresourcedefinition",
   "clusterissuers.cert-manager.io:customresourcedefinition",
   "issuers.cert-manager.io:customresourcedefinition",
   "orders.acme.cert-manager.io:customresourcedefinition",
]
k8s_resource(new_name="cert_crd", objects=CERT_CRD)

# Cert-manager deployments depend on the CRD
CERT_MANAGER_RESOURCES = ['cert-manager', 'cert-manager-cainjector', 'cert-manager-webhook']
for r in CERT_MANAGER_RESOURCES:
   k8s_resource(r, resource_deps=["cert_crd"])

# Certificates depend on the cert-manager deployments to be up
CERTS = [
   "hub-ca:certificate",
   "notebooks-ca:certificate",
   "proxy-api-ca:certificate",
   "proxy-cert:certificate",
   "proxy-client-ca:certificate",
   "services-ca:certificate",
   "proxy-client-ca-issuer:issuer",
   "self-signed-issuer:issuer",
]
k8s_resource(new_name="certs", objects=CERTS, resource_deps=CERT_MANAGER_RESOURCES)


# All other services depend on the certs
k8s_resource("backend", resource_deps=["certs"])
k8s_resource("jupyterhub", resource_deps=["certs"])
k8s_resource("jupyterhub-proxy", resource_deps=["certs"])

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
load('ext://git_resource', 'git_checkout')


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
    git_checkout('https://github.com/tilt-dev/tilt-avatars.git',
                 checkout_dir='tilt-avatars')


# Edit your Tiltfile without restarting Tilt
#   While running `tilt up`, Tilt watches the Tiltfile on disk and
#   automatically re-evaluates it on change.
#
#   To see it in action, try uncommenting the following line with
#   Tilt running.
# tilt_demo()
