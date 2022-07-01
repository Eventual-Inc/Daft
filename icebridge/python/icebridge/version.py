import os

BASE_VERSION = '0.0.1'

def get_version() -> str:
    subversion = os.environ.get('GITHUB_RUN_NUMBER', 'dev')
    return f'{BASE_VERSION}.{subversion}'