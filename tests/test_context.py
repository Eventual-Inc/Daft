import subprocess
import sys

script_to_test = """
import daft
print(daft.context.get_context()._runner_config is None)
"""


def test_fresh_context_on_import():
    """Test that a freshly imported context doesn't have a runner config set"""
    result = subprocess.run([sys.executable, "-c", script_to_test], capture_output=True)
    assert result.stdout.decode().strip() == "True"
