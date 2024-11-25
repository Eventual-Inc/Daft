# Working dir for submission

This an example of a working directory which can be included inside of runs of the ray-cluster in a GitHub Actions workflow.

## Usage

In order to submit your own script, create a file in this directory, add+commit+push it to GitHub, and submit a GitHub Actions workflow request by specifying the path to this directory and a python command to execute the file.

## Example

First create the file:

```bash
touch .github/working-dir/my_script.py
echo "print('Hello, world!')" >> .github/working-dir/my_script.py
```

Then submit the request to execute the workflow to run this file on a ray-cluster.
You can either do this via the GitHub Actions UI or by running the GitHub CLI:

```bash
gh workflow run run-cluster.yaml --ref $MY_BRANCH -f command="python my_script.py"
```
