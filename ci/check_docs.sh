set -e

if git diff --quiet HEAD^ HEAD -- docs/; then
  echo "Documentation has not been updated."
  exit 1
else
  echo "Documentation is up-to-date."
fi
