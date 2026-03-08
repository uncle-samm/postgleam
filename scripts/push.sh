#!/bin/sh
set -e

branch=$(git branch --show-current)
if [ "$branch" != "master" ]; then
  echo "Not on master branch (on $branch). Merge to master first."
  exit 1
fi

echo "Pushing master to origin..."
git push origin master

echo "Pushing master to main..."
git push origin master:main

echo "Done."
