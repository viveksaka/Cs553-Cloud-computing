#!/bin/bash

TEST_REPO_URL="https://github.com/viveksaka/PA3"
TEST_REPO_DIR="viveksaka/PA3"

if [ ! -d $TEST_REPO_DIR ]
then
    git clone $TEST_REPO_URL
fi

cd $TEST_REPO_DIR
git fetch & git pull
cd ..

bash $TEST_REPO_DIR/my-check-submission.sh all
bash $TEST_REPO_DIR/my-submission-test.sh all
