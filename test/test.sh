#!/bin/bash

if [ ${TEST_CASE} == "all" ]
then
  echo "Running all tests"
  python3 -m unittest discover -p *test.py
else
  echo "Running $TEST_CASE"
  python3 -m unittest ${TEST_CASE}
fi