#!/bin/bash

PROJECT_ROOT=$(pwd)

export PYTHONPATH=$PROJECT_ROOT:$PYTHONPATH

source .venv/bin/activate

echo "======================================"
echo "Environment Activated"
echo "======================================"

python --version
echo "PYTHONPATH=$PYTHONPATH"