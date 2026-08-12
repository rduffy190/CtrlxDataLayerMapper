#!/usr/bin/env bash
set -e
TARGET_DOTNET=linux-x64
PUBLISH_DIR=./publish

rm -rf ${PUBLISH_DIR}

dotnet publish --configuration Release --runtime ${TARGET_DOTNET} --property:PublishDir=${PUBLISH_DIR} --self-contained true /property:GenerateFullPaths=true /property:PublishSingleFile=true /property:PublishTrimmed=true /property:DebugType=None /property:DebugSymbols=false

/home/riley/ctrlx-automation-sdk/scripts/build-snap-amd64.sh
