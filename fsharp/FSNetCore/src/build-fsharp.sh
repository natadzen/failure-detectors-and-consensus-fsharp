#!/bin/sh
DIRS="DataContracts NetworkServer Node NodesTest"
for dir in $DIRS; do
	rm -rf $dir/bin $dir/obj
done
for dir in $DIRS; do
	dotnet build $dir
done