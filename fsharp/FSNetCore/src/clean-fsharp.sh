#!/bin/sh
DIRS="DataContracts NetworkServer Node NodesTest"
for dir in $DIRS; do
	rm -rf $dir/bin $dir/obj
done