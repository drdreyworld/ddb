#!/bin/bash

path="`pwd`"

binpath="$path/bin/"

cd $path/server/bin && go build -o $binpath/server
