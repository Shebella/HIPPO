#!/bin/sh

find . -name "*.xml" -exec grep "synchronous" '{}' \; -print
