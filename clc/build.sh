#!/bin/sh

export JAVA_HOME=/usr/java/jdk1.7.0_21
export ANT_OPTS=-XX:-UseSplitVerifier

ant clean
ant build
