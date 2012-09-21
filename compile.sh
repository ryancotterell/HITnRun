#!/bin/bash

javac -cp .:$(find . -name *.jar | tr '\n' ':') -d bin/ $(find src -name *.java)
