#!/bin/bash

javac -cp .:$(find . -name *.jar | tr '\n' ':'):/usr/share/java/mongo.jar -d bin/ $(find src -name *.java)
