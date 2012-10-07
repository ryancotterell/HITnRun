#!/bin/bash

java -cp .:$(find . -name *.jar | tr '\n' ':'):/usr/share/java/mongo.jar:bin edu.jhu.ccb.$@
