#!/bin/bash

java -cp .:$(find . -name *.jar | tr '\n' ':'):bin edu.jhu.ccb.$@
