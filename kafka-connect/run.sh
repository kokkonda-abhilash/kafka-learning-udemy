#!/usr/bash/env bash
#run twitter connector
connect-standalone connect-standalone.properties twitter.properties
#OR (Linux / mac OSX)
connect-standalone.sh connect-standalone.properties twitter.properties
#OR (Windows)
connect-standalone.bat connect-standalone.properties twitter.properties