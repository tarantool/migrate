#!/bin/bash

curl -s https://packagecloud.io/install/repositories/tarantool/1_6/script.deb.sh | sudo bash
sudo apt-get install -y tarantool tarantool-dev libsmall-dev libmsgpuck-dev cmake --force-yes
TARANTOOL_DIR=/usr/include cmake . -DCMAKE_BUILD_TYPE=Release
make
ctest
