#!/bin/sh

rm -rf doc/rst && mkdir doc/rst
make doc
pandoc --read=html --write=rst doc/leo_backend_db_api.html -o doc/rst/leo_backend_db_api.rst
pandoc --read=html --write=rst doc/leo_backend_db_bitcask.html -o doc/rst/leo_backend_db_bitcask.rst
pandoc --read=html --write=rst doc/leo_backend_db_eleveldb.html -o doc/rst/leo_backend_db_eleveldb.rst
pandoc --read=html --write=rst doc/leo_backend_db_ets.html -o doc/rst/leo_backend_db_ets.rst
pandoc --read=html --write=rst doc/leo_backend_db_server.html -o doc/rst/leo_backend_db_server.rst
