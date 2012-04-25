#!/bin/bash
#
# Runs client bindings tests.
#
# Usage: ./run_tests.sh

export NUCLEUS_ENV=integrationTest

root=`dirname "$0"`

source "$root/environment"
export DB_PORT=`../bin/nuc-config mongod.port`
export DX_APISERVER_PORT=`../bin/nuc-config apiserver.port`
export DX_APISERVER_HOST=localhost
export PYTHONPATH="$root/../execserver/env/lib/python2.7/site-packages:$PYTHONPATH"

dbpath="$root/test_db"
mkdir -p "$dbpath"
rm -rf "$dbpath"/*

echo "Starting MongoDB in path '$dbpath' and port '$DB_PORT'"
mongod --dbpath "$dbpath" --port $DB_PORT --nojournal --noprealloc >> "$root/mongod.log" &
mongod_pid=$!

echo -n "Starting API server on port $DX_APISERVER_PORT..."
"$root"/../apiserver/server.js --port $DX_APISERVER_PORT > apiserver.log 2>&1 &
api_server_pid=$!
echo " (PID $api_server_pid)"

# Need to wait until the API server is up and ready to handle requests before
# starting to run tests.
sleep 3

echo -n "Initializing database..."
"$root"/../apiserver/tasks/init_database.js
echo " done."

echo -n "Starting tests..."
"$root"/lib/python/test/test_dxpy.py &
test_pid=$!
echo " (PID $test_pid)"

function kill_them {
  echo "Killing tests ($test_pid)"
  kill $test_pid 2>/dev/null
  echo "Killing API server ($api_server_pid)"
  kill $api_server_pid 2>/dev/null
  echo "Killing MongoDB ($mongod_pid)"
  kill $mongod_pid 2>/dev/null

  wait $mongod_pid
  rm -rf "$dbpath"/*

  exit
}

trap kill_them int

echo "Waiting on tests... (PID $test_pid)"
wait $test_pid
exit_code=$?

echo "Killing API server and child processes (PID $api_server_pid)"
pkill -P $api_server_pid
kill $api_server_pid
wait $api_server_pid

echo "Killing MongoDB (PID $mongod_pid)"
kill $mongod_pid
wait $mongod_pid
rm -rf "$dbpath"/*

exit $exit_code
