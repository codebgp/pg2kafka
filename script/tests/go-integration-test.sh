#!/bin/bash
set -x

scriptdir=`dirname $0`
rootdir=$(dirname `dirname $scriptdir`)
name=pggotest

[[ $(docker ps -f "name=$name" --format '{{.Names}}') == $name ]] || \
	docker run -d -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres -p 5432:5432 --name $name postgres:12

export DATABASE_URL="postgres://postgres@localhost/postgres?sslmode=disable"
INTERVAL=1 MAX=5; for i in `seq 1 $MAX`; do \
	docker exec pggotest psql $DATABASE_URL -c "select 1;" > /dev/null && break \
	|| (echo waiting for postgres && sleep $INTERVAL && [[ $i = $MAX ]] && echo failed waiting for postgres && exit 1); done;
pg_status=$?
if [[ ! $pg_status = 0 ]]
then
	exit $pg_status
fi

go test $rootdir/... --count=1 -tags integrationtests
test_status=$?

docker rm -f $name

exit $test_status
