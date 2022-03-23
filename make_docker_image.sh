#!/bin/bash
## sole parameter is an integer indicating incremental daily version
## git branch --set-upstream-to=origin/dev dev	

if [ $# -lt 2 ]; then
    echo "ERROR: Must specify <github branch> and <integer indicating incremental daily version> e.g."
    echo "USAGE: $0 dev 1 <optional: --no-cache>"
    exit 1
fi

if [ $# -eq 3 ]; then
    NO_CACHE=$3
fi

APP_ROOT=boranga
GIT_BRANCH=$1
BUILD_TAG=dbcawa/$APP_ROOT:v$(date +%Y.%m.%d).$2
#BUILD_TAG=graememuller/$APP_ROOT:v$(date +%Y.%m.%d).$2
#BUILD_TAG=ghcr.io/GraemeMuller/$APP_ROOT:v$(date +%Y.%m.%d).$2
#git checkout $GIT_BRANCH &&
#git pull &&
#cd $APP_ROOT/frontend/$APP_ROOT/
#npm run build &&
#cd ../../../ &&
#source venv/bin/activate &&
#./manage.py collectstatic --no-input &&
docker image build $NO_CACHE --tag $BUILD_TAG . &&
echo $BUILD_TAG &&
docker push $BUILD_TAG
