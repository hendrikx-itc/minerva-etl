#!/bin/sh -e
# /usr/lib/emacsen-common/packages/remove/minerva-storage-geospatial-schema

FLAVOR=$1
PACKAGE=minerva-storage-geospatial-schema

if [ ${FLAVOR} != emacs ]; then
    if test -x /usr/sbin/install-info-altdir; then
        echo remove/${PACKAGE}: removing Info links for ${FLAVOR}
        install-info-altdir --quiet --remove --dirname=${FLAVOR} /usr/share/info/minerva-storage-geospatial-schema.info.gz
    fi

    echo remove/${PACKAGE}: purging byte-compiled files for ${FLAVOR}
    rm -rf /usr/share/${FLAVOR}/site-lisp/${PACKAGE}
fi
