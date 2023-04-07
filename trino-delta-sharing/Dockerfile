ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-delta-sharing-$VERSION/ /usr/lib/trino/plugin/deltasharing/
ADD catalog/deltasharing.properties /etc/trino/catalog/deltasharing.properties
