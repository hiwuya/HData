# Build with the connectors needed by one job, for example:
# docker build --build-arg MODULES=hdata-jdbc,hdata-kafka -t hdata:local -f deploy/docker/Dockerfile .
FROM docker.io/library/maven:3.9.11-eclipse-temurin-17 AS build

ARG MODULES=hdata-jdbc
ARG MAVEN_PROFILES=
WORKDIR /workspace
COPY . .

# Build only the requested connector modules and their reactor dependencies. The runtime image receives
# their jars plus all resolved runtime dependencies; it does not rely on a development Maven repository.
RUN mvn -B -pl "hdata-core,${MODULES}" -am install -DskipTests ${MAVEN_PROFILES:+-P${MAVEN_PROFILES}} \
 && mvn -B -pl "hdata-core,${MODULES}" -am dependency:copy-dependencies ${MAVEN_PROFILES:+-P${MAVEN_PROFILES}} \
      -DincludeScope=runtime -DoutputDirectory=/opt/hdata/lib \
 && find hdata-* -path '*/target/*.jar' -type f -exec cp {} /opt/hdata/lib/ \;

FROM docker.io/library/eclipse-temurin:17-jre
WORKDIR /opt/hdata
COPY --from=build /opt/hdata/lib/ ./lib/
COPY deploy/docker/entrypoint.sh /entrypoint.sh
COPY deploy/flink/submit.sh /opt/hdata/deploy/flink/submit.sh
RUN chmod 0555 /entrypoint.sh /opt/hdata/deploy/flink/submit.sh \
 && useradd --system --uid 10001 --create-home hdata
USER 10001
ENTRYPOINT ["/entrypoint.sh"]
