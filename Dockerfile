# This is a temporary file do not edit
                            # edit /Users/Rune/Projects/hobbes/docker/Dockerfile.service instead
                         FROM builder AS build
ARG EXECUTABLE
ARG MAJOR_ARG=0
ARG MINOR_ARG=0
ARG BUILD_VERSION_ARG=1

ENV EXECUTABLE=${EXECUTABLE}
ENV MAJOR=${MAJOR_ARG}
ENV MINOR=${MINOR_ARG}
ENV BUILD_VERSION=${BUILD_VERSION_ARG}
RUN dotnet publish -c ${BUILD_CONFIGURATION} -o /app

# final stage/image
FROM kmdrd/runtime:5.0
COPY --from=build /tmp/start.sh /tmp/start.sh 
WORKDIR /app
COPY --from=build /app .

ENV port 8085
ENTRYPOINT /tmp/start.sh