# check=skip=InvalidDefaultArgInFrom
ARG DOCKER_REGISTRY
FROM ${DOCKER_REGISTRY}/proxy-images/eclipse-temurin:26-jdk-amd64

ENV GRADLE_VERSION=9.5.0
ENV GRADLE_HOME=/opt/gradle/gradle-${GRADLE_VERSION}
ENV PATH=${GRADLE_HOME}/bin:${PATH}

RUN apt-get update && apt-get install -y curl unzip git \
    && curl -fsSL https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip -o /tmp/gradle.zip \
    && unzip /tmp/gradle.zip -d /opt/gradle \
    && rm /tmp/gradle.zip \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean && rm -rf /var/lib/apt/lists/
