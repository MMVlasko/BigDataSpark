FROM amazoncorretto:11-alpine

ENV SPARK_VERSION=3.5.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark

RUN apk add --no-cache \
    python3 \
    py3-pip \
    wget \
    bash \
    procps \
    curl \
    && ln -sf python3 /usr/bin/python

RUN pip3 install --break-system-packages --upgrade pip && \
    pip3 install --break-system-packages psycopg2-binary clickhouse-driver pandas

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

RUN wget -q -P ${SPARK_HOME}/jars/ \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.3.1/httpclient5-5.3.1.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.2.4/httpcore5-5.2.4.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.2.4/httpcore5-h2-5.2.4.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar

COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PATH=$PATH:${SPARK_HOME}/bin

WORKDIR ${SPARK_HOME}

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]