# настройки установки hadoop, многое отсюда https://github.com/ayyoubmaul/hadoop-docker/tree/main 
FROM eclipse-temurin:8-jdk-focal

RUN apt-get update && apt-get install -y ssh net-tools curl rsync

ENV HADOOP_VERSION=3.3.6
ENV HADOOP_URL=https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN curl -fSL "$HADOOP_URL" -o /tmp/hadoop.tar.gz \
    && tar -xzvf /tmp/hadoop.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm /tmp/hadoop.tar.gz

RUN ln -s $HADOOP_HOME/etc/hadoop /etc/hadoop

ADD https://raw.githubusercontent.com/big-data-europe/docker-hadoop/master/base/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# настройки клиента
WORKDIR /app
COPY entrypoint.sh .
COPY shadow.txt .
RUN chmod +x ./entrypoint.sh
