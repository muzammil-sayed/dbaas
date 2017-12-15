FROM openjdk:8u111-jre
MAINTAINER template-maintainer@jivesoftware.com

RUN wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | apt-key add -
RUN echo 'deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main 9.5' >> /etc/apt/sources.list.d/postgresql.list
RUN apt-get update -y && \
apt-get install -y postgresql-client-9.5 && \
apt-get clean -y

RUN cd /tmp/

EXPOSE 8080 8081

WORKDIR /app
ENTRYPOINT [ "/app/entrypoint.sh" ]
CMD [ "server" ]

ADD app /app/
ADD target/service.jar /app/service.jar
ADD target/swagger.json /app/swagger.json
ADD secrets/certs /etc/ssl/jiveprivate
ADD app/cloneschema.sh /app/cloneschema.sh
ADD secrets/instances.yaml /secrets/data-dbaas-instances/instances.yaml
ADD secrets/token /secrets/data-dbaas-auth-token/token
ADD secrets/aes.yaml /secrets/data-dbaas-aes/aes.yaml

RUN chmod +x /app/cloneschema.sh