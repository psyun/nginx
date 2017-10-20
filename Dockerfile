FROM 172.26.1.26:5000/base/centos:v1

WORKDIR /gowild/appserver

ADD ./ ./qdream-nginx-proxy
RUN yum install -y readline-devel pcre-devel openssl-devel gcc git wget gcc-c++;yum clean all

RUN echo "/usr/local/lib" >> /etc/ld.so.conf && ldconfig && \
    wget https://github.com/edenhill/librdkafka/archive/v0.11.0.tar.gz && \
    tar zxvf v0.11.0.tar.gz && \
    cd librdkafka-0.11.0 \
    ./auto/configure \
    make && make install

RUN cd qdream-nginx-proxy  \
    ./auto/configure --without-http --with-stream --with-ld-opt="-lrdkafka" \
    make && make install

CMD ["/usr/local/nginx/sbin/nginx", "-g", "daemon off;"]

