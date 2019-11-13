# 1. scala + spark image
# FROM openjdk:8u222-jdk-stretch
# RUN mkdir /usr/local/scala \
#  && cd /usr/local/scala \
#  && wget -q --show-progress --progress=bar:force:noscroll https://downloads.lightbend.com/scala/2.12.9/scala-2.12.9.tgz \
#  && tar -zxf scala-2.12.9.tgz \
#  && mv scala-2.12.9/* . \
#  && rm -rf scala-2.12.9* \
#  && mkdir /usr/local/spark \
#  && cd /usr/local/spark \
#  && wget -q --show-progress --progress=bar:force:noscroll https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz \
#  && tar -zxf spark-2.4.3-bin-hadoop2.7.tgz \
#  && mv spark-2.4.3-bin-hadoop2.7/* . \
#  && rm -rf spark-2.4.3-bin-hadoop2.7*
# ENV PATH="/usr/local/spark/bin:/usr/local/scala/bin:$PATH"

# export PATH="/usr/local/spark/bin:/usr/local/scala/bin:$PATH"


# Note: Scala хэл яг ч суулгах шаардлагагүй юм шиг байна..?
# src: https://stackoverflow.com/questions/27589614/scala-is-a-must-for-spark
# src: https://stackoverflow.com/questions/27590474/how-can-spark-shell-work-without-installing-scala-beforehand

FROM openjdk:8u222-jdk-stretch
# Image has already python 2.x installed, installing dependencies
RUN mkdir /usr/local/spark \
 && cd /usr/local/spark \
 && wget --quiet --show-progress --progress=bar:force:noscroll https://archive.apache.org/dist/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz \
 && tar -zxf spark-2.4.*.tgz \
 && mv spark-2.4.*-bin-hadoop2.7/* . \
 && rm -rf spark-2.4.*-bin-hadoop2.7* \
 && apt-get update > /dev/null \
 && apt-get install -qq libgtk2.0-dev cron > /dev/null \
 && rm -rf /var/lib/apt/lists/* \
 && curl -sS https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
 && python get-pip.py \
 && rm -rf get-pip.py
ENV PATH="/usr/local/spark/bin:$PATH"

# Installing source code
WORKDIR /erxes-ai
COPY crontab /etc/cron.d/ai-cron
COPY . .
RUN pip install -q -r requirements.txt \
 && chmod 0644 /etc/cron.d/ai-cron \
 && /usr/bin/crontab /etc/cron.d/ai-cron

ENTRYPOINT ["cron", "-f"]

# apt-get install libglib2.0-0 (didn't work)
# libgtk2.0-dev (worked)
