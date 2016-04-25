FROM base-centos7
MAINTAINER zig
RUN yum install gcc -y && yum install supervisor -y && yum install python34 -y && yum install python34-devel -y && yum install python34-setuptools -y
RUN easy_install-3.4 pip
COPY . /opt/platform/dbop/
RUN cd /opt/platform/dbop && pip3.4 install -r requirements.txt
RUN chown -R seven.seven /opt/platform/dbop
USER seven
CMD cd /opt/platform/dbop && python3.4 dkrstrap.py