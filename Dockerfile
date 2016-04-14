FROM base-centos7
MAINTAINER zig
RUN yum install gcc -y && yum install supervisor -y && yum install python34 -y && yum install python34-devel -y && yum install python34-setuptools -y
RUN easy_install-3.4 pip
COPY . /home/seven/dbop/
RUN cd /home/seven/dbop && pip3.4 install -r requirements.txt
CMD cd /home/seven/dbop && python3.4 dkrstrap.py