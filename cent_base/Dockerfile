FROM centos:7
RUN  yum install -y openmpi-devel
RUN  yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel
RUN yum install -y wget
RUN yum install -y make
RUN wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz
RUN tar xzf Python-3.7.9.tgz
RUN cd Python-3.7.9; ./configure --enable-optimizations; make altinstall
RUN export CC=/usr/lib64/openmpi/bin/mpicc; env MPICC=/usr/lib64/openmpi/bin/mpicc pip3.7 install --no-cache-dir mpi4py
CMD [ "/bin/bash" ]