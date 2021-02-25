
FROM mcleanj/ubuntuconda:latest

ENV ACCEPT_INTEL_PYTHON_EULA yes
# Configure the Intel MPI parameters
# ENV I_MPI_FABRICS shm:ofi
# ENV I_MPI_PMI_LIBRARY /lib64/libpmi.so
# ENV I_MPI_DEBUG 1
# ### FOR QDR NETWORK #####
# ENV FI_MPI_PROVIDER psm
# ENV FI_PSM_TAGGED_RMA 0
# ENV FI_PSM_AM_MSG 1
# ENV FI_PSM_UUID uuidgen

RUN conda create -n mpi
#note environment won't be auto activated, this just activates for the run stuff
SHELL ["conda", "run", "-n", "mpi", "/bin/bash", "-c"]

RUN conda config --add channels intel
RUN conda install -y -q intelpython3_core

RUN apt-get update -qq
# RUN apt-get install -y -qq g++


COPY . /app
WORKDIR /app

# RUN pip install --upgrade pip
RUN conda install --file requirements.txt

RUN apt-get update && \
    apt-get install -y git vim.tiny curl jq && \
    curl -L -sk -o /usr/local/bin/jq "https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64" && \
    chmod a+x /usr/local/bin/jq


    # install the cli from git
RUN git clone -b v2.3 https://github.com/TACC-Cloud/agave-cli.git /usr/local/agave-cli && \
    chmod +x /usr/local/agave-cli/bin/*

ENV PATH /opt/apps/software/lib/libfabric/1.9.1/bin:/usr/local/agave-cli/bin:$PATH



#ENV FI_UNIVERSE_SIZE 2

#export FI_LOG_LEVEL=Debug
ENV MV2_ENABLE_AFFINITY 0

RUN mkdir /root/.agave
COPY ./current_store/current /root/.agave/current

RUN cd /app

#conda run --no-capture-output -n mpi python -u driver.py
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "mpi", "python", "-u", "driver.py"]