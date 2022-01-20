# use base python image with python 2.7
FROM python:3.8

ENV PYTHONUNBUFFERED true 

# set working directory to /app/
WORKDIR /app/

# Anaconda installing
#RUN wget https://repo.continuum.io/archive/Anaconda3-5.0.1-Linux-x86_64.sh
#RUN bash Anaconda3-5.0.1-Linux-x86_64.sh -b
#RUN rm Anaconda3-5.0.1-Linux-x86_64.sh

# Set path to conda
#ENV PATH /root/anaconda3/bin:$PATH


# copy code base to the image
COPY . .
# install python dependencies

RUN pip3 install -r requirements.txt
