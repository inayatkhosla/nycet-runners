FROM python:3.6

RUN mkdir /src
WORKDIR /src
COPY requirements.txt /src/
RUN pip install -r requirements.txt
COPY /src/ /src/

CMD python $FILE_NAME $PASSWORD
