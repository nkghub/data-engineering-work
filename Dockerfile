FROM python:3.9.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
#COPY new_ingest.py new_ingest.py 
#COPY ingest_dataa.py ingest_dataa.py 
COPY ingest_newdata.py ingest_newdata.py 

#ENTRYPOINT [ "python3.9", "new_ingest.py" ]
ENTRYPOINT [ "python3.9", "ingest_newdata.py" ]
