FROM ubuntu:20.04
RUN apt-get update && apt-get install --no-install-recommends -y python3.9 python3.9-dev python3-pip
COPY . .
RUN pip install -r requirements.txt
RUN chmod +x full_insert_script.py
ENTRYPOINT "./full_insert_script.py"
