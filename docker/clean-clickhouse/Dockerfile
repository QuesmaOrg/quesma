FROM python:3.9-alpine

RUN pip install requests python-dotenv

COPY .* clean.py ./
COPY /schema /schema
CMD ["python", "./clean.py"]