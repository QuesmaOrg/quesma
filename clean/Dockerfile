FROM python:3.9-alpine

RUN pip install requests python-dotenv

COPY .* clean.py ./

CMD ["python", "./clean.py"]