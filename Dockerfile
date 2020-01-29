FROM python:3-alpine
ADD . /opt/accounting.client
ADD ./b2sharecollector /etc/periodic/hourly/b2sharecollector
RUN chmod +x /etc/periodic/hourly/b2sharecollector
WORKDIR /opt/accounting.client

RUN pip3 install --no-cache .

CMD ["sh", "-c", "echo \"starting crond\" && (crond) && echo \"tailing...\" && : >> /srv/app/.accounting.log && tail -f /srv/app/.accounting.log"]