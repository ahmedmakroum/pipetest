# pipetest
pipeline test using csv bucket samples from gcp to dump them in bq sink



if it doesnt work try it in a virtual environement ($ python3 -m venv testenv)
dont forget to activate it (.\testenv\Scripts\activate)
and also install gcp sdk with pip (pip install apache-beam[gcp])
you'll also need to change the bucket name and bigquery dataset to yours after you authentificated with gcloud on your terminal (gcloud auth) and created your buckets and sink.

for the masterparse branch you might need to install some libraries try ($ pip install apache-beam[gcp] google-cloud-storage google-cloud-bigquery pandas)


if there's a problem and the code wont work for you try installing the gcp sdk and libraries in the same virtual env or eun them in higher privileges (root (sudo -your cmd))
