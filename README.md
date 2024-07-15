# pipetest
pipeline test using csv bucket samples from gcp to dump them in bq sink



if it doesnt work try it in a virtual environement ($ python3 -m venv testenv)
dont forget to activate it (.\testenv\Scripts\activate)
and also install gcp sdk with pip (pip install apache-beam[gcp])
you'll also need to change the bucket name and bigquery dataset to yours after you authentificated with gcloud on your terminal (gcloud auth) and created your buckets and sink.

