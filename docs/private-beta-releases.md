## Adding customer to EAP programme 

We have generated 10 docker images for our EAP programme. Each docker image has built-in Quesma Docker license key.
The list of the docker images is kept in [this spreadsheet](https://docs.google.com/spreadsheets/d/1ODsINV5AmlJsb2cepIQEsjzPgLIfpXND4AcTnwL_hHk/edit?usp=sharing).

Quesma EAP docker images are stored in GCR repository which allows **unauthenticated access.** No credentials are required to pull the image.
```
docker pull \
     europe-central2-docker.pkg.dev/metal-figure-407109/quesma-private-beta/quesma:${ANY_EAP_IMAGE_TAG}
```

Our telemetry collector has a 
[list of the EAP license keys](https://github.com/QuesmaOrg/telemetry-collector/blob/main/phone_home/main.go).
