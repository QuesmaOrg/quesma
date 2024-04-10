## Adding access for customer to download Quesma docker image a.k.a private beta release

1. Create Service Account [here](https://console.cloud.google.com/iam-admin/serviceaccounts?authuser=1&project=metal-figure-407109). \
   Make sure `Name` and `Description` are human-readable and meaningful. 
2. Go to Artifact Registry [repositories list](https://console.cloud.google.com/artifacts?referrer=search&authuser=1&project=metal-figure-407109), check `quesma-private-beta` repository.
3. In the `Permissions` tab click `Add Principal`, select Service Account you've just created and assign the role of `Artifact Registry Reader`.
4. Go to `Service Accounts` and create key for the Service Account. 

### Using the created Service Account Key to log in to GCR repository

Upon SA key creation it appeared in your `Downloads` folder. Encode it to base64 and send it to the customer.
Example:
```shell 
base64 -i metal-figure-407109-5ffc1ab67b2a.json -o encoded_sa_key.txt
```

What customer has to do is:  
```shell
cat encoded_sa_key.txt | docker login -u _json_key_base64 --password-stdin https://europe-central2-docker.pkg.dev
```
And they should be able to pull the image:
```
docker pull \
     europe-central2-docker.pkg.dev/metal-figure-407109/quesma-private-beta/quesma:first-blood
```


Sources:
* https://cloud.google.com/artifact-registry/docs/docker/authentication