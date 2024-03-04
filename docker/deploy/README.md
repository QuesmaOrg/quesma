### Quesma AiO (All in One) VM

Simplistic setup - a VM in GCP which clones out repo and boots everything up with `docker-compose`.
There is a system user named `quesma` with your public key added to `~/.ssh/authorized_keys` so that you can SSH into the VM.

Because currently this deployment script clones the repo, it also requires you to have our [`Quesma AiO GCP vm` GitHub deploy key](https://github.com/QuesmaOrg/poc-elk-mitmproxy/settings/keys) to be places in
`~/.quesma_deploy_keys/quesma-aio-gcp-vm-deploy-key` on your local machine. You can get this key from 1Password.


## Terraform primer

After installing terraform (`brew install terraform`):
1. Make sure you have GCP service account created with credentials file saved on your machine - see [quesma-all-in-one-vm.tf](quesma-all-in-one-vm.tf)
2. Initialize terraform: `terraform init` (required only once, to download GCP provider plugin)
3. Create the resources: `terraform apply`. Once done, it will output the public IP of the created instance.

Destroy the resources with `terraform destroy`.

Good practice when sharing resources pool is to prefix them somehow. I'd suggest local system username for that.
Unfortunately, terraform cannot just read the environment variable - it has to be passed explicitly. 
Also, you need to pass your IP address to punch a hole in the firewall.

So eventually, your TF commands would look like this:
```
TF_VAR_user=$USER TF_VAR_allowed_ip=$(dig +short myip.opendns.com @resolver1.opendns.com) terraform <COMMAND>
```