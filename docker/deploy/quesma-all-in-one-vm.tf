provider "google" {
  # You need to create service account:
  # add a key to it, download the json file and place in the location specified below
  # https://console.cloud.google.com/iam-admin/serviceaccounts/create?hl=en&orgonly=true&project=metal-figure-407109&supportedpurview=organizationId
  credentials = file("~/.gcp_creds/my-personal-quesma-creds.json")
  project     = "metal-figure-407109" # This is a project created by Jacek
  region      = "us-central1"
}

variable "user" {
  type = string
  default = "anonymous-dev"
}

variable "allowed_ip" {
  description = "External IP address which will be allowed to accesc the VM"
  type = string
  default = "89.64.94.88"
}

resource "google_compute_instance" "vm_instance" {

  name         = "${var.user}-quesma-aio-vm"
  machine_type = "n1-standard-8"
  zone         = "europe-central2-a"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-10"
    }
  }

  network_interface {
    network = "default"
    access_config {
    }
  }

  metadata = {
     ssh-keys = "przemyslaw_hejman:${file("~/.ssh/id_rsa.pub")} \n quesma:${file("~/.ssh/id_rsa.pub")}"
  }

  metadata_startup_script = <<-EOF
    #!/bin/bash -x
    apt-get install ca-certificates curl
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc

    # Add the repository to Apt sources:
    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
      $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
    sudo apt-get update

    sudo apt-get -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin git htop

    whoami
    su - quesma

    echo "${file("~/.quesma_deploy_keys/quesma-aio-gcp-vm-deploy-key")}" > /home/quesma/.ssh/id_ed25519
    echo "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl
github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=
github.com ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCj7ndNxQowgcQnjshcLrqPEiiphnt+VTTvDP6mHBL9j1aNUkY4Ue1gvwnGLVlOhGeYrnZaMgRK6+PKCUXaDbC7qtbW8gIkhL7aGCsOr/C56SJMy/BCZfxd1nWzAOxSDPgVsmerOBYfNqltV9/hWCqBywINIR+5dIg6JTJ72pcEpEjcYgXkE2YEFXV1JHnsKgbLWNlhScqb2UmyRkQyytRLtL+38TGxkxCflmO+5Z8CSSNY7GidjMIZ7Q4zMjA2n1nGrlTDkzwDCsw+wqFPGQA179cnfGWOWRVruj16z6XyvxvjJwbz0wQZ75XK5tKSb7FNyeIEs4TT4jk+S4dhPeAUC5y+bDYirYgM4GC7uEnztnZyaVWQ7B381AK4Qdrwt51ZqExKbQpTUNn+EjqoTwvqNj4kqx5QUCI0ThS/YkOxJCXmPUWZbhjpCg56i+2aB6CmK2JGhn57K5mj0MNdBXA4/WnwH6XoPWJzK5Nyu2zB3nAZp+S5hpQs+p1vN1/wsjk=
" > /home/quesma/.ssh/known_hosts

    echo "Host github.com
    StrictHostKeyChecking no" > /home/quesma/.ssh/config

    chmod 600 /home/quesma/.ssh/id_ed25519
    chmod 700 /home/quesma/.ssh
    chown -R quesma:quesma /home/quesma/.ssh
    # Start SSH agent and add key
    eval "$(ssh-agent -s)"
    ssh-add /home/quesma/.ssh/id_ed25519
    sudo -u quesma git clone git@github.com:QuesmaOrg/poc-elk-mitmproxy.git /home/quesma/source

    sudo usermod -aG docker quesma

    sudo docker compose -f /home/quesma/source/docker/local-dev.yml up -d
  EOF


  tags = ["quesma-aio"]
}

resource "google_compute_firewall" "quesma_aio_allowed_ports" {
  name    = "${var.user}-quesma-aio-allowed-ports"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["8080", "5601", "8081", "9999", "8123"]
  }

  source_ranges = [
    # This your external IP, check with `dig +short myip.opendns.com @resolver1.opendns.com`
    # We can add more to the list.
    "${var.allowed_ip}/32"
  ]
}

output "external_ip" {  # this outputs external ip so that you can ssh quesma@<external_ip>
  value = google_compute_instance.vm_instance.network_interface.0.access_config.0.nat_ip
}

