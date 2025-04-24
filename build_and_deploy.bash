#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

export DOCKER_BUILDKIT=1


echo "Building Docker image..."
docker build -t hsuanyichu840/quesma-sawmill:latest .

echo "Pushing Docker image to Docker Hub..."
docker push hsuanyichu840/quesma-sawmill:latest

echo "Applying Kubernetes config to dev-azure-koreacentral..."
kubectl --context dev-azure-koreacentral -n elasticsearch apply -f /Users/hsuanyi.chu/Desktop/quesma/quesma.yaml


kubectl --context dev-azure-koreacentral -n elasticsearch rollout restart deployment quesma

echo "Deployment complete."
