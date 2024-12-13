Quesma Helm Chart
=================

This Helm Chart runs Quesma demo on Kubernetes cluster. It has been tested on Minikube and is not intended for production use. 
* it assumes that you already have your ClickHouse/Hydrolix cluster up and running.
* it installs Quesma along with minimal instance of Elasticsearch and Kibana.

1. Create values.yaml file based on the template file:
    ```shell
    cp quesma/values.template.yaml quesma/values.yaml
    ```
2. Fill in the values in `values.yaml` file in the `config` section.
   Alternatively, you can just edit the Quesma configuration in `quesma/templates/configmap.yml`.
3. Install the chart:
    ```shell
    helm install quesma quesma/ -f quesma/values.yaml
    ``` 
4. Profit!   

This installs `quesma` helm chart from `quesma/` directory.

You can access the services by setting up a minikube tunnel:
```
minikube tunnel
```
**Note:** This command will block the terminal, so you will need to keep that terminal window open all the time if you 
want to access the services exposed in k8s cluster.

Then follow to:
* http://127.0.0.1:30560 Kibana
* http://127.0.0.1:30999 Quesma Admin UI
* http://127.0.0.1:30808 Quesma frontend connector (Elasticsearch API in this case)

Sometimes `minikube tunnel` doesn't work, in that case you can use `kubectl port-forward` command to forward the ports to your local machine.
```bash
kubectl port-forward svc/kibana 30560:5601
kubectl port-forward svc/quesma-ext-admin 30999:9999
kubectl port-forward svc/quesma-ext-frontend 30808:8080
```
And then access the aforementioned URLs in your browser. You also need to keep the terminal process up.

You can remove it anytime with
```bash
helm uninstall quesma
```

### Local development 

Make sure you have `helm` and `minikube` installed (both can be installed with `brew install helm minikube`).
Make sure you have local k8s cluster running (`minikube (start|status|stop|delete)`) before installing the chart.



