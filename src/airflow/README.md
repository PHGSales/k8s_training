### To deploy the airflow  
`kubectl create namespace airflow`  
`helm repo add apache-airflow https://airflow.apache.org`  
`helm repo update`  
`helm install airflow apache-airflow/airflow --namespace airflow --debug --timeout 10m0s`  
`helm ls -n airflow`

### To show the values.yaml  
`helm show values apache-airflow/airflow > values.yaml`

### To apply changes on values.yaml  
`helm upgrade --install airflow apache-airflow/airflow -n airflow  -f values.yaml --debug`

### To access the web ui  
`kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow`  

#### Access the link:
`localhost:8080` 
Use the default credentials:
login: admin
password: admin


By: https://www.astronomer.io/events/recaps/official-airflow-helm-chart/