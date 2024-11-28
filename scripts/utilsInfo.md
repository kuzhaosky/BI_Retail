```bash
docker images --filter "dangling=true" -q | xargs -r docker rmi
docker images --format "{{.Repository}}:{{.Tag}}" | grep -v "downloads.unstructured.io/unstructured-io/unstructured-api" | xargs -r -I {} docker rmi {}
```

```bash
docker exec -it docker-airflow-worker-1 bash
```



```bash
cd /opt/airflow 
```


