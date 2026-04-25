
# Git
git init
git add .
git commit -m "init dbt project"



# Kestra
docker run --pull=always -it -p 8080:8080 --user=root --name kestra --restart=always --env-file "D:\dbt_capstone\dbt_bigquery\.env_encoded" -v kestra_data:/app/storage -v /var/run/docker.sock:/var/run/docker.sock -v /tmp:/tmp kestra/kestra:latest server local
