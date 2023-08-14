# Tips for future Aidan

## Build docker image

```bash
export tag=ghcr.io/artificialinc/elasticsearch:8.9.0-aidan-azure-c2426e20a68
export version=8.9.0
DOCKER_CONTEXT=default ./gradlew buildDockerImage --info
DOCKER_CONTEXT=default docker tag docker.elastic.co/elasticsearch/elasticsearch:$version-SNAPSHOT $tag
DOCKER_CONTEXT=default docker push $tag
```

## Test

```bash
./gradlew ':modules:repository-azure:test'

./gradlew ':modules:repository-azure:test' --tests "org.elasticsearch.repositories.azure.AzureClientProviderTests.testCanCreateAClientWithWorkloadIdentitySuccess" -Dtests.seed=A020C4CF22BC5FB9 -Dtests.locale=ar-EG -Dtests.timezone=Asia/Hovd -Druntime.java=20
```
