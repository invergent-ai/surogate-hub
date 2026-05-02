# Surogate Hub Helm Chart

A Helm chart to deploy Surogate Hub on Kubernetes.

This will start Surogate Hub while data will be stored inside the container and will not be persisted.

### Installing the chart

To install the chart with custom configuration values:

```bash
# Deploy Surogate Hub with helm release "my-shub"
helm install -f my-values.yaml my-shub shub/shub
```

Example `my-values.yaml` using PostgreSQL:

```yaml
secrets:
  databaseConnectionString: postgres://postgres:myPassword@my-shub-db.rds.amazonaws.com:5432/shub?search_path=shub
  authEncryptSecretKey: <some random string>
shubConfig: |
  database:
    type: postgres
  blockstore:
    type: s3
    s3:
      region: us-east-1
  gateways:
    s3:
      domain_name: s3.shub.example.com
```

Example `my-values.yaml` using PostgreSQL with Cloud SQL Auth Proxy in GCP:

```yaml
secrets:
  databaseConnectionString: postgres://<DB_USERNAME>:<DB_PASSWORD>@localhost:5432/<DB_NAME>
  authEncryptSecretKey: <some random string>
shubConfig: |
  database:
    type: postgres
  blockstore:
    type: gs
    gs:
      credentials_json: '<credentials_json>'
serviceAccount:
  name: <service account name>
gcpFallback:
  enabled: true
  instance: <my_project>:<my_region>:<my_instance>=tcp:5432
```

Example `my-values.yaml` using DynamoDB:

```yaml
secrets:
  authEncryptSecretKey: <some random string>
shubConfig: |
  database:
    type: dynamodb
    dynamodb:
      table_name: my-shub
      aws_region: us-east-1
  blockstore:
    type: s3
    s3:
      region: us-east-1
  gateways:
    s3:
      domain_name: s3.shub.example.com
```

Sensitive information like `database_connection_string` (used by PostgreSQL) is given through "secrets" section, and will be injected into Kubernetes secrets.

You should give your Kubernetes nodes access to all S3 buckets (or other resources) you intend to use Surogate Hub with.
If you can't provide such access, Surogate Hub can be configured to use an AWS key-pair to authenticate (part of the `shubConfig` YAML).

## Configurations

| **Parameter**                         | **Description**                                                                                                                                                                                                                                                                            | **Default**                         |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------- |
| `secrets.databaseConnectionString`    | PostgreSQL connection string to be used by Surogate Hub. (Ignored when existingSecret is set)                                                                                                                                                                                              |                                     |
| `secrets.authEncryptSecretKey`        | A random (cryptographically safe) generated string that is used for encryption and HMAC signing. (Ignored when existingSecret is set)                                                                                                                                                      |                                     |
| `existingSecret`                      | Name of existing secret to use for the chart's secrets (by default the charts create a secret to hold the authEncryptSecretKey and databaseConnectionString                                                                                                                                |                                     |
| `secretKeys.databaseConnectionString` | Name of key in existing secret to use for a PostgreSQL databaseConnectionString (no default). Only used when sed when `existingSecret is set`                                                                                                                                              |                                     |
| `secretKeys.authEncryptSecretKey`     | Name of key in existing secret to use for authEncryptSecretKey. Only used when existingSecret is set.                                                                                                                                                                                      |                                     |
| `shubConfig`                          | Surogate Hub config YAML stringified, as shown above. See [reference](https://docs.shub.io/reference/configuration.html) for available configurations.                                                                                                                                     |                                     |
| `replicaCount`                        | Number of Surogate Hub pods                                                                                                                                                                                                                                                                | `1`                                 |
| `resources`                           | Pod resource requests & limits                                                                                                                                                                                                                                                             | `{}`                                |
| `service.type`                        | Kubernetes service type                                                                                                                                                                                                                                                                    | ClusterIP                           |
| `service.port`                        | Kubernetes service external port                                                                                                                                                                                                                                                           | 80                                  |
| `extraEnvVars`                        | Adds additional environment variables to the deployment (in yaml syntax)                                                                                                                                                                                                                   | `{}` See [values.yaml](values.yaml) |
| `extraEnvVarsSecret`                  | Name of a Kubernetes secret containing extra environment variables                                                                                                                                                                                                                         |                                     |
| `s3Fallback.enabled`                  | If set to true, an [S3Proxy](https://github.com/gaul/s3proxy) container will be started. Requests to Surogate Hub S3 gateway with a non-existing repository will be forwarded to this container.                                                                                                 |                                     |
| `s3Fallback.aws_access_key`           | An AWS access key to be used by the S3Proxy for authentication                                                                                                                                                                                                                             |                                     |
| `s3Fallback.aws_secret_key`           | An AWS secret key to be used by the S3Proxy for authentication                                                                                                                                                                                                                             |                                     |
| `gcpFallback.enabled`                 | If set to true, an [GCP Proxy](https://github.com/GoogleCloudPlatform/cloud-sql-proxy) container will be started.                                                                                                                                                                          |                                     |
| `gcpFallback.instance`                | The instance to connect to. See the example above for the format.                                                                                                                                                                                                                          |                                     |
| `committedLocalCacheVolume`           | A volume definition to be mounted by Surogate Hub and used for caching committed metadata. See [here](https://kubernetes.io/docs/concepts/storage/volumes/#volume-types) for a list of supported volume types. The default values.yaml file shows an example of how to use this parameter. |                                     |
| `serviceAccount.name`                 | Name of the service account to use for the Surogate Hub pods. If not set, use the `default` service account.                                                                                                                                                                               |                                     |
