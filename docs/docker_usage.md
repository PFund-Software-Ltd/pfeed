[MinIO]: https://min.io/
[Docker]: https://www.docker.com/

# Docker Usage

## Docker Users
For convenience, you can use `pfeed docker-compose ...` to run the docker compose file stored in your config path.

To find your config path, run `pfeed config where`.

This docker compose file uses [Profiles](https://docs.docker.com/reference/compose-file/profiles/) to let you run different services separately.
e.g. `pfeed docker-compose up -d minio` will only run [MinIO].

---

## Docker Novices / Traders
> This section is primarily for users who are **traders and not familiar with what Docker is**.

In simple terms, [Docker] is like a **box that runs software** without needing to install it directly on your machine.

This means you can run software like [MinIO] just by running `pfeed docker-compose up -d minio`.

### Starting a Service
To start a service, run `pfeed docker-compose up -d {software_name}`.

### Stopping a Service
To stop a service, run `pfeed docker-compose down {software_name}`.

### Removing Data
If a service has stored data, you can check in the "Volumes" section of Docker Desktop.
To remove stored data, add the `-v` flag, run `pfeed docker-compose down -v {software_name}`.

### Services in PFeed's docker-compose.yml
When you run `pfeed docker-compose`, it will use the docker-compose.yml file stored in your config path.

To see which services are available, run `pfeed config open --docker-file` to open the docker-compose.yml file.

```{tip} Note for Traders
`pfeed docker-compose` is only provided for convenience. 
If you are willing to learn some basics about Docker, you can ignore pfeed's docker-compose.yml file and use `docker compose` directly as you prefer.
```
