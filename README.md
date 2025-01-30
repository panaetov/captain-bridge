# What is Captain Bridge?

Captain Bridge is data-driven team management tool.

Build metrics for your team using data of Jira, Git and many other sources. Also, plan workload of your team with automatic Gantt charts.


# Full Documentation
Go to [wiki](https://github.com/panaetov/captain-bridge/wiki) 

# Terms of use

CaptainBridge is an Open Source project licensed under the terms of the LGPLv3 license.
Please see <http://www.gnu.org/licenses/lgpl-3.0.html> for license text.

Also, there is an enterprise version with a commercial-friendly license and many very useful features.

Please contact https://t.me/alexey_panaetov for purchasing options.

# Architecture

![](https://storage.yandexcloud.net/captain-bridge/architecture3.png)

### Server
Backend of the service.

### Index Periodic Job
Periodic job that collects and normalizes data from data sources. Normalized data goes to mongo database.

### Planning Periodic Job
Periodic job that is used for planning feature.


# Installation

## docker-compose


```
version: '3'
services:
  mongo:
    image: mongo:7.0-jammy
    environment:
      - MONGO_INITDB_ROOT_USERNAME=dbuser
      - MONGO_INITDB_ROOT_PASSWORD=dbpassword
    ports:
      - '27017:27017'
    volumes:
      - ./data:/data/db

  migrations:
    image: cryptolynx/captain-bridge:latest
    pull_policy: always
    command: migrate
    depends_on:
      - mongo

    environment:
      # Connection string of mongo database.
      - SERVICE_DB_DSN=mongodb://dbuser:dbpassword@mongo:27017

      # Database name in mongo database.
      - SERVICE_DB_NAME=db1

      # Path to CA file.
      # You can use this variable if TLS is used for connection to mongo database.
      # For this, CA file must be "injected" to container using volume.  
      - SERVICE_DB_CAFILE=

      # URL of HTTP endpoint returning static CA file.
      # You can use this variable as an alternative to $SERVICE_DB_CAFILE
      # if TLS is used for connection to mongo database.
      # Example: https://s3.cloud.net/myfolder/mongo_root.crt
      - SERVICE_DB_CAFILE_URL=

      # Base URL of Captain Bridge server.
      - SERVICE_BASIC_URL="http://server:9000"

      # Logging level (DEBUG, INFO, WARNING, ERROR).
      - SERVICE_LOG_LEVEL=INFO

    stdin_open: true
    tty: true

  server:
    image: cryptolynx/captain-bridge:latest
    command: server
    pull_policy: always
    depends_on:
      - migrations

    ports:
      - "9000:9000"

    environment:
      # Connection string of mongo database.
      - SERVICE_DB_DSN=mongodb://dbuser:dbpassword@mongo:27017

      # Database name in mongo database.
      - SERVICE_DB_NAME=db1

      # Path to CA file.
      # You can use this variable if TLS is used for connection to mongo database.
      # For this, CA file must be "injected" to container using volume.  
      - SERVICE_DB_CAFILE=

      # URL of HTTP endpoint returning static CA file.
      # You can use this variable as an alternative to $SERVICE_DB_CAFILE
      # if TLS is used for connection to mongo database.
      # Example: https://s3.cloud.net/myfolder/mongo_root.crt
      - SERVICE_DB_CAFILE_URL=

      # Base URL of Captain Bridge server.
      - SERVICE_BASIC_URL="http://server:9000"

      # Logging level (DEBUG, INFO, WARNING, ERROR).
      - SERVICE_LOG_LEVEL=INFO

    stdin_open: true
    tty: true

  index-jira-job:
    image: cryptolynx/captain-bridge:latest
    command: index_jira
    pull_policy: always
    depends_on:
      - migrations

    environment:
      # Connection string of mongo database.
      - SERVICE_DB_DSN=mongodb://dbuser:dbpassword@mongo:27017

      # Database name in mongo database.
      - SERVICE_DB_NAME=db1

      # Path to CA file.
      # You can use this variable if TLS is used for connection to mongo database.
      # For this, CA file must be "injected" to container using volume.  
      - SERVICE_DB_CAFILE=

      # URL of HTTP endpoint returning static CA file.
      # You can use this variable as an alternative to $SERVICE_DB_CAFILE
      # if TLS is used for connection to mongo database.
      # Example: https://s3.cloud.net/myfolder/mongo_root.crt
      - SERVICE_DB_CAFILE_URL=

      # Base URL of Captain Bridge server.
      - SERVICE_BASIC_URL="http://server:9000"

      # Logging level (DEBUG, INFO, WARNING, ERROR).
      - SERVICE_LOG_LEVEL=INFO

    stdin_open: true
    tty: true

  planning-job:
    image: cryptolynx/captain-bridge:latest
    command: actualize_plannings
    pull_policy: always
    depends_on:
      - migrations

    environment:
      # Connection string of mongo database.
      - SERVICE_DB_DSN=mongodb://dbuser:dbpassword@mongo:27017

      # Database name in mongo database.
      - SERVICE_DB_NAME=db1

      # Path to CA file.
      # You can use this variable if TLS is used for connection to mongo database.
      # For this, CA file must be "injected" to container using volume.  
      - SERVICE_DB_CAFILE=

      # URL of HTTP endpoint returning static CA file.
      # You can use this variable as an alternative to $SERVICE_DB_CAFILE
      # if TLS is used for connection to mongo database.
      # Example: https://s3.cloud.net/myfolder/mongo_root.crt
      - SERVICE_DB_CAFILE_URL=

      # Base URL of Captain Bridge server.
      - SERVICE_BASIC_URL="http://server:9000"

      # Logging level (DEBUG, INFO, WARNING, ERROR).
      - SERVICE_LOG_LEVEL=INFO
    stdin_open: true
    tty: true
```
Then open http://localhost:9000 in your favourite browser.
