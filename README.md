# globomap-driver-acs
Python library for globomap-loader to get data from Cloudstack

## Plugin environment variables configuration
All of the environment variables below must be set for the plugin to work properly.

| Variable                  |  Description                    | Example                                      |
|---------------------------|---------------------------------|----------------------------------------------|
| ACS_API_URL               | Cloudstack API URL              | http://yourdomain.cloudstack:8080/api/client |
| ACS_API_KEY               | Cloudstack API key              | jIkLGAz0yqbJC15lS_XqHKRPZXI8M6               |
| ACS_API_SECRET_KEY        | Cloudstack API Secret           | RJK0Xhb3iMwrIUIxJ3T7jL5fFrG14b               |
| ACS_RMQ_HOST              | Cloudstack RabbitMQ host        | rabbitmq.yourdomain.cloudstack               |
| ACS_RMQ_USER              | Cloudstack RabbitMQ user        | user-name                                    |
| ACS_RMQ_PASSWORD          | Cloudstack RabbitMQ password    | password                                     |
| ACS_RMQ_PORT              | Cloudstack RabbitMQ port        | 5673 (default value)                         |
| ACS_RMQ_QUEUE		        | Cloudstack RabbitMQ queue name  | events                                       |
| ACS_RMQ_VIRTUAL_HOST      | Cloudstack RabbitMQ virtual host| /globomap                                    |