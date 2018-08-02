# globomap-driver-acs
Python library for globomap-loader to get data from Cloudstack

## Plugin environment variables configuration
All of the environment variables below must be set for the plugin to work properly.
The variables are the combination of the the prefix 'ACS', the environment (region)
code passed on the driver constructor and the variable name.

| Variable                    |  Description                    | Example                                      |
|-----------------------------|---------------------------------|----------------------------------------------|
| ACS_$env_API_URL            | Cloudstack API URL              | http://yourdomain.cloudstack:8080/api/client |
| ACS_$env_API_KEY            | Cloudstack API key              | jIkLGAz0yqbJC15lS_XqHKRPZXI8M6               |
| ACS_$env_API_SECRET_KEY     | Cloudstack API Secret           | RJK0Xhb3iMwrIUIxJ3T7jL5fFrG14b               |
| ACS_$env_RMQ_HOST           | Cloudstack RabbitMQ host        | rabbitmq.yourdomain.cloudstack               |
| ACS_$env_RMQ_USER           | Cloudstack RabbitMQ user        | user-name                                    |
| ACS_$env_RMQ_PASSWORD       | Cloudstack RabbitMQ password    | password                                     |
| ACS_$env_RMQ_PORT           | Cloudstack RabbitMQ port        | 5673 (default value)                         |
| ACS_$env_RMQ_QUEUE          | Cloudstack RabbitMQ queue name  | events                                       |
| ACS_$env_RMQ_EXCHANGE       | Cloudstack RabbitMQ Exchange    | cloudstack-events (default value)            |
| ACS_$env_RMQ_LOADER_EXCHANGE| Cloudstack RabbitMQ Loader Exchange| cloudstack-globomap-loader                |
| ACS_$env_RMQ_VIRTUAL_HOST   | Cloudstack RabbitMQ virtual host| /globomap                                    |

## Environment variables configuration to use CloudstackDataLoader
| Variable                       |  Description                    | Example                                      |
|--------------------------------|---------------------------------|----------------------------------------------|
| GLOBOMAP_LOADER_API_URL        | GloboMap Loader API endpoint    | http://api.globomap.loader.domain.com:8080   |
| GLOBOMAP_LOADER_API_USER       | GloboMap Loader API user        | user                                         |
| GLOBOMAP_LOADER_API_PASSWORD   | GloboMap Loader API password    | password                                     |


## Example of use

```python
from globomap_driver_acs.driver import Cloudstack
driver = Cloudstack({'env':'ENV_NAME'})
driver.process_updates(print)
```
