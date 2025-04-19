import configparser
import sys

config = configparser.ConfigParser()
config.read("config.ini")

# Nodes amount config:
filter_nodes_2000s = int(config["DEFAULT"]["2000s_FILTER_NODES"])
ar_es_filter_nodes = int(config["DEFAULT"]["AR_ES_FILTER_NODES"])
ar_filter_nodes = int(config["DEFAULT"]["AR_FILTER_NODES"])
top_5_investors_filter_nodes = int(config["DEFAULT"]["TOP_5_INVESTORS_FILTER_NODES"])

# Create compose file
output_file = "docker-compose-dev.yaml"

docker_compose_txt: str = """services:"""

# RABBIT MQ ----------------------------------------------------------------------------------------
docker_compose_txt += """
    rabbitmq:
        image: rabbitmq:management
        container_name: rabbitmq
        ports:
            # Web Panel port
            - "15672:15672"
        environment:
            # To log in at web panel
            RABBITMQ_DEFAULT_USER: admin
            RABBITMQ_DEFAULT_PASS: admin
"""

# WORKERS ------------------------------------------------------------------------------------------
# FILTERS
for i in range(1, filter_nodes_2000s + 1):
    docker_compose_txt += f"""
    2000s-filter-{i}:
        container_name: 2000s-filter-{i}
        image: filter:latest
        environment:
            - FILTER_TYPE=2000s_filter
            - FILTER_NUM={i}
        networks:
            - tp1_net
        depends_on:
            # TODO: esto solo asegura que el container de rabbitmq se levante primero, no que esté funcionando
            - rabbitmq 
"""
    
for i in range(1, ar_es_filter_nodes + 1):
    docker_compose_txt += f"""
    ar-es-filter-{i}:
        container_name: ar-es-filter-{i}
        image: filter:latest
        environment:
            - FILTER_TYPE=ar_es_filter
            - FILTER_NUM={i}
        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""
    
for i in range(1, ar_filter_nodes + 1):
    docker_compose_txt += f"""
    ar-filter-{i}:
        container_name: ar-filter-{i}
        image: filter:latest
        environment:
            - FILTER_TYPE=ar_filter
            - FILTER_NUM={i}
        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""
    
for i in range(1, top_5_investors_filter_nodes + 1):
    docker_compose_txt += f"""
    top-5-investors-filter-{i}:
        container_name: top-5-investors-filter-{i}
        image: filter:latest
        environment:
            - FILTER_TYPE=top-5-investors-filter
            - FILTER_NUM={i}
        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""
    
# tp1_net: Isolated network that allow containers to communicate
# ipam: IP Address Management settings, used to configure the network
# driver: default. Use default IPAM driver to manage IP addresses
networks_section = """
networks:
  tp1_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""

docker_compose_txt += networks_section


# Write file
with open(output_file, "w") as f:
    f.write(docker_compose_txt)

print(f"File {output_file} succesfully generated!")
