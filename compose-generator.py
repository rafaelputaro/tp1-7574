import configparser

config = configparser.ConfigParser()
config.read("config.ini")

# Nodes amount config:
filter_nodes_2000s = int(config["DEFAULT"]["2000s_FILTER_NODES"])
ar_es_filter_nodes = int(config["DEFAULT"]["AR_ES_FILTER_NODES"])
ar_filter_nodes = int(config["DEFAULT"]["AR_FILTER_NODES"])
single_country_origin_filter_nodes = int(config["DEFAULT"]["SINGLE_COUNTRY_ORIGIN_FILTER_NODES"])
nlp_nodes = int(config["DEFAULT"]["NLP_NODES"])
shards = int(config["DEFAULT"]["SHARDS"])
top_5_investors_filter_nodes = shards

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
        volumes:
            - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf
        networks:
            - tp1_net
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
            - SHARDS={shards}
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
            - SHARDS={shards}
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
            - SHARDS={shards}
        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""

for i in range(1, single_country_origin_filter_nodes + 1):
    docker_compose_txt += f"""
    single-country-origin-filter-{i}:
        container_name: single-country-origin-filter-{i}
        image: filter:latest
        environment:
            - FILTER_TYPE=single_country_origin_filter
            - FILTER_NUM={i}
            - SHARDS={shards}

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
            - FILTER_TYPE=top_5_investors_filter
            - FILTER_NUM={i}
            - SHARDS={shards}

        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""
# JOINER --------------------------------------------------------------------------------------------
for i in range(1, shards + 1):
    docker_compose_txt += f"""
    joiner_group_by_movie_id_ratings_{i}:
        container_name: joiner_group_by_movie_id_ratings_{i}
        image: joiner:latest
        environment:
            - JOINER_TYPE=group_by_movie_id_ratings
            - JOINER_ID={i}
            - JOINER_INPUT_QUEUE_BASE_NAME=ar_movies_2000_and_later
            - JOINER_INPUT_QUEUE_SEC_NAME=ratings
            - JOINER_OUTPUT_QUEUE_NAME=movies_top_and_bottom
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""

for i in range(1, shards + 1):
    docker_compose_txt += f"""
    joiner_group_by_movie_id_credits_{i}:
        container_name: joiner_group_by_movie_id_credits_{i}
        image: joiner:latest
        environment:
            - JOINER_TYPE=group_by_movie_id_credits
            - JOINER_ID={i+1}
            - JOINER_INPUT_QUEUE_BASE_NAME=ar_movies_2000_and_later
            - JOINER_INPUT_QUEUE_SEC_NAME=credits
            - JOINER_OUTPUT_QUEUE_NAME=actor_movies_count
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""




# AGGREGATOR --------------------------------------------------------------------------------------------
docker_compose_txt += f"""
    aggregator_movies:
        container_name: aggregator_movies
        image: aggregator:latest
        environment:
            - AGGREGATOR_TYPE=movies
            - AGGREGATOR_ID=1
            - AGGREGATOR_AMOUNT_SOURCES={ar_es_filter_nodes}
            - AGGREGATOR_INPUT_QUEUE_NAME=movies_ar_es_2000s
            - AGGREGATOR_OUTPUT_QUEUE_NAME=movies_report
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""

docker_compose_txt += f"""
    aggregator_top_5:
        container_name: aggregator_top_5
        image: aggregator:latest
        environment:
            - AGGREGATOR_TYPE=top_5
            - AGGREGATOR_ID=2
            - AGGREGATOR_AMOUNT_SOURCES={top_5_investors_filter_nodes}
            - AGGREGATOR_INPUT_QUEUE_NAME=movies_top_5_investors
            - AGGREGATOR_OUTPUT_QUEUE_NAME=top_5_report
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""


docker_compose_txt += f"""
    aggregator_top_10:
        container_name: aggregator_top_10
        image: aggregator:latest
        environment:
            - AGGREGATOR_TYPE=top_10
            - AGGREGATOR_ID=3
            - AGGREGATOR_AMOUNT_SOURCES={shards}
            - AGGREGATOR_INPUT_QUEUE_NAME=actor_movies_count
            - AGGREGATOR_OUTPUT_QUEUE_NAME=top_10_report
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""


docker_compose_txt += f"""
    aggregator_top_and_bottom:
        container_name: aggregator_top_and_bottom
        image: aggregator:latest
        environment:
            - AGGREGATOR_TYPE=top_and_bottom
            - AGGREGATOR_ID=4
            - AGGREGATOR_AMOUNT_SOURCES={shards}
            - AGGREGATOR_INPUT_QUEUE_NAME=movies_top_and_bottom
            - AGGREGATOR_OUTPUT_QUEUE_NAME=top_and_bottom_report
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""


docker_compose_txt += f"""
    aggregator_metrics:
        container_name: aggregator_metrics
        image: aggregator:latest
        environment:
            - AGGREGATOR_TYPE=metrics
            - AGGREGATOR_ID=5
            - AGGREGATOR_AMOUNT_SOURCES=1
            - AGGREGATOR_INPUT_QUEUE_NAME=negative_movies
            - AGGREGATOR_INPUT_QUEUE_SEC_NAME=positive_movies
            - AGGREGATOR_OUTPUT_QUEUE_NAME=metrics_report
        networks:
            - tp1_net
        depends_on:
            - rabbitmq
        links: 
            - rabbitmq
"""


# NLP
for i in range(1, nlp_nodes + 1):
    docker_compose_txt += f"""
    nlp-{i}:
        container_name: nlp-{i}
        image: nlp:latest
        environment:
            - NODE_NUM={i}
            - SHARDS={shards}
        networks:
            - tp1_net
        depends_on:
            - rabbitmq 
"""

# CONTROLLER ----------------------------------------------------------------------------------------
all_dependencies = ["rabbitmq"]
for i in range(1, filter_nodes_2000s + 1):
    all_dependencies.append(f"2000s-filter-{i}")
for i in range(1, ar_es_filter_nodes + 1):
    all_dependencies.append(f"ar-es-filter-{i}")
for i in range(1, ar_filter_nodes + 1):
    all_dependencies.append(f"ar-filter-{i}")
for i in range(1, top_5_investors_filter_nodes + 1):
    all_dependencies.append(f"top-5-investors-filter-{i}")
for i in range(1, top_5_investors_filter_nodes + 1):  # same count as NLP nodes
    all_dependencies.append(f"nlp-{i}")

controller_depends_on = "\n".join([f"            - {name}" for name in all_dependencies])

docker_compose_txt += f"""
    controller:
        container_name: controller
        image: controller:latest
        build:
            context: .
            dockerfile: src/server/gateway/Dockerfile
        networks:
            - tp1_net
        depends_on:
{controller_depends_on}
            - aggregator_metrics
            - aggregator_top_and_bottom
            - aggregator_top_10
            - aggregator_top_5
            - aggregator_movies
            - joiner_group_by_movie_id_credits
            - joiner_group_by_movie_id_ratings
            - report

"""

# CLIENT --------------------------------------------------------------------------------------------
docker_compose_txt += f"""
    client:
        container_name: client
        image: client:latest
        build:
            context: .
            dockerfile: src/client/Dockerfile
        networks:
            - tp1_net
        depends_on:
            - controller
        volumes:
            - ./src/client/datasets:/app/datasets
        entrypoint: /client
        command:
            - /app/datasets/movies.csv
            - /app/datasets/ratings.csv
            - /app/datasets/credits.csv
"""

# REPORT -------------------------------------------------------------------------------------------
docker_compose_txt += f"""
    report:
        build:
            context: .
            dockerfile: src/server/report/Dockerfile
        container_name: report
        depends_on:
            - rabbitmq
        networks:
            - tp1_net
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
