import sys
from jinja2 import Environment, FileSystemLoader
import configparser


def main():
    config = configparser.ConfigParser()
    config.read("config.ini")
    default_config = config["DEFAULT"]

    env = Environment(loader=FileSystemLoader("templates"))
    template = env.get_template("docker-compose-dev.yaml.jinja")
    output = template.render(
        filter_nodes_2000s=int(default_config["2000s_FILTER_NODES"]),
        ar_es_filter_nodes=int(default_config["AR_ES_FILTER_NODES"]),
        ar_filter_nodes=int(default_config["AR_FILTER_NODES"]),
        single_country_origin_filter_nodes=int(default_config["SINGLE_COUNTRY_ORIGIN_FILTER_NODES"]),
        nlp_nodes=int(default_config["NLP_NODES"]),
        shards=int(default_config["SHARDS"]),
        top_5_investors_filter_nodes=int(default_config["SHARDS"]),
        client_nodes=int(default_config["CLIENT_NODES"]),
        resilience_manager_nodes=int(default_config["RESILIENCE_MANAGER_NODES"]),
    )

    with open("docker-compose-dev.yaml", "w") as f:
        f.write(output)


if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: python3 compose-generator.py")
        sys.exit(1)

    main()
