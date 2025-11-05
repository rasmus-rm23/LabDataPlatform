import json

def dummy_func(a):
    b = 2*a
    return b


if __name__ == "__main__":
    # Load config
    config_path = 'local_config.json'
    with open(config_path, "r") as f:
        config = json.load(f)

    print(config)
