import random
import string

def random_string(length=8):
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def main(count=10000, delete_ratio=0.05):
    """
    Print random key=value pairs to stdout.
    
    count: total number of operations (puts + deletes)
    delete_ratio: fraction of operations that are deletes (0.05 = 5%)
    """
    keys = []  # track keys for possible deletes

    for _ in range(count):
        if keys and random.random() < delete_ratio:
            # emit a delete operation
            key_to_delete = random.choice(keys)
            print(f"\\delete {key_to_delete}")
            keys.remove(key_to_delete)
        else:
            # emit a put operation
            key = random_string(random.randint(3, 10))
            value = random_string(random.randint(5, 15))
            print(f"{key}={value}")
            keys.append(key)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate random key=value pairs with optional deletes.")
    parser.add_argument("-n", "--num", type=int, default=10000, help="Total number of operations")
    parser.add_argument("--delete-ratio", type=float, default=0.05, help="Fraction of operations that are deletes")
    args = parser.parse_args()

    main(args.num, args.delete_ratio)