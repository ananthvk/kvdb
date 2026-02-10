import random
import string

def random_string(length=8):
    """Generate a random string of given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def main(count=10000):
    """
    Print random key=value pairs to stdout.
    
    count: total number of operations
    """
    for _ in range(count):
        # emit a put operation
        key = random_string(random.randint(3, 10))
        value = random_string(random.randint(5, 15))
        print(f"{key}={value}")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate random key=value pairs.")
    parser.add_argument("-n", "--num", type=int, default=10000, help="Total number of operations")
    args = parser.parse_args()

    main(args.num)