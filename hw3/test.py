import random
import time
import json
import subprocess

TOTAL_IDS = 3


def gen_rand_key():
    return str(random.randint(1, 5))


def gen_rand_val():
    return str(random.randint(1, 1000000000))


def gen_rand_host():
    return random.randint(1, TOTAL_IDS)


def gen_random_patch_curl():
    return [
        "curl",
        "-X", "PATCH",
        f"http://localhost:{8000 + gen_rand_host()}",
        "-H", "Content-Type: application/json",
        "-d", f"{json.dumps({gen_rand_key(): gen_rand_val()})}",
    ]


def gen_get_curl(id):
    return [
        "curl",
        "-H", "Accept: application/json",
        f"http://localhost:{8000 + id}",
    ]


def check_consistent():
    output = subprocess.check_output(gen_get_curl(1))
    for id in range(1, TOTAL_IDS + 1):
        assert subprocess.check_output(gen_get_curl(id)) == output


def spam_requests(cnt):
    for i in range(cnt):
        subprocess.Popen(gen_random_patch_curl())
        time.sleep(0.01)

spam_requests(10)
time.sleep(10)
check_consistent()


spam_requests(100)
time.sleep(100)
check_consistent()

for i in range(5):
    spam_requests(5)
    time.sleep(5)
    check_consistent()