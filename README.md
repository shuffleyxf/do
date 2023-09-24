## Do
Do is a simple library for retry. Some important tasks may fail, and Do allow you to easily retry these tasks.

## Supported Features
- Multiple Storage Methods
- Customize Task Name
- Multiple Ways to Stop Task
- Multiple Strategy to Retry Task

## Usage
```
import random
import time
import traceback

from do import do, start_do
import requests

from base import IntervalStrategy


@do
def guess_game(answer: int):
    guess = random.randint(0, 10)
    if guess != answer:
        raise Exception("wrong.")


@do(retry_strategy=IntervalStrategy(interval=10))
def upload_data(remote: str, data: dict):
    requests.post(remote, json=data, timeout=10)


if __name__ == '__main__':
    start_do(block=False)

    try:
        guess_game(5)
    except Exception:
        traceback.print_exc()

    try:
        upload_data('https://jsonplaceholder.typicode.com/posts', {'value': 123456})
    except Exception:
        traceback.print_exc()

    while True:
        time.sleep(100000)
```