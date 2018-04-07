"""
Answer for question 8.

"""
import time
import random

def timer(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        print("Time taken for execution in millisecond", (te - ts) * 1000)
        return result
    return timed

@timer
def random_number_gen():
    for i in range(10):
        print(random.uniform(0, 1)*10)
        time.sleep(random.uniform(0, 1))

if __name__ == '__main__':
    random_number_gen()
