# coding=utf-8
import sys, redis
sys.path.append("task")

__G_REDIS_IP__ = "localhost"

def main():
    r = redis.Redis(host=__G_REDIS_IP__, port=6379)
    r.set('foo', 'Bar')
    print r.get('foo')


if __name__ == "__main__":
    main()