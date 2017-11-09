# coding=utf-8

import threading, datetime, time

def LivePrint():
    now = datetime.datetime.now
    print "iam live @ " + str(now)
    t = threading.Timer(5, LivePrint)
    t.start()



def main():
    LivePrint()
    time.sleep(20)
    print "sleep over"
    pass


if __name__ == "__main__":
    main()

