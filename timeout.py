# __author__ = 'tracedeng'

import gevent


def main():
    timeout = gevent.Timeout(2)
    timeout.start()
    try:
        gevent.sleep(2.1)
        print "hello"
    except gevent.timeout.Timeout:
        print "timeout"

#
# import gevent
#
# def beep(interval):
#     while True:
#         print("Beep %s" % interval)
#         gevent.sleep(interval)
#
# # for i in range(10):
# #     gevent.spawn(beep, i)
#
# beep(20)


def foo():
    gevent.Timeout(1).start()
    try:
        print(gevent.getcurrent())
        gevent.sleep(2)
    except gevent.Timeout as e:
        print("hell")
        print(e)

def foo2():
    data = None
    with gevent.Timeout(1, False):
        # data = mysock.makefile().readline()
        gevent.sleep(2)
        data = "abc"

    if data is None:
        print("...")  # 5 seconds passed without reading a line
    else:
        print("asd")



class TooLong(Exception):
    pass


def foo3():
    try:
        with gevent.Timeout(1, TooLong):
            gevent.sleep(2)
    except TooLong as e:
        print("i am here")

if '__main__' == __name__:
    # main()
    print(gevent.getcurrent())
    foo()
    # foo2()
    # foo3()