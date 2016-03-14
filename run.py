# -*- coding: utf-8 -*-
__author__ = 'tracedeng'

import sys
wrapper_path = ["./coframe", "./wrapper", "./proto", "./proto/lv2", "./lv2", "./proto/branch"]
for path in wrapper_path:
    if path not in sys.path:
        print("add path %s", path)
        sys.path.append(path)

from coframe.server import Server
if __name__ == "__main__":
    server = Server()
    server.run()