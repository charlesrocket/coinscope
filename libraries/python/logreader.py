import socket
import io
import libconf
from lib.contools import read_logs
from sys import argv

with io.open("/coinscope/netmine.cfg", encoding='utf-8') as f:
    cfg = libconf.load(f)
    logger_root = cfg.get("logger").get("root")
    log_all = log_net = logger_root + cfg.get("logger").get("clients").get("all")
    log_net = logger_root + cfg.get("logger").get("clients").get("bitcoin")
    log_bitcoin_msg = logger_root + cfg.get("logger").get("clients").get("bitcoin_msg")


targets = ['all', 'net', 'bitcoin_msg']

if 1 < len(argv) < 3:
    target_log = argv[1]

    if target_log == "all":
        target_log = log_all
    elif target_log == "net":
        target_log = log_net
    elif target_log == "bitcoin_msg":
        target_log = log_bitcoin_msg
    else:
        raise Exception("Invalid target. Available targets: " + str(targets))

    log_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM, 0)
    log_sock.connect(target_log)

    for log in read_logs(log_sock):
        print log


else:
    raise Exception("No target provided. Available targets: " + str(targets))
