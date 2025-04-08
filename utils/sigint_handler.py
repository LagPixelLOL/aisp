import sys
import signal

_SIGINT_COUNTER = 0

def get_sigint_count():
    return _SIGINT_COUNTER

def sigint_handler(signum, frame):
    global _SIGINT_COUNTER
    _SIGINT_COUNTER += 1
    print()
    if _SIGINT_COUNTER >= 3:
        print("Script force quit by user, exiting...")
        sys.exit(1)

def register_sigint_callback():
    signal.signal(signal.SIGINT, sigint_handler)
