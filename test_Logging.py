from my_logging import Logging

if __name__ == "__main__":
    log: Logging = Logging("x", False)
    print(log.IS_LOGGING)