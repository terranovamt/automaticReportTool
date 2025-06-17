import sys
import os

if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

    import pollingv3 as polling

    polling.main()

