import sys
import os

if __name__ == "__main__":
    sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

    import polling

    if len(sys.argv) > 1:
        polling.main(sys.argv[1])
    else:
        polling.main()
