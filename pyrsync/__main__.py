import sys
from .backup import Backup


def main():
    try:
        Backup(settings_file=sys.argv[1], extra_arguments=sys.argv[2:])
    except:
        Backup(settings_file=sys.argv[1], extra_arguments=[])
if __name__ == '__main__':
    main()
