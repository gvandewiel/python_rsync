import sys
from .backup import Backup


def main():
    print('in main')
    args = sys.argv[1:]
    print('count of args :: {}'.format(len(args)))

    for arg in args:
        print('passed argument :: {}'.format(arg))
    
    try:
        Backup(settings_file=sys.argv[1], extra_arguments=sys.argv[2:])
    except:
        Backup(settings_file=sys.argv[1], extra_arguments=[])
if __name__ == '__main__':
    main()
