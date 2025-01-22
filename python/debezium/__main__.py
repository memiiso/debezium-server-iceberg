import argparse

from debezium import Debezium


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debezium_dir', type=str, default=None,
                        help='Directory of debezium server application')
    parser.add_argument('--conf_dir', type=str, default=None,
                        help='Directory of application.properties')
    parser.add_argument('--java_home', type=str, default=None,
                        help='JAVA_HOME directory')
    _args, args = parser.parse_known_args()
    ds = Debezium(debezium_dir=_args.debezium_dir, conf_dir=_args.conf_dir, java_home=_args.java_home)
    ds.run(*args)


if __name__ == '__main__':
    main()
