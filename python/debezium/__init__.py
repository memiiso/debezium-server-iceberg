import argparse
import jnius_config
import logging
import os
import sys
#####  loggger
import threading
from pathlib import Path

log = logging.getLogger(name="debezium")
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s  [%(module)s] (%(funcName)s) %(message)s')
handler.setFormatter(formatter)
log.addHandler(handler)


#####

class Debezium():

    def __init__(self, debezium_dir: str = None, conf_dir: str = None, java_home: str = None):
        if debezium_dir is None:
            self.debezium_server_dir: Path = Path(__file__).resolve().parent
        else:
            if not Path(debezium_dir).is_dir():
                raise Exception("Debezium Server directory '%s' not found" % debezium_dir)
            self.debezium_server_dir: Path = Path(debezium_dir)
            log.info("Setting Debezium dir to:%s" % self.debezium_server_dir.as_posix())

        if conf_dir is None:
            self.conf_dir = self.debezium_server_dir.joinpath("conf")
        else:
            if not Path(conf_dir).is_dir():
                raise Exception("Debezium conf directory '%s' not found" % conf_dir)
            self.conf_dir: Path = Path(conf_dir)
            log.info("Setting conf dir to:%s" % self.conf_dir.as_posix())

        ##### jnius
        if java_home:
            self.java_home(java_home=java_home)

        DEBEZIUM_CLASSPATH: list = [
            self.debezium_server_dir.joinpath('*').as_posix(),
            self.debezium_server_dir.joinpath("lib/*").as_posix(),
            self.conf_dir.as_posix()]
        self.add_classpath(*DEBEZIUM_CLASSPATH)

    def add_classpath(self, *claspath):
        if jnius_config.vm_running:
            raise ValueError(
                "VM is already running, can't set classpath/options; VM started at %s" % jnius_config.vm_started_at)

        jnius_config.add_classpath(*claspath)
        log.info("VM Classpath: %s" % jnius_config.get_classpath())

    def java_home(self, java_home: str):
        if jnius_config.vm_running:
            raise ValueError("VM is already running, can't set java home; VM started at" + jnius_config.vm_started_at)

        os.putenv("JAVA_HOME", java_home)
        os.environ["JAVA_HOME"] = java_home
        log.info("JAVA_HOME set to %s" % java_home)

    # pylint: disable=no-name-in-module
    def run(self, *args: str):

        try:
            jnius_config.add_options(*args)
            log.info("Configured jvm options:%s" % jnius_config.get_options())

            from jnius import autoclass
            DebeziumServer = autoclass('io.debezium.server.Main')
            _dbz = DebeziumServer()
            return _dbz.main()
        finally:
            from jnius import detach
            detach()


class DebeziumRunAsyn(threading.Thread):
    def __init__(self, debezium_dir: str, java_args: list, java_home: str = None):
        threading.Thread.__init__(self)
        self.debezium_dir = debezium_dir
        self.java_args = java_args
        self.java_home = java_home
        self._dbz: Debezium = None

    def run(self):
        self._dbz = Debezium(debezium_dir=self.debezium_dir, java_home=self.java_home)
        return self._dbz.run(*self.java_args)


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
