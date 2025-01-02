import logging
import os
import sys
import threading
from pathlib import Path


class LoggerClass:
    def __init__(self):
        self._log = None

    @property
    def log(self):
        if not self._log:
            self._log = logging.getLogger(name="debezium")
            self._log.setLevel(logging.INFO)
            if not self._log.hasHandlers():
                handler = logging.StreamHandler(sys.stdout)
                handler.setLevel(logging.INFO)
                formatter = logging.Formatter('%(asctime)s %(levelname)s  [%(module)s] (%(funcName)s) %(message)s')
                handler.setFormatter(formatter)
                self._log.addHandler(handler)
        return self._log


class Debezium(LoggerClass):

    def __init__(self, debezium_dir: str = None, conf_dir: str = None, java_home: str = None):
        super().__init__()
        if debezium_dir is None:
            self.debezium_server_dir: Path = Path(__file__).resolve().parent
        else:
            if not Path(debezium_dir).is_dir():
                raise Exception("Debezium Server directory '%s' not found" % debezium_dir)
            self.debezium_server_dir: Path = Path(debezium_dir)
            self.log.info("Setting Debezium dir to:%s" % self.debezium_server_dir.as_posix())

        if conf_dir is None:
            self.conf_dir = self.debezium_server_dir.joinpath("conf")
        else:
            if not Path(conf_dir).is_dir():
                raise Exception("Debezium conf directory '%s' not found" % conf_dir)
            self.conf_dir: Path = Path(conf_dir)
            self.log.info("Setting conf dir to:%s" % self.conf_dir.as_posix())

        if java_home:
            os.putenv("JAVA_HOME", java_home)
            os.environ["JAVA_HOME"] = java_home
            self.log.info("JAVA_HOME env variable set to %s" % java_home)

    def _jnius_config(self, *java_args):
        import jnius_config

        if jnius_config.vm_running:
            raise ValueError(
                "VM is already running, can't set classpath/options; VM started at %s" % jnius_config.vm_started_at)

        # NOTE this needs to be set before add_classpath
        jnius_config.add_options(*java_args)

        debezium_classpath: list = [
            self.debezium_server_dir.joinpath('*').as_posix(),
            self.debezium_server_dir.joinpath("lib/*").as_posix(),
            self.conf_dir.as_posix()]

        jnius_config.add_classpath(*debezium_classpath)
        self.log.info("VM Classpath: %s" % jnius_config.get_classpath())
        return jnius_config

    def _sanitize(self, jvm_option: str):
        """Sanitizes jvm argument like `my.property.secret=xyz` if it contains secret.
        >>> dbz = Debezium()
        >>> dbz._sanitize("source.pwd=pswd")
        'source.pwd=*****'
        >>> dbz._sanitize("source.password=pswd")
        'source.password=*****'
        >>> dbz._sanitize("source.secret=pswd")
        'source.secret=*****'
        """
        if any(x in jvm_option.lower() for x in ['pwd', 'password', 'secret', 'apikey', 'apitoken']):
            head, sep, tail = jvm_option.partition('=')
            return head + '=*****'
        else:
            return jvm_option

    # pylint: disable=no-name-in-module
    def run(self, *java_args: str):
        jnius_config = self._jnius_config(java_args)
        try:
            __jvm_options: list = [self._sanitize(p) for p in jnius_config.get_options()]
            self.log.info("Configured jvm options:%s" % __jvm_options)

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
