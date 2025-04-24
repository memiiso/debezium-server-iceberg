# Python Runner for Debezium Server

It's possible to use python to run,operate debezium server

For convenience this project additionally provides Python scripts to automate the startup, shutdown, and configuration
of Debezium Server.
Using Python, you can do various Debezium Server operation and take programmatic, dynamic, debezium configuration.
example:

```commandline
pip install git+https://github.com/memiiso/debezium-server-iceberg.git@master#subdirectory=python
debezium
# running with custom arguments
debezium --debezium_dir=/my/debezium_server/dir/ --java_home=/my/java/homedir/
```

```python
from debezium import Debezium

d = Debezium(debezium_dir="/dbz/server/dir", java_home='/java/home/dir')
java_args = []
java_args.append("-Dquarkus.log.file.enable=true")
java_args.append("-Dquarkus.log.file.path=/logs/dbz_logfile.log")
d.run(*java_args)
```

```python
import os
from debezium import DebeziumRunAsyn

java_args = []
# using python we can dynamically influence debezium 
# by chaning its config within python
if my_custom_condition_check is True:
    # Option 1: set config using java arg
    java_args.append("-Dsnapshot.mode=always")
    # Option 2: set config using ENV variable
    os.environ["SNAPSHOT_MODE"] = "always"

java_args.append("-Dquarkus.log.file.enable=true")
java_args.append("-Dquarkus.log.file.path=/logs/dbz_logfile.log")
d = DebeziumRunAsyn(debezium_dir="/dbz/server/dir", java_home='/java/home/dir', java_args=java_args)
d.run()
d.join()
```