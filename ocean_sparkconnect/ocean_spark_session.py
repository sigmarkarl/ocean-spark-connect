import multiprocessing
import os
import sys
import subprocess
import traceback
from multiprocessing import Process

import grpc
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.connect.client import ChannelBuilder
from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
from pyspark.sql import SparkSession as SparkSession
from ocean_spark_connect.inverse_websockify import Proxy


multiprocessing.set_start_method("fork", force=True)


def load_profiles():
    homedir = os.path.expanduser("~")
    creds = os.path.join(homedir, ".spotinst", "credentials")
    profile_map = {}
    if os.path.exists(creds):
        current_profile = None
        with open(creds, "r") as f:
            for line in f:
                if line.startswith("["):
                    profile = line[1:-2]
                    current_profile = {}
                    profile_map[profile] = current_profile
                elif "=" in line:
                    key, value = line.split("=")
                    current_profile[key.strip()] = value.strip()
    return profile_map


class OceanChannelBuilder(ChannelBuilder):
    def __init__(self, url: str, bind_address: str):
        super().__init__(url)
        self._bind_address = bind_address

    def toChannel(self) -> grpc.Channel:
        if self._bind_address.startswith("/"):
            channel = grpc.insecure_channel("unix://" + self._bind_address)
        else:
            channel = super().toChannel()
        return channel


def _run_gh_process(query: str):
    process = subprocess.Popen(["gh", "copilot", "suggest", "-t", "shell", query + ""], stdout=subprocess.PIPE)
    cmd = "select random()"
    while True:
        line = process.stdout.readline()
        if not line:
            break
        line = line.decode("utf-8").strip()
        print(line)
        if line.startswith("# Suggestion:"):
            line = process.stdout.readline()
            line = line.decode("utf-8").strip()
            while line == "":
                line = process.stdout.readline()
                line = line.decode("utf-8").strip()
            total = ""
            while line != "":
                total += line + "\n"
                line = process.stdout.readline()
                line = line.decode("utf-8").strip()

            cmd = total.strip()

            # if not line.startswith("from ") and not line.startswith("import "):
            #    if "spark." in line or line.startswith("spark-sql -e ") or line.startswith(
            #            "SELECT") or line.startswith("select"):
            print("attempt to parse: " + line)
        elif "? Select" in line or "Suggestion not readily available" in line:
            break

    process.terminate()
    return cmd


class OceanSparkSession(RemoteSparkSession):
    def __init__(self, connection: ChannelBuilder,
                 jspark: SparkSession | None,
                 my_process: Process | None,
                 bind_address: str):
        super().__init__(connection, None)
        self._jspark = jspark
        self._my_process = my_process
        self._bind_address = bind_address

    def parseCmd(self, cmd: str) -> DataFrame:
        if cmd.startswith("SELECT ") or cmd.startswith("select "):
            sql = str.join(" ", cmd.split("\n"))
            return self.sql(sql)
        elif cmd.startswith("python -c "):
            i = cmd.index("python -c ")
            cmd = cmd[i + 11:-1]
            return self.parseCmd(cmd)
        elif "spark." in cmd:
            if cmd.endswith(".toPandas()"):
                cmd = cmd[:-11]
            i = cmd.rindex("spark.")
            suff = cmd[i:].strip()
            pref = cmd[:i].strip()
            if pref != "":
                # exec(pref)
                try:
                    sparkcmd = "self." + suff[6:]
                    # df = self.eval(sparkcmd)
                    df = eval(sparkcmd)
                    if type(df).__name__ == 'DataFrame':
                        return df
                    else:
                        raise Exception("The result of the command is not a pyspark dataframe")
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    traceback.print_tb(tb)  # Fixed format
                    tb_info = traceback.extract_tb(tb)
                    filename, line, func, text = tb_info[-1]

                    print('An error occurred on line {} in statement {}'.format(line, text))
                    exit(1)
                    # print(f"error running command {cmd}: {e}")
                    # raise e
            cmd = "self." + suff[6:]
        elif cmd.startswith("spark-sql -e "):
            sql = cmd[14:-1]
            return self.sql(sql)

        print(f"about to run command {cmd}")
        try:
            df = eval(cmd)
            if type(df).__name__ == 'DataFrame':
                return df
            else:
                raise Exception("The result of the command is not a pyspark dataframe")
        except Exception as e:
            print(f"error running command {cmd}: {e}")
            raise e

    def ai(self, query: str) -> DataFrame:
        cmd = teststr.strip()  # self._run_gh_process(query)
        if cmd != "":
            return self.parseCmd(cmd)
        else:
            raise Exception("Empty ai suggestion")

    def stop(self):
        try:
            super().stop()
        finally:
            if self._my_process is not None:
                self._my_process.kill()
                self._my_process = None
            if self._bind_address.startswith("/"):
                os.unlink(self._bind_address)
            elif self._jspark is not None:
                self._jspark.stop()
                self._jspark = None

    class Builder(RemoteSparkSession.Builder):
        _token: str = None
        _profile: str = None
        _appId: str = None
        _accountId = None
        _clusterId = None
        _ping_interval: float = -1.0
        _jvm = False
        _channel_builder = False
        _scheme = "wss"
        _host = "api.spotinst.io"
        _port = -1
        _bind_address = "0.0.0.0"

        def __init__(self):
            super().__init__()

        def use_java(self, value: bool):
            self._jvm = value
            return self

        def ping_interval(self, value: float):
            self._ping_interval = value
            return self

        def appid(self, value: str):
            self._appId = value
            return self

        def token(self, value: str):
            self._token = value
            return self

        def profile(self, value: str):
            self._profile = value
            return self

        def cluster_id(self, value: str):
            self._clusterId = value
            return self

        def account_id(self, value: str):
            self._accountId = value
            return self

        def port(self, value: int):
            self._port = value
            return self

        def bind_address(self, value: str):
            self._bind_address = value
            return self

        def host(self, value: str):
            self._host = value
            return self

        def scheme(self, value: str):
            self._scheme = value
            return self

        def getOrCreate(self):
            profile_map = load_profiles()
            if self._appId is not None:
                if self._clusterId is None:
                    raise Exception("clusterId is required")

                if self._token is None:
                    if self._profile is None:
                        raise Exception("token or profile is required")
                    else:
                        if self._profile not in profile_map:
                            raise Exception(f"Profile {self._profile} not found")
                        self._token = profile_map[self._profile]["token"]

                if self._accountId is None:
                    if self._profile is None:
                        raise Exception("accountId or profile is required")
                    else:
                        if self._profile not in profile_map:
                            raise Exception(f"Profile {self._profile} not found")
                        self._accountId = profile_map[self._profile]["account"]

                if self._jvm:
                    if self._port == -1:
                        self._port = 15002

                    _jspark = SparkSession.builder.master("local[1]") \
                        .config("spark.jars.repositories",
                                "https://us-central1-maven.pkg.dev/ocean-spark/ocean-spark-adapters") \
                        .config("spark.jars.packages", "com.netapp.spark:clientplugin:1.4.1") \
                        .config("spark.jars.excludes", "org.glassfish:javax.el,log4j:log4j") \
                        .config("spark.plugins", "com.netapp.spark.SparkConnectWebsocketTranscodePlugin") \
                        .config("spark.code.submission.clusterId", f"{self._clusterId}") \
                        .config("spark.code.submission.accountId", f"{self._accountId}") \
                        .config("spark.code.submission.appId", f"{self._appId}") \
                        .config("spark.code.submission.token", f"{self._token}") \
                        .config("spark.code.submission.ports", f"{self._port}") \
                        .getOrCreate()
                    SparkContext._active_spark_context = None
                    SparkSession._instantiatedSession = None

                    channel_builder = OceanChannelBuilder(f"sc://localhost:{self._port}", self._bind_address)
                    return OceanSparkSession(connection=channel_builder, jspark=_jspark, bind_address=self._bind_address, my_process=None)
                else:
                    url = f"{self._scheme}://{self._host}/ocean/spark/cluster/{self._clusterId}/app/{self._appId}/connect?accountId={self._accountId}"
                    _proxy = Proxy(url, self._token, self._port, self._bind_address, self._ping_interval)
                    _process = Process(target=_proxy.inverse_websockify, args=())
                    _process.start()

                    channel_builder = OceanChannelBuilder(f"sc://localhost:{_proxy.port}", _proxy.addr)
                    return OceanSparkSession(connection=channel_builder, jspark=None, bind_address=_proxy.addr, my_process=_process)


if __name__ == "__main__":
    spark_0 = None
    try:
        spark_0 = OceanSparkSession.Builder() \
            .cluster_id("osc-739db584") \
            .appid("spark-connect-40982-foxes") \
            .profile("default") \
            .getOrCreate()

        spark_0.sql("select random()").show()
        # spark = OceanSparkSession.Builder().getOrCreate()
        # spark.ai("create a dataframe of random gaussian distributed numbers using pyspark. the length of the new dataframe should be 10000").show()
        # spark.ai("from the jaffle schema, select all columns in the customers table and generate a random gender").show()
        # spark.ai("show me the names of the tables joined with their column names in the jaffle schema").show()
        # asyncio.all_tasks()
    finally:
        if spark_0 is not None:
            spark_0.stop()
