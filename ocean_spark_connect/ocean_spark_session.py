import asyncio
import os
import sys
import subprocess
import threading
import traceback
from asyncio import AbstractEventLoop

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
from pyspark.sql import SparkSession as SparkSession
from ocean_spark_connect.inverse_websockify import Proxy


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


_loop: AbstractEventLoop = None
_my_thread: threading.Thread = None
_jspark: SparkSession = None

teststr = """
python -c "from pyspark.sql.types import DoubleType; from pyspark.sql.functions import rand; spark.range(10000).select(rand().alias('value')).toPandas()"
"""

class OceanSparkSession(RemoteSparkSession):
    def _run_gh_process(self, query: str):
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

                #if not line.startswith("from ") and not line.startswith("import "):
                #    if "spark." in line or line.startswith("spark-sql -e ") or line.startswith(
                #            "SELECT") or line.startswith("select"):
                print("attempt to parse: " + line)
            elif "? Select" in line or "Suggestion not readily available" in line:
                break
 
        process.terminate()
        return cmd

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
                #exec(pref)
                try:
                    sparkcmd = "self." + suff[6:]
                    #df = self.eval(sparkcmd)
                    df = eval(sparkcmd)
                    if type(df).__name__ == 'DataFrame':
                        return df
                    else:
                        raise Exception("The result of the command is not a pyspark dataframe")
                except Exception as e:
                    _, _, tb = sys.exc_info()
                    traceback.print_tb(tb) # Fixed format
                    tb_info = traceback.extract_tb(tb)
                    filename, line, func, text = tb_info[-1]
                
                    print('An error occurred on line {} in statement {}'.format(line, text))
                    exit(1)
                    #print(f"error running command {cmd}: {e}")
                    #raise e
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
        cmd = teststr.strip() #self._run_gh_process(query)
        if cmd != "":
            return self.parseCmd(cmd)
        else:
            raise Exception("Empty ai suggestion")

    def stop(self):
        try:
            super().stop()
        finally:
            if _loop is not None:
                _loop.stop()
            if _my_thread is not None:
                _my_thread.join()
            if _jspark is not None:
                _jspark.stop()

    class Builder(RemoteSparkSession.Builder):
        _proxy: Proxy = None
        _token: str = None
        _profile: str = None
        _appId: str = None
        _accountId = None
        _clusterId = None
        _jvm = False
        _scheme = "wss"
        _host = "api.spotinst.io"
        _port = "15002"
        _bindAddress = "0.0.0.0"

        def __init__(self):
            super().__init__()

        def usejava(self, value):
            self._jvm = value
            return self

        def appid(self, value):
            self._appId = value
            return self

        def token(self, value):
            self._token = value
            return self

        def profile(self, value):
            self._profile = value
            return self

        def cluster_id(self, value):
            self._clusterId = value
            return self

        def account_id(self, value):
            self._accountId = value
            return self

        def port(self, value):
            self._port = value
            return self

        def bind_address(self, value):
            self._bindAddress = value
            return self

        def host(self, value):
            self._host = value
            return self
        
        def scheme(self, value):
            self._scheme = value
            return self

        def inverse_websockify(self, url: str) -> None:
            global _loop
            _loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_loop)
            proxy = Proxy(url, self._token, self._port, self._bindAddress)
            _loop.run_until_complete(proxy.start())
            _loop.run_forever()

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
                    _jspark = SparkSession.builder.master("local[1]") \
                        .config("spark.jars.repositories", "https://us-central1-maven.pkg.dev/ocean-spark/ocean-spark-adapters") \
                        .config("spark.jars.packages", "com.netapp.spark:clientplugin:1.2.1") \
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
                else:
                    global _my_thread
                    if _my_thread is None:
                        url = f"{self._scheme}://{self._host}/ocean/spark/cluster/{self._clusterId}/app/{self._appId}/connect?accountId={self._accountId}"
                        _my_thread = threading.Thread(target=self.inverse_websockify, args=(url,))
                        _my_thread.start()

            url = f"sc://localhost:{self._port}"
            return OceanSparkSession(url)


if __name__ == "__main__":
    spark = None
    try:
        spark = OceanSparkSession.Builder() \
            .host("localhost:8091") \
            .scheme("ws") \
            .port("15003") \
            .cluster_id("osc-739db584") \
            .appid("spark-hivethrift-6420f-lucid") \
            .profile("default") \
            .getOrCreate()
        spark.sql("select random()").show()
        # spark = OceanSparkSession.Builder().getOrCreate()
        # spark.ai("create a dataframe of random gaussian distributed numbers using pyspark. the length of the new dataframe should be 10000").show()
        # spark.ai("from the jaffle schema, select all columns in the customers table and generate a random gender").show()
        # spark.ai("show me the names of the tables joined with their column names in the jaffle schema").show()
        # asyncio.all_tasks()
    finally:
        if spark is not None:
            spark.stop()
