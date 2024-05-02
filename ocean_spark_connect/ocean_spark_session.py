import multiprocessing
import os
from multiprocessing import Process

import grpc
from pyspark import SparkContext
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
            channel = grpc.insecure_channel(self.url)
        return channel


class OceanSparkSession(RemoteSparkSession):
    def __init__(self, connection: ChannelBuilder,
                 jspark: SparkSession | None,
                 my_process: Process | None,
                 bind_address: str):
        super().__init__(connection, None)
        self._jspark = jspark
        self._my_process = my_process
        self._bind_address = bind_address

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
        _jvm = False
        _channel_builder = False
        _scheme = "wss"
        _host = "api.spotinst.io"
        _port = "-1"
        _bind_address = "0.0.0.0"

        def __init__(self):
            super().__init__()

        def use_java(self, value):
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
            self._bind_address = value
            return self

        def host(self, value):
            self._host = value
            return self

        def scheme(self, value):
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
                    _jspark = SparkSession.builder.master("local[1]") \
                        .config("spark.jars.repositories",
                                "https://us-central1-maven.pkg.dev/ocean-spark/ocean-spark-adapters") \
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

                    channel_builder = OceanChannelBuilder(f"sc://localhost:{self._port}", self._bind_address)
                    return OceanSparkSession(connection=channel_builder, jspark=_jspark, bind_address=self._bind_address, my_process=None)
                else:
                    url = f"{self._scheme}://{self._host}/ocean/spark/cluster/{self._clusterId}/app/{self._appId}/connect?accountId={self._accountId}"
                    _proxy = Proxy(url, self._token, self._port, self._bind_address)
                    _process = Process(target=_proxy.inverse_websockify, args=())
                    _process.start()

                    channel_builder = OceanChannelBuilder(f"sc://localhost:{_proxy.port}", _proxy.addr)
                    return OceanSparkSession(connection=channel_builder, jspark=None, bind_address=_proxy.addr, my_process=_process)
