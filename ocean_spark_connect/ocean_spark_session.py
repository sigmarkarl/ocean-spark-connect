import asyncio
import os
import threading
from asyncio import AbstractEventLoop

from pyspark.sql.connect.session import SparkSession as RemoteSparkSession
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


class OceanSparkSession(RemoteSparkSession):
    def stop(self):
        if _loop is not None:
            _loop.stop()
        if _my_thread is not None:
            _my_thread.join()
        super().stop()
    
    class Builder(RemoteSparkSession.Builder):
        _proxy: Proxy = None
        _token: str = None
        _profile: str = None
        _appId: str = None
        _accountId = None
        _clusterId = None
        _host = "api.spotinst.io"
        _port = "15002"
        _bindAddress = "0.0.0.0"

        def __init__(self):
            super().__init__()

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

        def inverse_websockify(self, url: str, loop: AbstractEventLoop) -> None:
            proxy = Proxy(url, self._token, self._port, self._bindAddress)
            loop.run_until_complete(proxy.start())
            loop.run_forever()

        def getOrCreate(self):
            global _loop, _my_thread
            profile_map = load_profiles()
            if self._appId is None:
                raise Exception("appId is required")
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

            if _loop is None:
                url = f"wss://{self._host}/ocean/spark/cluster/{self._clusterId}/app/{self._appId}/connect?accountId={self._accountId}"
                _loop = asyncio.get_event_loop()
                _my_thread = threading.Thread(target=self.inverse_websockify, args=(url, _loop))
                _my_thread.start()

            url = f"sc://localhost:{self._port}"
            return OceanSparkSession(connectionString=url)


if __name__ == "__main__":
    spark = OceanSparkSession.Builder().cluster_id("osc-239fd6f0").appid("spark-connect-7cea8-havoc").profile("default").config("spark.jars.ivy", "/tmp").getOrCreate()
    spark.sql("select random()").show()
    spark.stop()