import requests
from commonlog import CommonLog
import time


class zhimaProxy:
    def __init__(self):
        # 获取IP的API
        self.get_url = "http://webapi.http.zhimacangku.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions="
        # baidu用作测试IP可用性
        self.test_url = "https://www.baidu.com/"
        # 创建日志实例
        self.log = CommonLog().log()

    # 获取IP后，测试其可用性，通过则返回IP
    def getIP(self):
        ip = requests.get(url=self.get_url).text.strip()
        proxies = {
            "http": "http://" + ip,
            "https": "http://" + ip
        }

        try:
            response = requests.get(url=self.test_url, proxies=proxies, timeout=3)
        except Exception as err:
            # self.log.error(f"{proxies}不可用：{err}")
            time.sleep(3)
            self.getIP()
        else:
            # self.log.info(f"{proxies}可用")
            return ip


# proxies = zhimaProxy().getIP()
# print(proxies)

