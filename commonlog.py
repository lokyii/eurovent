import logging
import time
import os

class CommonLog:
    def log(self):
        # 创建日志器
        logger = logging.getLogger("logger")
        # 设置日志输出最低等级
        logger.setLevel(logging.DEBUG)
        if not logger.handlers:
            # 创建处理器
            sh = logging.StreamHandler()
            # 创建格式器
            # formatter = logging.Formatter(fmt="%(asctime)s %(filename)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %X")
            formatter = logging.Formatter(fmt="%(asctime)s %(filename)s %(levelname)s %(message)s",
                                          datefmt="%Y-%m-%d %X")
            sh.setFormatter(formatter)
            logger.addHandler(sh)

            # fh = logging.FileHandler(filename="../Log/{filename}_log_{time}.log".format(filename=os.path.split(__file__)[-1],
            #                         time=time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())), encoding="utf-8")

            fh = logging.FileHandler(filename="D:/Software/GitSpace/eurovent/Log/{filename}_log_{time}.log".format(filename=os.path.split(__file__)[-1],
                                    time=time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())), encoding="utf-8")
            fh.setFormatter(formatter)
            logger.addHandler(fh)

        return logger

# 将运行文件的文件名设置为filehandler的filename




