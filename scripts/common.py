from sys import platform
import os

class DirectoryFields:
    
    if platform == "darwin":
        # MAC OS X
        LOCAL_LOCUS_PATH = f"{os.path.expanduser('~')}/Dropbox/locus/"
        LOCAL_WEBDRIVER_PATH = "/usr/local/bin/chromedriver"
    elif platform == "linux":
        # LINUX
        LOCAL_LOCUS_PATH = "/home/ec2-user/locus/"
        LOCAL_WEBDRIVER_PATH = "/home/ubuntu/chromedriver"
    else:
        # WINDOWS
        LOCAL_LOCUS_PATH = "C:/Users/jsbmm/Dropbox/locus/"
        LOCAL_WEBDRIVER_PATH = "C:/Program Files/chromedriver.exe"