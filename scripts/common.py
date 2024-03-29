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
        LOCAL_WEBDRIVER_PATH = "/home/ec2-user/chromedriver"
    else:
        # WINDOWS
        LOCAL_LOCUS_PATH = "C:/Users/jsbmm/Dropbox/locus/"
        LOCAL_WEBDRIVER_PATH = "C:/Program Files/chromedriver.exe"
    
    S3_PATH_NAME = "locus-data"
    S3_PATH = f"s3://{S3_PATH_NAME}/"

    """
    sudo wget https://chromedriver.storage.googleapis.com/95.0.4638.69/chromedriver_linux64.zip
    sudo unzip chromedriver_linux64.zip
    rm -rf chromedriver_linux64.zip
    sudo mv chromedriver /home/ec2-user/chromedriver

    curl https://intoli.com/install-google-chrome.sh | bash
    sudo mv /usr/bin/google-chrome-stable /usr/bin/google-chrome
    google-chrome --version && which google-chrome
    """