#!/usr/bin/env bash

sudo apt update
sudo apt install software-properties-common

sudo add-apt-repository ppa:deadsnakes/ppa

sudo apt install python3.9 -y

sudo apt install python3-pip -y

sudo apt-get install -yq git supervisor


apt-get install chromium -y
sudo apt install curl unzip -y
sudo apt install wget -y
FILE=chromedriver_linux64.zip


[[ -f $FILE ]] || wget https://chromedriver.storage.googleapis.com/104.0.5112.79/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver

python3.9 -m pip install pip

# Account to own server process
useradd -m -d /home/pythonapp pythonapp

[[ -d opt/app ]] || mkdir opt/app

# Set ownership to newly created account
chown -R pythonapp:pythonapp /opt/app

cd opt/app/

[[ -d testshell ]] || git clone https://github.com/SantiagoCalvo/rsu_test.git

cd rsu_test/
git pull origin main

python3.9 -m pip install -r requirements.txt

# Put supervisor configuration in proper place
cp /opt/app/rsu_test/rsu-app.conf /etc/supervisor/conf.d/rsu-app.conf


# Start service via supervisorctl
supervisorctl reread
supervisorctl update
