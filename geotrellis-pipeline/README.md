# Instructions
1) Install the Python requirements
```sh
pip install -r requirements.txt
```

2) Install sbt

```sh
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
sudo apt-get update
sudo apt-get install sbt
```

3) Build the fat jar
``` sh
cd geotrellis
sbt assembly
```

4) Run the main.py script
```sh
cd ../
python main.py
```
