# Permet d'installer kafka
```bash
 docker run -d -p 9092:9092 --name broker apache/kafka:latest
```
```bash
 docker exec -it broker bash
 ```

```bash
cd /opt/kafka
```

https://hub.docker.com/r/apache/kafka

# Creation d'un environnement virtuel
```bash
python -m venv venv
```
```bash 
pip install -r requirements.txt
```
```bash
source venv/bin/activate
python script.py
```