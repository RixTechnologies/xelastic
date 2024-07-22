TODO - pytest does not run - eventually path is wrong, need to fix it
pip install pytest
pip install build

# Preparing

## Configure cerebro and create image

* docker run -d -p 9000:9000 -e CEREBRO_HOSTS=elasticsearch:9200 lmenezes/cerebro
* open the created container in VSCode (attach container) or docker desktop
* open files tab
* Find application.conf - /opt/cerebro/conf/aplication.conf
* Replace play and hosts, remove Authentification section:

```conf
play {
  # Cerebro port, by default it's 9000 (play's default)
  server.http.port = ${?CEREBRO_PORT}
  ws.ssl.loose.acceptAnyCertificate = true
}
hosts = [
  {
    host = "http://elasticsearch:9200"
    name = "local"
    headers-whitelist = [ "x-proxy-user", "x-proxy-roles", "X-Forwarded-For" ]
  }
]
```

* Save
* docker commit (container) cerebro-dev
* remove (container)

## Build and run elasticsearch

* docker build -t xelastic-dev .
* docker compose create
* start container xelastic-elasticsearch-1 (wait for healthy) (for testing)
* Start container xelastic-app-1, open terminal and enter bash

#Testing
##Testing Howto samples
* cd ..
* check howto samples: python howto/howto_templates.py
##Run unit tests
** cd tests
** pytest

#Deploying code
Update version number in setup.cfg

##Commit and push (VSCode)

##Build and deploy distributive
```
cd ..
python -m build
```
* TODO create tag and release locally??
* In github create new release, set tag (version), upload whl file form dist folder, save release.
* Update docker files in other projects to use new xelastic version

#Building and deploying documentation
* elasticsearch container not used here
* cd ..
* mkdocs serve -a 0.0.0.0:8000
* git pull --ff
* mkdocs gh-deploy
