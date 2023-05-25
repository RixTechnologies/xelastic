#Preparing
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
##Commit and push (VSCode)

##Build and deploy distributive
```
cd ..
python -m build
```
* In github create new release, set tag (version), upload whl file form dist folder, save release.
* Update docker files in other projects to use new xelastic version

#Building and deploying documentation
* elasticsearch container not used here
* cd ..
* mkdocs serve -a 0.0.0.0:8000
* git pull (--f-only -rebase or -base ??)
* mkdocs gh-deploy
