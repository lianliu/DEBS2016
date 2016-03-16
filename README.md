First get yourself setup with link: [Virtual Env] so we don't break any other Python stuff you have on your machine. After you've got that installed let's setup an environment for our app:

```sh
$ virtualenv app
New python executable in cypher-app/bin/python
Installing setuptools, pip...done.
```

```sh
$ source cypher-app/bin/activate
```

The next step is to install the dependencies for the app:

```sh
(cypher-app)$ pip install -r requirements.txt
...
Successfully installed py2neo
Cleaning up...
```
[Virtual Env]: <http://docs.python-guide.org/en/latest/dev/virtualenvs/>
