First get yourself setup with link:http://docs.python-guide.org/en/latest/dev/virtualenvs/[Virtual Env] so we don't break any other Python stuff you have on your machine. After you've got that installed let's setup an environment for our app:

----
$ virtualenv app
New python executable in cypher-app/bin/python
Installing setuptools, pip...done.
----

----
$ source cypher-app/bin/activate
----

The next step is to install the dependencies for the app:

----
(cypher-app)$ pip install -r requirements.txt
...
Successfully installed py2neo
Cleaning up...
----

And finally let's start up a Bottle web server:

