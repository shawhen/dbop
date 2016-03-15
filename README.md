# dbop
a database operation stuff

# Deploy
## preprocessor
* DIR: project path

## external service dependencies
* [mysql]
* [redis]

## configuration
The main configuration file is `configs/db.py`, we need property set the `driver`, `host`, `port`, `user`, `passwd` and `__dbbase__`.

## precedure
### install project's packages
	DIR$ pip install -r requirements.txt
	
### supervisor setup
	[program:dbop_mysql]
	command=python3.5 $DIR/main.py 32002
	autostart=true
	autorestart=true
	redirect_stderr=true
	stdout_logfile = $DIR/logs/dbop_mysql.log
	
	[program:dbop_redis]
	command=python3.5 $DIR/main.py 32001
	autostart=true
	autorestart=true
	redirect_stderr=true
	stdout_logfile = $DIR/logs/dbop_redis.log

