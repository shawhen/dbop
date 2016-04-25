# docker run参数
## -d
需要使用-d参数指定container后台运行

## -v persist_path:/opt/platform/dbop/logs
需要使用-v为container指定/opt/platform/dbop/logs的persist存储路径

## 示一例
docker run -d -v /home/ops/game/dbop/logs:/opt/platform/dbop/logs dbop

# curl参数传递
参数使用POST传递

## 初始化接口url
http://container_ip:49977/setup

### node
指定这个dbop示例提供哪类数据引擎存储功能，目前有redis/mysql

### db_host
实际数据存储引擎服务地址

### db_port
实际数据存储引擎服务端口

### listen_port
这个dbop实例服务的监听端口

### 示一例
curl -X POST --data "node=redis" --data "db_host=redis_container_ip" --data "db_port=6379" --data "listen_port=32001" http://container_ip:49977/setup

