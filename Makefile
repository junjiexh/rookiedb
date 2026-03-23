.PHONY: compile cli server client test clean

# 编译项目
compile:
	mvn compile

# 启动命令行界面
cli:
	mvn compile exec:java -Dexec.mainClass="edu.berkeley.cs186.database.cli.CommandLineInterface"

# 启动服务端 (端口 18600)
server:
	mvn compile exec:java -Dexec.mainClass="edu.berkeley.cs186.database.cli.Server"

# 启动 Python 客户端连接服务端
client:
	python3 client.py

# 运行指定项目的测试, 用法: make test PROJ=3
test:
	mvn clean test -Dproj=$(PROJ)

# 清理构建产物
clean:
	mvn clean
