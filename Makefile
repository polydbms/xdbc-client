build_dockerimage:
	docker build --no-cache -f Dockerfile -t xdbc-client:latest .
	#docker build -f Dockerfile -t xdbc-client:latest .
