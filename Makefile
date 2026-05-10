docker-run: docker-up
	curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @connectors/minio-sink.json 

docker-up:
	docker-compose up -d