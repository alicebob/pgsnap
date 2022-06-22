test: clean
	psql -q --no-psqlrc < docker-postgres.sql
	PGSNAPURL="postgres:///?sslmode=disable" go test

clean:
	rm -f *.txt
	

tidy:
	go mod tidy
