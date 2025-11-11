Run from three terminals:

go run ./server --id=n1 --addr=:5001 --peers=:5002,:5003

go run ./server --id=n2 --addr=:5002 --peers=:5001,:5003

go run ./server --id=n3 --addr=:5003 --peers=:5001,:5002


After running try typing req in one of ther terminals