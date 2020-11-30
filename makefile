archivo = $(GOPATH)/LAB1/logistica/logistica.go

run :
	go run $(archivo)
	
clean:
	rm -f ./datanode0/chunks/*
	rm -f ./datanode1/chunks/*
	rm -f ./datanode2/chunks/*
	rm -f ./namenode/LOG.txt
	rm -f ./namenode/descargas/*
