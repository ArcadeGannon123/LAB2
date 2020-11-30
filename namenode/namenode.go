package main

import (
	"fmt"
	"context"
	"strconv"
	"os"
	"LAB2/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"math/rand"
	"math"
	"io/ioutil"
    "log"
	"strings"
	"sync"
	"time"
	"bufio"
)

type server struct{}

const FILE = "LOG.txt"
const PUERTO = ":4040"
const DATANODE0 = "localhost:3030"
const DATANODE1 = "localhost:3040"
const DATANODE2 = "localhost:3050"
const NAMENODE = "localhost:4040"
const fileChunk = 250000 // 250 KB, change this to your requirement

var (

	mutex sync.Mutex
	DATANODE []string = []string{DATANODE0,//direcciones ip con puerto de cada datanodo
								 DATANODE1,
								 DATANODE2}
	ACCESO []int32 //cola de acceso para la exclusion mutua centralizada
	ESTADO="LIBRE" //variable que indica si el area critica esta siendo ocupada


)

//=========================================================================================================//
//NAMENODE=================================================================================================//
//=========================================================================================================//
func enqueue(ID int32){//encola la id de un datanode que quiera hacer uso del area critica

	ACCESO = append(ACCESO, ID)

}

func dequeue(){//desencola la primera id de la cola
	
	ACCESO = ACCESO[1:]

}

//Esta funcion se encarga de entregar la listo con los libros disponibles
func (s *server) SolicitarLista(ctx context.Context, solicitud *proto.Empty) (*proto.Lista, error){

	input, err := ioutil.ReadFile(FILE)
	if err != nil {
			log.Fatalln(err)
	}

	lines := strings.Split(string(input), "\n")
	Libro := make([]string,0)

	for _, line := range lines {
			
		temp := strings.Split(line, " ")
		_,err := strconv.Atoi(temp[len(temp)-1])

		if err==nil{
			Libro=append(Libro,temp[0])
		}
				
	}


	return &proto.Lista{Libros: Libro},nil
}

//Esta funcion entrega las ubicaciones de cada chunk de un libro solicitado
func (s *server) SolicitarUbicaciones(ctx context.Context, solicitud *proto.Libro) (*proto.Ubicaciones, error){

	input, err := ioutil.ReadFile(FILE)
	if err != nil {
			log.Fatalln(err)
	}

	lines := strings.Split(string(input), "\n")
	ubicacion := make([]string,0)

	for i, line := range lines {
			
		temp := strings.Split(line, " ")
		if temp[0] == solicitud.GetName(){
			N_chunks,_ := strconv.Atoi(temp[1])
			ubicacion=append(ubicacion,lines[i+1:i+1+N_chunks]...)
			break
		}
				
	}


	return &proto.Ubicaciones{Ubicacion: ubicacion},nil
}

//Cuando existan datanode en la cola de espera esta funcion les indica cuando es su turno para acceder a recurso
func comunicar_turno(){

	conn, err := grpc.Dial(DATANODE[ACCESO[0]], grpc.WithInsecure())
	dequeue()
	defer conn.Close()
	
	if err == nil{
		client := proto.NewDataNodeServiceClient(conn)
		_, _ = client.Turno(context.Background(), &proto.ReplyNN{Respuesta: "OK"})
	}
}

//Esta funcion se encarga de entregar el acceso a los datanodes que requieran usar el recurso
//si el recurso esta siendo ocupado, encola la peticion del datanode
func (s *server) SolicitarAcceso(ctx context.Context, solicitud *proto.Solicitud) (*proto.ReplyNN, error){
	
	mutex.Lock()
	defer mutex.Unlock()
	var respuesta string

	if ESTADO=="LIBRE"{

		respuesta = "OK"
		ESTADO = "OCUPADO"

	}else{
		respuesta = "En COLA"
		enqueue(solicitud.GetIDNodo())
	}
	
	return &proto.ReplyNN{Respuesta: respuesta},nil
}

//Registra los datos en el LOG
func registrar(Name string, N_chunks int32, Chunks []string){

	input, err := ioutil.ReadFile(FILE)
	if err != nil {
			log.Fatalln(err)
	}

	lines := strings.Split(string(input), "\n")
	FLAG := 1

	for _, line := range lines {
			
		temp := strings.Split(line, " ")
		if temp[0] == Name{
			FLAG = 0
			break
		} 			
	}

	if FLAG == 1{

		lines = append(lines, Name + " " + strconv.Itoa(int(N_chunks)))
		lines = append(lines, Chunks...)
		output := strings.Join(lines, "\n")
		err = ioutil.WriteFile(FILE, []byte(output), 0644)
		if err != nil {
				log.Fatalln(err)
		}
	}
}

//Los datanodes acceden a esta funcion cuando es su turno de usar el recurso
func (s *server) RegistrarCentralizado(ctx context.Context, DATA *proto.Registro) (*proto.Empty, error){

	registrar(DATA.GetName(), DATA.GetNchunks(), DATA.GetChunks())
	

	if len(ACCESO)!=0{
		comunicar_turno()
	}else{
		ESTADO = "LIBRE"

	}
	return &proto.Empty{},nil
}

//Esta funcion se encarga de verificar si un datanode esta en funcionamiento
func verificar_funcionamiento(DESTINO string)int32{

	ERROR := int32(0)
	conn, err := grpc.Dial(DESTINO, grpc.WithInsecure())
	
	if err == nil{
		
		client := proto.NewDataNodeServiceClient(conn)
		_, err := client.RecibirPropuesta(context.Background(), &proto.Propuesta{})

		if err != nil{
			ERROR=1
		}

	}else{

		ERROR = 1
	}
	defer conn.Close()
	

	return ERROR

}

//Si una propuesta de un datanode no es valida, esta funcion se encarga de crear una nueva
func generar_nueva_propuesta(N_chunk int32) []int32{

	a := []int32{0,1,2}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })

	ID_DESTINOS := a
	N_necesario := int32(math.Min(float64(N_chunk),float64(len(ID_DESTINOS))))

	
	for{
		N_confirmado := int32(0)
		N_nodos := int32(len(ID_DESTINOS))

		for i:=int32(0); i<N_necesario; i++{
	
			Respuesta := verificar_funcionamiento(DATANODE[ID_DESTINOS[i]])
	
			if Respuesta == 0{

				N_confirmado = N_confirmado + 1
			}else{

				ID_DESTINOS[i] = -1

			}
	
		}
		if N_confirmado == N_necesario{
			break
		}else{
			
			for i:=int32(0); i<N_nodos; i++{

				if ID_DESTINOS[i] == -1{
					ID_DESTINOS = append(ID_DESTINOS[:i],ID_DESTINOS[i+1:]...)
					i=-1
					N_nodos = N_nodos - 1
					N_necesario = N_necesario -1
				}
			}
		}
	}

	return ID_DESTINOS
}

//los datanodes envian sus propuestas de distribucion a esta funcion
//si la propuesta es valida, se envia la misma propuesta, si no, se envia una nueva
func (s *server) RecibirPropuesta(ctx context.Context, PROP *proto.PropuestaNamenode) (*proto.RespuestaNamenode, error){

	N_chunks := PROP.GetNchunks()
	ID_DESTINOS := PROP.GetIDdatanode()
	flag:=0

	for i:=0; i<len(ID_DESTINOS); i++{
	
		Respuesta:=verificar_funcionamiento(DATANODE[ID_DESTINOS[i]])

		if Respuesta!=0{
			flag = 1
			break
		}

	}

	if flag == 1{

		ID_DESTINOS=generar_nueva_propuesta(N_chunks)

	}

	return &proto.RespuestaNamenode{IDdatanode: ID_DESTINOS},nil

}


//=========================================================================================================//
//CLIENTE==================================================================================================//
//=========================================================================================================//


//Envia los chunks de algun archivo hacia un datanode
func Enviar(COM_DATANODE string, Name string, datos [][]byte) int32{
	ERROR:=int32(0)
	conn, err := grpc.Dial(COM_DATANODE, grpc.WithInsecure())
	if err == nil{
		client := proto.NewDataNodeServiceClient(conn)
		response, err := client.Upload(context.Background(), &proto.Chunk{Name: Name, Data: datos})
		if err == nil{
			fmt.Println(response.Respuesta)
		}else{
			fmt.Println("No se pudo conectar con el DataNode "+COM_DATANODE)
			fmt.Println(err)
			ERROR=1
		}
	}else{
		fmt.Println("No se pudo conectar con el DataNode "+COM_DATANODE)
		fmt.Println(err)
		ERROR=1
	}
	defer conn.Close()
	return ERROR
}


//Esta funcion se encarga de dividir el archivo en chunks para enviarlos a un datanode
func Subir_archivo(archivo string){

	file, err := os.Open("./libros/"+archivo)

	if err != nil {
			fmt.Println("Archivo invalido")
			fmt.Println(err)
	}else{

		fileInfo, _ := file.Stat()

		var fileSize int64 = fileInfo.Size()
	
		// calculate total number of parts the file will be chunked into
	
		totalPartsNum := uint64(math.Ceil(float64(fileSize) / float64(fileChunk)))
	
		book := make([][]byte, totalPartsNum)
		for i := uint64(0); i < totalPartsNum; i++ {
	
				partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
	
				partBuffer := make([]byte, partSize)
	
				file.Read(partBuffer)
	
				book[i] = partBuffer
	
	
				
		}
	
		a := []int32{0,1,2}
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
		for i:=0;i<4;i++{
	
			if i==3{
				fmt.Printf("No se puedo subir el archivo")		
			}else{
				ERROR:=Enviar(DATANODE[a[i]], strings.Split(archivo,".pdf")[0], book)
				if ERROR ==0{
					break
				}
			}	
		}


	}

	defer file.Close()

	
}

//Esta funcion se encarga de comunicarse con el namenode para que este le entregue las ubicaciones de cada chunk de algun archivo
func solicitar_ubicaciones(nombre string) []string{

	UBICACIONES := []string{}

	conn, err := grpc.Dial(NAMENODE, grpc.WithInsecure())
	if err != nil{
		fmt.Println("No se pudo conectar con el NameNode")
		fmt.Println(err)
	}else{

		client := proto.NewNameNodeServiceClient(conn)
		response, err := client.SolicitarUbicaciones(context.Background(), &proto.Libro{Name: nombre})
		if err != nil{
			fmt.Println("No se pudo conectar con el NameNode")
			fmt.Println(err)
		}else{

			UBICACIONES=response.Ubicacion
		}
	}
	defer conn.Close()

	return UBICACIONES
}

//esta funcion se encarga de descargar los chunks de algun archivo que esta distribuido entre los datanodes
func descargar_chunks(UBICACION string, NOMBRE string) []byte{

	CHUNK := []byte{}
	conn, err := grpc.Dial(UBICACION, grpc.WithInsecure())
	if err != nil{
		fmt.Println("No se pudo concretar la conexion "+UBICACION)
		fmt.Println(err)
	}else{
		client := proto.NewDataNodeServiceClient(conn)
		response, err := client.Download(context.Background(), &proto.ChunkName{Name: NOMBRE})
		if err != nil{
			fmt.Println("No se pudo concretar la conexion "+UBICACION)
			fmt.Println(err)
		}else{

			CHUNK=response.Data

		}
	}
	defer conn.Close()

	return CHUNK
}

//esta funcion se encarga de descargar el archivo solicitado
func Descargar(Archivo string){

	UBICACIONES:=solicitar_ubicaciones(Archivo)
	flag:=0

	if len(UBICACIONES)!=0{
		totalPartsNum := int32(len(UBICACIONES))

		newFileName := "./descargas/"+Archivo+".pdf"
		_, err := os.Create(newFileName)
	
		if err != nil {
				fmt.Println(err)
				os.Exit(1)
		}
	
		file, err := os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	
		if err != nil {
				fmt.Println(err)
				os.Exit(1)
		}
	
		for i := int32(0); i < totalPartsNum; i++ {
			UBICACION:=strings.Split(UBICACIONES[i]," ")

			chunkBufferBytes:=descargar_chunks(UBICACION[1],UBICACION[0])
			
			if len(chunkBufferBytes)!=0{
				_, err := file.Write(chunkBufferBytes)
		
				if err != nil {
						fmt.Println(err)
						os.Exit(1)
				}
			
				file.Sync()
			
				chunkBufferBytes = nil 

			}else{
				fmt.Println("Uno de los chunks no existe o no es accesible")
				flag=1
				break
			}
					
			
		}
		if flag!=1{
			fmt.Println("Descarga completa")
		}
		
	}else{
		fmt.Println("No existe el archivo solicitado")

	}
	

	
}

//Esta funcion se comunica con el namenode para que este le entregue una lista con los libros disponibles
func solicitar_lista() []string{

	LISTA := []string{}

	conn, err := grpc.Dial(NAMENODE, grpc.WithInsecure())
	if err != nil{
		fmt.Println("No se pudo conectar con el NameNode")
		fmt.Println(err)
	}else{
		client := proto.NewNameNodeServiceClient(conn)
		response, err := client.SolicitarLista(context.Background(), &proto.Empty{})
		if err != nil{
			fmt.Println("No se pudo conectar con el NameNode")
			fmt.Println(err)
		}else{
			LISTA=response.Libros
			
		}


	}
	defer conn.Close()

	

	return LISTA


}


//=========================================================================================================//
//=========================================================================================================//
//=========================================================================================================//

//Prepara los archivos y variables necesarios para el funcionamiento del programa

func DOWNLOADER(){
	var Seleccion int32

	for{
		fmt.Println("=============================================")
		fmt.Println("Seleccione la accion a realizar: ")
		fmt.Println("1.-Solicitar lista de libros")
		fmt.Println("2.-Descargar un libro")
		fmt.Println("3.-Salir")
		fmt.Scan(&Seleccion)
		if Seleccion ==1{
			fmt.Println("Lista========================================")
			for i,libro := range solicitar_lista(){
				fmt.Println(i+1,".-",libro)
	
			}
	
		}else if Seleccion == 2{
			for{
				fmt.Println("Escriba el nombre del archivo a descargar (Ejemplo: libro_1):")
				fmt.Println("-> x para cancelar: ")
				reader := bufio.NewReader(os.Stdin)
				code, _ := reader.ReadString('\n')
		
				if (code[:len(code)-1] == "x"){
					break
				}else{
					Descargar(code[:len(code)-1])
		
				}
				fmt.Println("=============================================")
			}
		}else{
			break
		}


	}
	
}

func UPLOADER(){
	var Seleccion int32
	for{
		fmt.Println("=============================================")
		fmt.Println("Seleccione la accion a realizar: ")
		fmt.Println("1.-Solicitar lista de libros")
		fmt.Println("2.-Subir un libro")
		fmt.Println("3.-Salir")
		fmt.Scan(&Seleccion)
		if Seleccion ==1{
			fmt.Println("Lista========================================")
			for i,libro := range solicitar_lista(){
				fmt.Println(i+1,".-",libro)

			}
			fmt.Println("=============================================")

		}else if Seleccion == 2{
			for{
				fmt.Println("Escriba el nombre del archivo a subir (Ejemplo: libro_1.pdf):")
				fmt.Println("-> x para cancelar: ")
				reader := bufio.NewReader(os.Stdin)
				code, _ := reader.ReadString('\n')

				if (code[:len(code)-1] == "x"){
					break
				}else{
					Subir_archivo(code[:len(code)-1])

				}
				fmt.Println("=============================================")
			}

		}else{
			break
		}
	}
}


func CLIENTE(){

	var Seleccion int32

	for{

		fmt.Println("Seleccionar tipo de cliente: ")
		fmt.Println("1.-Uploader")
		fmt.Println("2.-Downloader")
		fmt.Scan(&Seleccion)
		if Seleccion == 1{
			UPLOADER()

		}else{

			DOWNLOADER()

		}
		
	}


}


func INICIO(){

	file, err := os.Open(FILE)
	defer file.Close()
	if err!=nil{
		csvFile, err := os.Create(FILE)
		if err != nil{
			log.Fatalf("failed creating file: %s", err)
		}
		csvFile.Close()
	}

	
}



func main(){

	INICIO()
	listener, err :=net.Listen("tcp",PUERTO)
	
	if err != nil{
		panic(err)
	}
	srv := grpc.NewServer()
	
	proto.RegisterNameNodeServiceServer(srv, &server{})
	reflection.Register(srv)
	go CLIENTE()
	if e := srv.Serve(listener); e!= nil{
		panic(e)
	}

}