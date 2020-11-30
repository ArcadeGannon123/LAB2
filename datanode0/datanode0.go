package main

import (
		"fmt"
		"context"
		"strconv"
		"os"
		"LAB2/proto"
		"io/ioutil"
		"google.golang.org/grpc"
		"google.golang.org/grpc/reflection"
		"net"
		"math"
		"math/rand"
		"time"
		"bufio"
		"sync"
		"strings"
)

type server struct{}

type DatosRegistro struct{

	N_chunks int32
	Name string
	Propuesta []int32

}


const PUERTO = ":3030"
const DATANODE0 = "localhost:3030"
const DATANODE1 = "localhost:3040"
const DATANODE2 = "localhost:3050"
const NAMENODE = "localhost:4040"
const fileChunk = 250000 // 250 KB, change this to your requirement

var (
	mutex sync.Mutex
	mutex2 sync.Mutex

	ID_Actual int32 = 0//ID de la maquina actual
	EXCLUSION string //variable que indica el tipo de exclusion mutua que hay entre los nodos
	state string = "RELEASED" //Variable de estado del algoritmo de distribucion
	DATANODE []string = []string{DATANODE0,DATANODE1,DATANODE2}//direcciones ip con puerto de cada datanodo
	EN_ESPERA []DatosRegistro//cola con los datos que quedaron en espera para ser registrador en el namenode
	TIMESTAMP int64//reloj de la maquina actual

)

func enqueue(DATOS DatosRegistro){//encola los datos para el registro en el namenode
	mutex.Lock()
	defer mutex.Unlock()
	EN_ESPERA = append(EN_ESPERA, DATOS)
	
}

func dequeue(){
	mutex2.Lock()
	defer mutex2.Unlock()
	EN_ESPERA = EN_ESPERA[1:]

}

//Esta funcion se encarga de enviar los chunks hacia algun datanode
func enviar_chunks(DATOS [][]byte, DESTINO string, name string, numero int32) string{

	ERROR := "OK"
	conn, err := grpc.Dial(DESTINO, grpc.WithInsecure())

	if err == nil{
		
		client := proto.NewDataNodeServiceClient(conn)
		_, err := client.RecibirChunks(context.Background(), &proto.ChunkDatanode{Name: name, Number: numero, Data: DATOS})
		if err != nil{
			ERROR = "No se pudo concretar la accion"
		}
	}else{
		ERROR = "No se pudo concretar la accion"

	}
	defer conn.Close()
	

	return ERROR


}

//Esta funcion se encarga de entregar al cliente los chunks que solicita
func (s *server) Download(ctx context.Context, Datos *proto.ChunkName) (*proto.Descarga, error){

	
	Name := Datos.GetName()

	currentChunkFileName := "./chunks/"+Name

	newFileChunk, err := os.Open(currentChunkFileName)

	chunkBufferBytes := make([]byte, 0)

	if err != nil {
			fmt.Println("NO EXISTE EL ARCHIVO")
			fmt.Println(err)
	}else{

		

		chunkInfo, err := newFileChunk.Stat()
	
		if err != nil {
				fmt.Println(err)
				os.Exit(1)
		}
	
		var chunkSize int64 = chunkInfo.Size()
		chunkBufferBytes = make([]byte, chunkSize)
	
	
		reader := bufio.NewReader(newFileChunk)
		_, err = reader.Read(chunkBufferBytes)
	
		if err != nil {
				fmt.Println(err)
				os.Exit(1)
		}


	}

	defer newFileChunk.Close()

	return &proto.Descarga{Data:chunkBufferBytes},nil
}



//Esta funcion recibe los chunks que le corresponde al datanode actual almacenar
func (s *server) RecibirChunks(ctx context.Context, Datos *proto.ChunkDatanode) (*proto.Empty, error){
	
	DATOS:=Datos.GetData()
	Name:=Datos.GetName()
	number := int(Datos.GetNumber())

	
	k:=1
	for i := uint64(0); i < uint64(len(DATOS)); i++ {
		
		fileName := "./chunks/"+Name +"_"+ strconv.Itoa(number+k)
		_, err := os.Create(fileName)

		if err != nil {
				fmt.Println(err)
				os.Exit(1)
		}

		
		ioutil.WriteFile(fileName, DATOS[i], os.ModeAppend)

		k=k+1
	}
	
	return &proto.Empty{},nil

}

//Esta funcion se encarga de registrar la propuesta de distribucion en el namenode
func registrar_propuesta(NAMENODE string, N_chunks int32, Name string, propuesta []int32) {

	N_nodos := int32(len(propuesta))

	division := int32(math.Round(float64(N_chunks)/float64(N_nodos)))
	direccion := make([]string,N_chunks)
	distribucion := make([]int32,N_nodos)

	for i:=int32(0); i<N_nodos-1; i++{
		distribucion[i]=division
	}
	distribucion[N_nodos-1]=division + (N_chunks - N_nodos*division)

	k := 0
	for i:=int32(0); i<N_nodos; i++{

		for j:=int32(0); j<distribucion[i]; j++{
			direccion[k] = Name+"_"+strconv.Itoa(k+1)+" "+DATANODE[propuesta[i]]
			k = k+1
		}
	}


	conn, err := grpc.Dial(NAMENODE, grpc.WithInsecure())

	if err != nil{
		panic(err)
	}
	defer conn.Close()

	client := proto.NewNameNodeServiceClient(conn)
	_, err = client.RegistrarCentralizado(context.Background(), &proto.Registro{Name: Name,Nchunks: N_chunks,Chunks: direccion})
	if err != nil{
		panic(err)
	}
}

//Esta funcion se encarga de distribuir los chunks hacia los datanodes de la propuesta
func enviar_chunks_datanode(DATOS [][]byte, Propuesta []int32, Name string) string{

	ERROR := "OK"
	N_chunks := int32(len(DATOS))
	N_nodos := int32(len(Propuesta))



	division := int32(math.Round(float64(N_chunks)/float64(N_nodos)))
	distribucion := make([]int32,N_nodos)

	for i:=int32(0); i<N_nodos-1; i++{
		distribucion[i]=division
	}
	distribucion[N_nodos-1]=division + (N_chunks - N_nodos*division)


	for i:=int32(0); i<N_nodos -1; i++{

		Respuesta:=enviar_chunks(DATOS[i*division:(i+1)*division], DATANODE[Propuesta[i]], Name, i*division)
		
		if Respuesta!= "OK"{
			
			ERROR = "No se pudo concretar la accion"
			
		}

	}
	
	Respuesta:=enviar_chunks(DATOS[(N_nodos -1)*division:], DATANODE[Propuesta[N_nodos -1]],Name,(N_nodos -1)*division)

	if Respuesta != "OK"{
			
		ERROR = "No se pudo concretar la accion"
		
	}


	return ERROR

	
}


//=========================================================================================================//
//CENTRALIZADO=============================================================================================//
//=========================================================================================================//


//En esta funcion el namenode le avisa al datanode que puede acceder al registro
func (s *server) Turno(ctx context.Context, Datos *proto.ReplyNN) (*proto.Empty, error){

	registrar_propuesta(NAMENODE, EN_ESPERA[0].N_chunks, EN_ESPERA[0].Name, EN_ESPERA[0].Propuesta)
	dequeue()
	return &proto.Empty{},nil
}

//En esta funcion el datanode solicita al namenode el acceso al registro
func solicitar_Acceso() string{
	
	conn, err := grpc.Dial(NAMENODE, grpc.WithInsecure())
	if err != nil{
		panic(err)
	}
	defer conn.Close()

	client := proto.NewNameNodeServiceClient(conn)
	response, err := client.SolicitarAcceso(context.Background(), &proto.Solicitud{IDNodo: ID_Actual})
	if err != nil{
		panic(err)
	}

	return response.Respuesta
}

//En esta funcion el datanode genera una propuesta de distribucion y se la envia al namenode
func generar_propuesta_centralizado(N_chunks int32) []int32{

	a := []int32{0,1,2}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })

	ID_DESTINOS := a[:int(math.Min(3,float64(N_chunks)))]

	ID_DESTINOS = enviar_propuesta_centralizado(N_chunks, ID_DESTINOS)

	return ID_DESTINOS
}

//Esta funcion se encarga enviar la propuesta de distribucion al namenode
func enviar_propuesta_centralizado(chunks int32, propuesta []int32) []int32{

	conn, err := grpc.Dial(NAMENODE, grpc.WithInsecure())
	if err != nil{
		panic(err)
	}
	defer conn.Close()

	client := proto.NewNameNodeServiceClient(conn)
	response, err := client.RecibirPropuesta(context.Background(), &proto.PropuestaNamenode{Nchunks: chunks,IDdatanode: propuesta})
	if err != nil{
		panic(err)
	}
	

	return response.IDdatanode
}




//=========================================================================================================//
//DISTRIBUIDO==============================================================================================//
//=========================================================================================================//


//Esta funcion recibe una propuesta de distribucion de algun otro datanode y la acepta
//El criterio de aceptacion es solo poder realizar la conexion
func (s *server) RecibirPropuesta(ctx context.Context, PROP *proto.Propuesta) (*proto.Response, error){

	return &proto.Response{Respuesta:"OK"},nil
}


//Esta funcion se encarga de comunicarse con los datanodes que participen de la propuesta de distribucion
//EL criterio de aceptacion es que se pueda realizar la conexion
func enviar_propuesta_distribuido(COM_DATANODE string) string{

	ERROR := "OK"

	conn, err := grpc.Dial(COM_DATANODE, grpc.WithInsecure())
	
	if err == nil{
		
		client := proto.NewDataNodeServiceClient(conn)
		_, err := client.RecibirPropuesta(context.Background(), &proto.Propuesta{})

		if err != nil{
			ERROR = "No se pudo concretar la accion"
		}

	}else{
		ERROR = "No se pudo concretar la accion"

	}
	defer conn.Close()

	return ERROR
}

//En esta funcion se crea la propuesta de distribucion
func generar_propuesta_distribuido(N_chunks int32) []int32{

	a := []int32{0,1,2}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })

	ID_DESTINOS := a
	N_necesario := int32(math.Min(float64(N_chunks),float64(len(ID_DESTINOS))))

	
	for{
		N_confirmado := int32(0)
		N_nodos := int32(len(ID_DESTINOS))
		for i:=int32(0); i<N_necesario; i++{
	
			Respuesta:=enviar_propuesta_distribuido(DATANODE[ID_DESTINOS[i]])
	
			if Respuesta == "OK"{

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

//Esta funcion corresponde al algoritmo de exclusion mutua distribuida de ricart y agrawala
func (s *server) PermitirAcceso(ctx context.Context, solicitud *proto.SolicitudDistribuido) (*proto.RespuestaDistribuido, error){

	TS:=solicitud.GetTimestamp()
	id:=solicitud.GetIDSolicitante()

	BOOL := TIMESTAMP < TS

	if TIMESTAMP==TS{//si los timestamp son iguales, se desempata con los id de los procesos

		BOOL=ID_Actual<id

	}

	if id!= ID_Actual{

		if state=="HELD" || (state == "WANTED" && BOOL){

			for {//se queda esperando hasta que el proceso actual deje de ocupar el recurso
				time.Sleep(1*time.Millisecond)
				if state == "RELEASED"{
					break
				}
			}

		}


	}

	return &proto.RespuestaDistribuido{Respuesta:"OK"},nil
}

//Esta funcion se encarga de enviar la solicitud de acceso al recurso a los otros datanodes
func enviar_solicitud_distribuido(COM_DATANODE string, TS int64, id int32) string{

	RESPUESTA := "OK"
	conn, err := grpc.Dial(COM_DATANODE, grpc.WithInsecure())
	defer conn.Close()

	if err == nil{
		
		client := proto.NewDataNodeServiceClient(conn)
		_, err := client.PermitirAcceso(context.Background(), &proto.SolicitudDistribuido{Timestamp: TS, IDSolicitante: id})
		

		if err != nil{
			RESPUESTA = "OK"
		}

	}else{
		RESPUESTA = "OK"

	}

	return RESPUESTA
}

//esta funcion se encarga de enviar la solicitud de acceso al recurso hacia todos los nodos
//se queda esperando hasta que todos le respondan
func solicitar_acceso_distribuido(){

	TIMESTAMP = time.Now().Unix()

	_=enviar_solicitud_distribuido(DATANODE[0],TIMESTAMP,ID_Actual)

	_=enviar_solicitud_distribuido(DATANODE[1],TIMESTAMP,ID_Actual)

	_=enviar_solicitud_distribuido(DATANODE[2],TIMESTAMP,ID_Actual)

}



//=========================================================================================================//
//=========================================================================================================//
//=========================================================================================================//



//Esta funcion se encarga de recibir los chunks del cliente Uploader
func (s *server) Upload(ctx context.Context, DATOS *proto.Chunk) (*proto.Response, error){

	ERROR := "OK"
	N_chunks := int32(len(DATOS.GetData()))

	if EXCLUSION == "DISTRIBUIDO"{//Si la maquina opera con exclusion distribuida

		//StartTime := time.Now() 
		Propuesta:=generar_propuesta_distribuido(N_chunks)//Se crea una propuesta
		state = "WANTED"//Se cambia el estado de la maquina a WANTED
		solicitar_acceso_distribuido()//Se solicita el acceso al recurso a todos los datanodes
		state = "HELD"//Cuando se obtenda el acceso se cambia el estado a HELD
		
		registrar_propuesta(NAMENODE, N_chunks, DATOS.GetName(), Propuesta)//Se registra la distribucion en el namenode

		state = "RELEASED"//Se libera el recurso
		//Duracion := time.Since(StartTime)
		//fmt.Println("Tiempo de escritura en el LOG (Distribuido): ",Duracion)
		ERROR = enviar_chunks_datanode(DATOS.GetData(),Propuesta,DATOS.GetName())//Se envia los chunks a los datanodes de la propuesta
		
		
		

	}else{//Si la maquina opera con exclusion centralizada
		//StartTime := time.Now() 
		Propuesta:=generar_propuesta_centralizado(N_chunks)//Se crea una propuesta
		Respuesta := solicitar_Acceso()//Se pide al namenode acceso al recurso

		if Respuesta == "OK"{//Si la respuesta es positiva se procede a registrar la distribucion

			registrar_propuesta(NAMENODE, N_chunks, DATOS.GetName(), Propuesta)
			

		}else{//Si el recurso esta siendo ocupado se encola la peticion

			enqueue(DatosRegistro{N_chunks: N_chunks, Name: DATOS.GetName(), Propuesta: Propuesta})


		}
		//Duracion := time.Since(StartTime)
		//fmt.Println("Tiempo de escritura en el LOG (Centralizado): ",Duracion)
		ERROR = enviar_chunks_datanode(DATOS.GetData(),Propuesta,DATOS.GetName())//Se envia los chunks a los datanodes de la propuesta
		
	}

	

	return &proto.Response{Respuesta: ERROR},nil

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




func INICIO(){//Prepara los archivos y variables necesarios para el funcionamiento del programa

	var seleccion int32
	TIMESTAMP = time.Now().Unix()
	fmt.Println("Seleccionar tipo de exclusion mutua:")
	fmt.Println("1.-Centralizado")
	fmt.Println("2.-Distribuido")
	fmt.Scan(&seleccion)
	if seleccion ==1{
		EXCLUSION="CENTRALIZADO"
	}else{
		EXCLUSION="DISTRIBUIDO"
	}
	fmt.Println("============================================")

}
func main(){

	listener, err :=net.Listen("tcp",PUERTO)
	
	if err != nil{
		panic(err)
	}
	srv := grpc.NewServer()
	
	proto.RegisterDataNodeServiceServer(srv, &server{})
	reflection.Register(srv)

	INICIO()
	go CLIENTE()

	if e := srv.Serve(listener); e!= nil{
		panic(e)
	}
	
	


}