// FS

#include <stdio.h>
#include <string.h> //strlen
#include <stdlib.h>
#include <errno.h>
#include <unistd.h> //close
#include <arpa/inet.h> //close
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros

#include <ctype.h>

#define TRUE 1
#define FALSE 0
#define PORT 4444

void iniciarConexion();
/*void insert();
 void select();*/
int separarPalabra(char*, char**, char**);
void realizarPeticion(char*);
void verificarPeticion(char*, char*);
int numeroDePeticion(char*);
void stringToUpperCase(char*, char**);
int separarEnVector(char**, char[][30], int);
int cantidadValidaParametros(char*, int);
int parametrosValidos(int, char*, int (*criterioTiposCorrectos)(char[][30], int));
int tiposCorrectos(char*, int,
		int (*criterioTiposCorrectos)(char[][30], int));
int esUnNumero(char* cadena);

/*
 typedef enum {
 CREATE, INSERT, DROP
 } OPERACION;
 */

int main(int argc, char *argv[]) {
	while (1) {
		char* mensaje = malloc(1000);
		do {
			fgets(mensaje, 1000, stdin);
		} while (!strcmp(mensaje, "\n"));
		realizarPeticion(mensaje);
		free(mensaje);
	}
	//iniciarConexion();
	return 0;
}

void realizarPeticion(char* mensaje) {
	char* peticion = malloc(1000);
	char* parametros = malloc(1000);
	//printf("???%p - ???%p\n", peticion, parametros);
	int seInsertaronParametros = separarPalabra(mensaje, &peticion,
			&parametros);
	if (seInsertaronParametros) {
		verificarPeticion(peticion, parametros);
	} else {
		printf("No se ingresaron parametros\n");
	}
	free(peticion);
	free(parametros);
}

//Las cadenas son especiales, ya que cuando paso (char*) paso el valor de la cadena, no su referencia.
//Para modificar cadenas se usa la doble referencia
int separarPalabra(char* mensaje, char** palabra, char** restoDelMensaje) {
	char* delimitador = malloc(4);
	//Es necesario liberar la memoria de la variable que sigue?
	char* loQueSigue;
	strcpy(delimitador, " \n");
	strcpy(*palabra, strtok(mensaje, delimitador));
	//En la siguiente llamada strtok espera NULL en lugar de mensaje para saber que tiene que seguir operando con el resto
	loQueSigue = strtok(NULL, "\0");
	if (loQueSigue != NULL) {
		strcpy(*restoDelMensaje, loQueSigue);
	} else {
		return 0;
		//strcpy(*restoDelMensaje, "VACIO");
	}
	free(delimitador);
	return 1;
	//free(loQueSigue);
}

//Separa los parametros e indica si la cantidad de los mismos es igual a la cantidad que se necesita
//Hay que arreglar que en lugar de que las palabras sean 30 fijo de tamaño sean dinamicos
int separarEnVector(char** parametros, char parametrosSeparados[][30],
		int cantidadDeElementos) {
	char* delimitador = malloc(5);
	//Para no modificar el valor de la variable parametros hago una copia
	char* copiaParametros = malloc(sizeof(*parametros));
	strcpy(delimitador, " \n");
	strcpy(copiaParametros,*parametros);
	int i = 0;
	//printf("PPP%s - %i\n", parametrosSeparados[1], &parametrosSeparados);
	//printf("\"%s\"", copiaParametros);
	//Es necesario liberar la memoria de la variable que sigue?
	char* token = strtok(copiaParametros, delimitador);
	while (token != NULL && i<cantidadDeElementos) {
		//printf("$$$token: %s - i: %i\n", token, i);
		//Los vectores de char* son de solo lectura por eso vector de vectores de char para sobreescribir
		strcpy(parametrosSeparados[i], token);
		token = strtok(NULL, delimitador);
		i++;
		//printf("###token: %s - i: %i\n", token, i);
	}
	//printf("\nAAA%s\n", parametrosSeparados[0]);
	free(delimitador);
	free(copiaParametros);
	//si la cantidad de parametros es igual a lo necesario
	//printf("%i - %s\n", i, token);
	return (i == cantidadDeElementos && token == NULL);
}

//Aca no se necesita la referencia solo el valor
void verificarPeticion(char* peticion, char* parametros) {
	int instruccion = numeroDePeticion(peticion);
	//printf("???%p - ???%p\n", peticion, parametros);
	//parametrosSeparados[cantidadParametros] = malloc(1000);
	switch (instruccion) {
	case 1:
		printf("Seleccionaste Insert\n");
		//Defino de que manera van a ser validos los parametros del insert y luego paso el puntero de dicha funcion
		//Los parametros son validos si el segundo y el cuarto son numeros
		int criterioInsert(char parametrosSeparados[][30], int cantidadDeParametrosNecesarios){
			//printf("%i",sizeof(parametrosSeparados)/30);
			if(cantidadDeParametrosNecesarios==4){
				return esUnNumero(parametrosSeparados[1]) && esUnNumero(parametrosSeparados[3]);
			}
			return esUnNumero(parametrosSeparados[1]);
		}
		//puede o no estar el timestamp
		if(parametrosValidos(4, parametros, (void *)criterioInsert) || parametrosValidos(3, parametros, (void *)criterioInsert))
			//printf("%p", parametros);
			printf("ESTOY HACIENDO INSERT\n");
		break;
	case 2:
		printf("Seleccionaste Create\n");
		/*if (validarCantidadParametros(&parametros, 4)) {
			printf("La cantidad de parametros es valida");
		} else {
			printf("La cantidad de parametros no es valida");
		}*/
		break;
	default:
		printf("Error operacion invalida\n");
	}
}

int parametrosValidos(int cantidadDeParametrosNecesarios, char* parametros, int (*criterioTiposCorrectos)(char[][30], int)) {
	//printf("%i - %i", cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios), tiposCorrectos(parametros, cantidadDeParametrosNecesarios, (void *)criterioTiposCorrectos));
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios) &&
			tiposCorrectos(parametros, cantidadDeParametrosNecesarios, (void *)criterioTiposCorrectos);
}

int tiposCorrectos(char* parametros, int cantidadDeParametrosNecesarios,
		int (*criterioTiposCorrectos)(char[][30], int)){
	char parametrosSeparados[cantidadDeParametrosNecesarios][30];
	//printf("T%i\n",sizeof(parametrosSeparados)/30);
	//parametrosSeparados[0] = "123";
	//printf("OOO%s - %p\n", parametrosSeparados[0], parametrosSeparados);
	separarEnVector(&parametros,
				parametrosSeparados, cantidadDeParametrosNecesarios);
	//printf("OOO%s - %p\n", parametrosSeparados[0], parametrosSeparados);
	return criterioTiposCorrectos(parametrosSeparados, cantidadDeParametrosNecesarios);
}



int cantidadValidaParametros(char* parametros, int cantidadDeParametrosNecesarios) {
	//printf("---%i", cantidadDeParametrosNecesarios);
	char parametrosSeparados[cantidadDeParametrosNecesarios][30];
	//strcpy(parametrosSeparados[0], "HHHHHA");
	//printf("ZZ%s - %i\n", parametrosSeparados[1], &parametrosSeparados);
	int cantidadParametrosValida = separarEnVector(&parametros, parametrosSeparados, cantidadDeParametrosNecesarios);
	//printf("!!!%i -- ", cantidadParametrosValida);
	//printf("!!!%s", parametros);
	if(!cantidadParametrosValida)
		//hay que arreglar esto para que en el caso de insert solo lo muestre si no se cumple con 4 ni con 3
		printf("La cantidad de parametros no es valida\n");
	return cantidadParametrosValida;
}

int numeroDePeticion(char* peticion) {
	char* peticionUpperCase = malloc(strlen(peticion));
	stringToUpperCase(peticion, &peticionUpperCase);
	//printf("%s\n", peticionUpperCase);
	if (!strcmp(peticionUpperCase, "INSERT")) {
		free(peticionUpperCase);
		return 1;
	} else {
		if (!strcmp(peticionUpperCase, "CREATE")) {
			free(peticionUpperCase);
			return 2;
		} else {
			free(peticionUpperCase);
			return -1;
		}
	}
}

int esUnNumero(char* cadena){
	for(int i=0; i<strlen(cadena); i++){
		if(!isdigit(cadena[i])){
			return 0;
		}
	}
	return 1;
}

void stringToUpperCase(char* palabra, char** palabraEnMayusculas) {
	char* aux = malloc(strlen(palabra));
	strcpy(aux, palabra);
	int i = 0;
	while (aux[i] != '\0') {
		aux[i] = toupper(aux[i]);
		i++;
	}
	//strcpy(*palabraEnMayusculas, aux);
	strcpy(*palabraEnMayusculas, aux);
	free(aux);
}

/*void insert(){

 }

 void select(){

 }*/

/*
 void iniciarConexion(){
 int opt = TRUE;
 int master_socket , addrlen , new_socket , client_socket[30] ,
 max_clients = 30 , activity, i , valread , sd;
 int max_sd;
 struct sockaddr_in address;

 char buffer[1025]; //data buffer of 1K

 //set of socket descriptors
 fd_set readfds;

 //a message
 char *message = "Este es el mensaje del server\r\n";

 //initialise all client_socket[] to 0 so not checked
 for (i = 0; i < max_clients; i++)
 {
 client_socket[i] = 0;
 }

 //create a master socket
 if( (master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0)
 {
 perror("socket failed");
 exit(EXIT_FAILURE);
 }

 //set master socket to allow multiple connections ,
 //this is just a good habit, it will work without this
 if( setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt,
 sizeof(opt)) < 0 )
 {
 perror("setsockopt");
 exit(EXIT_FAILURE);
 }

 //type of socket created
 address.sin_family = AF_INET;
 address.sin_addr.s_addr = INADDR_ANY;
 address.sin_port = htons( PORT );

 //bind the socket to localhost port 8888
 if (bind(master_socket, (struct sockaddr *)&address, sizeof(address))<0)
 {
 perror("bind failed en lfs");
 exit(EXIT_FAILURE);
 }
 printf("Listener on port %d \n", PORT);

 listen(master_socket, 100);

 //accept the incoming connection
 addrlen = sizeof(address);
 puts("Waiting for connections ...");

 while(TRUE)
 {
 //clear the socket set
 FD_ZERO(&readfds);

 //add master socket to set
 FD_SET(master_socket, &readfds);
 max_sd = master_socket;

 //add child sockets to set
 for ( i = 0 ; i < max_clients ; i++)
 {
 //socket descriptor
 sd = client_socket[i];

 //if valid socket descriptor then add to read list
 if(sd > 0)
 FD_SET( sd , &readfds);

 //highest file descriptor number, need it for the select function
 if(sd > max_sd)
 max_sd = sd;
 }

 //wait for an activity on one of the sockets , timeout is NULL ,
 //so wait indefinitely
 activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL);

 if ((activity < 0) && (errno!=EINTR))
 {
 printf("select error");
 }

 //If something happened on the master socket ,
 //then its an incoming connection
 if (FD_ISSET(master_socket, &readfds))
 {
 new_socket = accept(master_socket,(struct sockaddr *)&address, (socklen_t*)&addrlen);
 if (new_socket <0)
 {

 perror("accept");
 exit(EXIT_FAILURE);
 }

 //inform user of socket number - used in send and receive commands
 printf("New connection , socket fd is : %d , ip is : %s , port : %d 	\n" , new_socket , inet_ntoa(address.sin_addr) , ntohs
 (address.sin_port));

 //send new connection greeting message
 if( send(new_socket, message, strlen(message), 0) != strlen(message) )
 {
 perror("send");
 }

 puts("Welcome message sent successfully");

 //add new socket to array of sockets
 for (i = 0; i < max_clients; i++)
 {
 //if position is empty
 if( client_socket[i] == 0 )
 {
 client_socket[i] = new_socket;
 printf("Adding to list of sockets as %d\n" , i);

 break;
 }
 }
 }

 //else its some IO operation on some other socket
 for (i = 0; i < max_clients; i++)
 {
 sd = client_socket[i];

 if (FD_ISSET( sd , &readfds))
 {
 //Check if it was for closing , and also read the
 //incoming message
 valread = read( sd , buffer, 1024);
 if ( valread == 0)
 {
 //Somebody disconnected , get his details and print
 getpeername(sd , (struct sockaddr*)&address , \
							(socklen_t*)&addrlen);
 printf("Host disconnected , ip %s , port %d \n" ,
 inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

 //Close the socket and mark as 0 in list for reuse
 close( sd );
 client_socket[i] = 0;
 }

 //Echo back the message that came in
 else
 {
 //set the string terminating NULL byte on the end
 //of the data read
 char mensaje[] = "Le llego tu mensaje al File System";
 buffer[valread] = '\0';
 printf("Memoria %d: %s\n",sd, buffer);
 send(sd , mensaje , strlen(mensaje) , 0 );

 }
 }
 }
 }

 }

 void menu(){
 int opcionElegida;

 printf("Elija una opcion : \n");
 printf("1. SELECT \n	2. INSERT \n	3. CREATE\n		4. DESCRIBE \n		5. DROP\n");
 do{
 scanf("%i",opcionElegida);
 }while(opcionElegida<1 || opcionElegida>6);

 switch(opcionElegida){
 case 1:
 printf("Elegiste SELECT\n");
 break;
 case 2:
 printf("Elegiste INSERT\n");
 break;
 case 3:
 printf("Elegiste CREATE\n");
 break;
 case 4:
 printf("Elegiste DESCRIBE\n");
 break;
 case 5:
 printf("Elegiste DROP\n");
 break;
 default:
 printf("ERROR");
 break;
 }
 }
 */
