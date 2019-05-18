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
int separarEnVector(char**, char*[]);

typedef enum{
	CREATE, INSERT, DROP
}OPERACION;

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
	if (separarPalabra(mensaje, &peticion, &parametros) != -1) {
		verificarPeticion(peticion, parametros);
	} else {
		printf("No se ingresaron parametros");
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

int separarEnVector(char** parametros, char* parametrosSeparados[]) {
	char* delimitador = malloc(5);
	strcpy(delimitador, " \n");
	int i = 0;
	//Es necesario liberar la memoria de la variable que sigue?
	char* token = strtok(*parametros, delimitador);
	while (token != NULL || i < sizeof(parametrosSeparados)) {
		printf("token: %s - i: %i\n", token,i);
		parametrosSeparados[i] = token;
		token = strtok(NULL, delimitador);
		i++;
		printf("---token: %s - i: %i\n", token,i);
	}
	free(delimitador);
	if(i>sizeof(parametrosSeparados)){
		return 0;
	}
	return 1;
}

//Aca no se necesita la referencia solo el valor
void verificarPeticion(char* peticion, char* parametros) {
	int instruccion = numeroDePeticion(peticion);
	//parametrosSeparados[cantidadParametros] = malloc(1000);
	switch (instruccion) {
	case 1:
		printf("Seleccionaste Insert");
		validarParametros(&parametros, 4);
		break;
	case 2:
		printf("Seleccionaste Create\n");
		validarParametros(&parametros, 4);
		break;
	default:
		printf("Error operacion invalida\n");
	}
}

int validarParametros(char* parametros, int cantidad){
	char* parametrosSeparados[cantidad];
	int cantidadParametrosValida = separarEnVector(&parametros, parametrosSeparados);
	if(cantidadParametrosValida){
		return 1;
	}
	return 0;
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
