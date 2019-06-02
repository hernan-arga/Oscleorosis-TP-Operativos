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
#include <sys/stat.h>
#include <dirent.h>
#include <time.h>

#include <commons/collections/dictionary.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>

#define TRUE 1
#define FALSE 0
#define PORT 4444

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, OPERACIONINVALIDA
} OPERACION;

typedef struct {
	int timestamp;
	int key;
	char* value;
}t_registro ;

//void iniciarConexion();

void insert(char*, char*, char*, char*);
int existeUnaListaDeDatosADumpear();

void realizarSelect(char*, char*);

void create(char*, char*, char*, char*);
void crearMetadata(char*, char*, char*, char*);
void crearBinarios(char*, int);
void asignarBloque(char*);

//Funciones Auxiliares
int existeLaTabla(char*);
void tomarPeticion(char*);
void realizarPeticion(char**);
OPERACION tipoDePeticion(char*);
int cantidadValidaParametros(char**, int);
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
int esUnNumero(char* cadena);
int esUnTipoDeConsistenciaValida(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero);
t_registro* obtenerDatosParaKeyDeseada(FILE *, int);


int main(int argc, char *argv[]) {
	while (1) {
		char* mensaje = malloc(1000);
		do {
			fgets(mensaje, 1000, stdin);
		} while (!strcmp(mensaje, "\n"));
		tomarPeticion(mensaje);
		free(mensaje);
	}
	//iniciarConexion();
	return 0;
}

void tomarPeticion(char* mensaje) {
	//Fijarse despues cual seria la cantidad correcta de malloc
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(mensaje, " \n");
	realizarPeticion(mensajeSeparado);
	free(mensajeSeparado);
}

//Mejor forma de hacer esto?
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero) {
	int i = 0;
	while (puntero[i] != NULL) {
		i++;
	}
	//Uno mas porque tambien se incluye el NULL en el vector
	return ++i;
}

void realizarPeticion(char** parametros) {
	char *peticion = parametros[0];
	OPERACION instruccion = tipoDePeticion(peticion);
	switch (instruccion) {
	case SELECT:
		printf("Seleccionaste Select\n");
		//Defino de que manera van a ser validos los parametros del select y luego paso el puntero de dicha funcion.
		//Los parametros son validos si el segundo (la key) es un numero, y la cantidadDeParametrosUsados solo se pasa para hacer
		//polimorfica la funcion criterioTiposCorrectos.
		int criterioSelect(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			return esUnNumero(key);
		}

		if( parametrosValidos(2,parametros,(void*)criterioSelect) ){
			printf("ESTOY HACIENDO SELECT\n");
			char* tabla = parametros[1];
			char* key = parametros[2];
			realizarSelect(tabla, key);
		}


		break;

	case INSERT:
		printf("Seleccionaste Insert\n");
		int criterioInsert(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}

			if (cantidadDeParametrosUsados == 4) {
				char* timestamp = parametros[4];
				if (!esUnNumero(timestamp)) {
					printf("El timestamp debe ser un numero.\n");
				}
				return esUnNumero(key) && esUnNumero(timestamp);
			}
			return esUnNumero(key);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(4, parametros, (void *) criterioInsert)) {
			printf("ESTOY HACIENDO INSERT\n");
			char *tabla = parametros[1];
			char *key = parametros[2];
			char *valor = parametros[3];
			char *timestamp = parametros[4];

			insert(tabla, key, valor, timestamp);

		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			printf("ESTOY HACIENDO INSERT\n");
			char *tabla = parametros[1];
			char *key = parametros[2];
			char *valor = parametros[3];
			//¿El timestamp nesecita conversion? esto esta en segundos y no hay tipo de dato que banque los milisegundos por el tamanio
			long int timestampActual = (long int) time(NULL);
			char* timestamp = string_itoa(timestampActual);
			insert(tabla, key, valor, timestamp);
		}
		break;
	case CREATE:
		printf("Seleccionaste Create\n");
		int criterioCreate(char** parametros, int cantidadDeParametrosUsados) {
			char* tiempoCompactacion = parametros[4];
			char* cantidadParticiones = parametros[3];
			char* consistencia = parametros[2];
			if (!esUnNumero(cantidadParticiones)) {
				printf("La cantidad de particiones debe ser un numero.\n");
			}
			if (!esUnNumero(tiempoCompactacion)) {
				printf("El tiempo de compactacion debe ser un numero.\n");
			}
			return esUnNumero(cantidadParticiones)
					&& esUnNumero(tiempoCompactacion)
					&& esUnTipoDeConsistenciaValida(consistencia);
		}
		if (parametrosValidos(4, parametros, (void *) criterioCreate)) {
			char* tabla = parametros[1];
			char* tiempoCompactacion = parametros[4];
			char* cantidadParticiones = parametros[3];
			char* consistencia = parametros[2];
			printf("ESTOY HACIENDO CREATE\n");
			create(tabla, consistencia, cantidadParticiones,
					tiempoCompactacion);
		}
		break;
	default:
		printf("Error operacion invalida\n");
	}
}


int parametrosValidos(int cantidadDeParametrosNecesarios, char** parametros,
		int (*criterioTiposCorrectos)(char**, int)) {
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios)
			&& criterioTiposCorrectos(parametros,
					cantidadDeParametrosNecesarios);;
}

int cantidadValidaParametros(char** parametros,
		int cantidadDeParametrosNecesarios) {
	//Saco de la cuenta la peticion y el NULL
	int cantidadDeParametrosQueTengo =
			cantidadDeElementosDePunteroDePunterosDeChar(parametros) - 2;
	if (cantidadDeParametrosQueTengo != cantidadDeParametrosNecesarios) {
		//hay que arreglar esto para que en el caso de insert solo lo muestre si no se cumple con 4 ni con 3
		printf("La cantidad de parametros no es valida\n");
		return 0;
	}
	return 1;
}

OPERACION tipoDePeticion(char* peticion) {
	string_to_upper(peticion);
	if (!strcmp(peticion, "SELECT")) {
		free(peticion);
		return SELECT;
	} else {
		if (!strcmp(peticion, "INSERT")) {
			free(peticion);
			return INSERT;
		} else {
			if (!strcmp(peticion, "CREATE")) {
				free(peticion);
				return CREATE;
			} else {
				free(peticion);
				return OPERACIONINVALIDA;
			}
		}
	}
}

int esUnNumero(char* cadena) {
	for (int i = 0; i < strlen(cadena); i++) {
		if (!isdigit(cadena[i])) {
			return 0;
		}
	}
	return 1;
}

int esUnTipoDeConsistenciaValida(char* cadena) {
	int consistenciaValida = !strcmp(cadena, "SC") || !strcmp(cadena, "SHC")
			|| !strcmp(cadena, "EC");
	if (!consistenciaValida) {
		printf(
				"El tipo de consistencia no es valida. Asegurese de que este en mayusculas\n");
	}
	return consistenciaValida;
}

void insert(char* tabla, char* key, char* valor, char* timestamp) {
	string_to_upper(tabla);
	if (!existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen("Error: no existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: no existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create("../erroresInsert.log", "LFS", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		if (!existeUnaListaDeDatosADumpear()) {
			//alocar memoria
		}
	}
}

int existeUnaListaDeDatosADumpear() {
	return 1;
}


void create(char* tabla, char* consistencia, char* cantidadDeParticiones,
		char* tiempoDeCompactacion) {
	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen("Error: ya existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: ya existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create("../erroresCreate.log", "LFS", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		char* path = malloc(strlen("../Tables/") + strlen(tabla) + 1);
		char* metadataPath = malloc(
				strlen("../Tables/") + strlen(tabla) + strlen("/metadata") + 1);
		strcpy(path, "../Tables/");
		strcat(path, tabla);
		//El segundo parametro es una mascara que define permisos
		mkdir(path, 0777);

		strcpy(metadataPath, path);
		strcat(metadataPath, "/metadata");
		//Si en algun momento quiero convertir string a int existe la funcion atoi
		crearMetadata(metadataPath, consistencia, cantidadDeParticiones,
				tiempoDeCompactacion);
		crearBinarios(path, atoi(cantidadDeParticiones));
		free(metadataPath);
		free(path);
	}
}

void crearBinarios(char* path, int cantidadDeParticiones) {
	for (int i = 0; i < cantidadDeParticiones; i++) {
		//10 para dejar cierto margen a la cantidad de particiones
		char* directorioBinario = malloc(strlen(path) + 10);
		char* numeroDeParticion = string_itoa(i);
		strcpy(directorioBinario, path);
		strcat(directorioBinario, "/");
		strcat(directorioBinario, numeroDeParticion);
		strcat(directorioBinario, ".bin");
		asignarBloque(directorioBinario);
		free(directorioBinario);
		free(numeroDeParticion);
	}
}

//No se como funciona esta parte
void asignarBloque(char* directorioBinario) {
	//resolver lo mismo del save y el create que paso con el crearMetadata y ver como asignar los bloques
	FILE *archivoBinario = fopen(directorioBinario, "w");
	t_config *binario = config_create(directorioBinario);
	//config_set_value(directorioBinario, "SIZE", size);
	//config_set_value(directorioBinario, "BLOCKS", bloques);
	//config_save_in_file(binario, directorioBinario);
	fclose(archivoBinario);
	config_destroy(binario);
}

void crearMetadata(char* metadataPath, char* consistencia,
		char* cantidadDeParticiones, char* tiempoDeCompactacion) {
	FILE *archivoMetadata = fopen(metadataPath, "w");
	t_config *metadata = config_create(metadataPath);
	config_set_value(metadata, "CONSISTENCY", consistencia);
	config_set_value(metadata, "PARTITIONS", cantidadDeParticiones);
	config_set_value(metadata, "COMPACTION_TIME", tiempoDeCompactacion);
	//config_save_in_file necesita el t_config creado que a su vez el config_create
	//necesita el archivo que se crea en el save por eso lo creo yo no se si esta bien
	config_save_in_file(metadata, metadataPath);
	fclose(archivoMetadata);
	config_destroy(metadata);
}

int existeLaTabla(char* nombreDeTabla) {
	DIR *directorio = opendir("../Tables");
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABAS existe alguna que tenga el mismo nombre
		if ((directorioALeer->d_type) == DT_DIR
				&& !strcmp((directorioALeer->d_name), nombreDeTabla)) {
			closedir(directorio);
			return 1;
		}
	}
	closedir(directorio);
	return 0;
}

//No le pongo "select" porque ya esta la funcion de socket y rompe
void realizarSelect(char* tabla, char* key) {
	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* pathMetadata = malloc(strlen("../Tables/") + strlen(tabla) + strlen("/Metadata") + 1);
		strcpy(pathMetadata, "../Tables/");
		strcat(pathMetadata, tabla);
		strcat(pathMetadata, "/Metadata");
		t_config *metadata = config_create(pathMetadata);
		int cantidadDeParticiones = config_get_int_value(metadata, "PARTITIONS");
		//printf("%i - %i\n",atoi(key), cantidadDeParticiones);
		int particionQueContieneLaKey = (atoi(key))%cantidadDeParticiones;
		printf("La key esta en la particion %i\n", particionQueContieneLaKey);

		char* pathParticionQueContieneKey = malloc(strlen("../Tables/") + strlen(tabla) + strlen("/") + strlen(particionQueContieneLaKey) + strlen(".bin") + 1);
		strcpy(pathParticionQueContieneKey, "../Tables/");
		strcat(pathParticionQueContieneKey, tabla);
		strcat(pathParticionQueContieneKey, "/");
		strcat(pathParticionQueContieneKey, particionQueContieneLaKey);
		strcat(pathParticionQueContieneKey, ".bin");
		t_config *tamanioYBloques = config_create(pathParticionQueContieneKey);
		char** vectorBloques = config_get_array_value(tamanioYBloques, "BLOCK"); //devuelve vector de STRINGS
		char** vectorBloquesAux;
		for(int i=0; i<((sizeof(vectorBloques))-1); i++){
			vectorBloquesAux[i] = atoi(vectorBloques[i]);
			// por cada bloque, tengo que entrar a este bloque
			char* pathBloque = malloc(strlen("../Bloques/") + strlen((vectorBloquesAux[i])) + strlen(".bin") +1);
			strcpy(pathBloque, "../Bloques/");
			strcat(pathBloque, (vectorBloquesAux[i]));
			strcat(pathBloque, ".bin");
			FILE *archivoBloque = fopen(pathBloque, "r");
			if(archivoBloque == NULL){
				printf("no se pudo abrir archivo de bloques");
				exit(1);
			}
			t_registro* vectorDatosParaKeyDeseada = obtenerDatosParaKeyDeseada(archivoBloque, (atoi(key))); //devuelve vector de structs que tienen la key deseada


			fclose(archivoBloque);
			free(pathBloque);
		}
		free(pathMetadata);
		free(pathParticionQueContieneKey);
		config_destroy(tamanioYBloques);
		config_destroy(metadata);
	} else {
		char* mensajeALogear = malloc(
				strlen("Error: no existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: no existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create("../erroresSelect.log", "LFS", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
}

t_registro* obtenerDatosParaKeyDeseada(FILE *archivoBloque, int key){
	char linea[50];
	char* vectorSeparado[3];
	int i = 0;
	t_registro* vectorStructs[100];
	while( fgets(linea,50,archivoBloque) != NULL ){
		vectorSeparado[0] = (string_split(linea,";"))[0];
		vectorSeparado[1] = (string_split(linea,";"))[1];
		vectorSeparado[2] = (string_split(linea,";"))[2];
		int unTimestamp = atoi(vectorSeparado[0]);
		int unaKey = atoi(vectorSeparado[1]);
		if(unaKey == key){
			t_registro* p_registro;
			p_registro->timestamp = unTimestamp;
			p_registro->key = unaKey;
			p_registro->value = malloc(sizeof(vectorSeparado[2]));
			strcpy(p_registro->value, vectorSeparado[2]);
			vectorStructs[i] = p_registro;
			i++;
		}
	}

	return vectorStructs;
}



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
