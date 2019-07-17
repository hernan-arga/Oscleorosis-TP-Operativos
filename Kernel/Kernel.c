// KERNEL
/*TAREA xd:
 * Sockets e Hilos
 * Parser para Metrics, Journal, Describe, Drop
 * Tablas
 * Criterios
 */
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <readline/readline.h>
#include <commons/collections/queue.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <commons/config.h>
#include <pthread.h>
#include <semaphore.h>
#include <commons/log.h>

struct Script{
	int PID;
	int PC;
	char* peticiones;
	int posicionActual;
};

struct datosMemoria{
	int ip;
};

struct tabla{
	int PARTITIONS;
	char *CONSISTENCY;
	int COMPACTION_TIME;
};

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL, ADD, RUN, METRICS, OPERACIONINVALIDA
} OPERACION;

OPERACION tipo_de_peticion(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char**);
int cantidadValidaParametros(char**, int);
//void configurar_kernel();
//void ejecutar();
int esUnNumero(char*);
int esUnTipoDeConsistenciaValida(char*);
int get_PID();
int IP_en_lista(int);
int numeroSinUsar();
void operacion_gossiping();
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
void pasar_a_new(char*);
void pasar_a_ready(t_queue*);
int PID_usada(int);
void planificador(char*);
void popear(struct Script *,t_queue*);
void realizar_peticion(char**,int,int*);
char* tempSinAsignar();
void tomar_peticion(char*,int, int*);
void separarPorComillas(char*, char* *, char* *, char* *);
void ejecutor(struct Script *);
void ejecutarReady();
void atenderPeticionesDeConsola();
void refreshMetadata();

void drop(char*);
void describeTodasLasTablas();
void describeUnaTabla(char*);
void actualizarDiccionarioDeTablas(char *, struct tabla *);

/*
void funcionLoca();
void funcionLoca2();
*/
t_queue* new;
t_queue* ready;
t_list * PIDs;
t_list * listaDeMemorias;
//tablas_conocidas diccionario que tiene structs tabla
t_dictionary *tablas_conocidas;
t_config* configuracion;
int enEjecucionActualmente = 0;
int n = 0;

t_log* g_logger;

int main(){
	configuracion = config_create("Kernel_config");
	PIDs = list_create();
	listaDeMemorias = list_create();
	tablas_conocidas = dictionary_create();
	new = queue_create();
	ready = queue_create();
	printf("\tKERNEL OPERATIVO Y EN FUNCIONAMIENTO.\n");
	//configurar_kernel();
	operacion_gossiping(); //Le pide a la memoria principal, las ip de las memorias conectadas y las escribe en el archivo IP_MEMORIAS
	pthread_t hiloEjecutarReady;
	pthread_t atenderPeticionesConsola;
	pthread_t describe;
	pthread_create(&hiloEjecutarReady, NULL, (void*)ejecutarReady, NULL);
	pthread_create(&atenderPeticionesConsola, NULL, (void*) atenderPeticionesDeConsola, NULL);
	pthread_create(&describe, NULL, (void*)refreshMetadata, NULL);
	pthread_join(describe, NULL);
	pthread_join(atenderPeticionesConsola, NULL);
	pthread_join(hiloEjecutarReady, NULL);
	return 0;
}

void refreshMetadata(){
	while(1){
		int refreshMetadata = config_get_int_value(configuracion, "METADATA_REFRESH");
		sleep(refreshMetadata);
		//Aca no me interesa esta variable pero la necesita
		int huboError;
		tomar_peticion("DESCRIBE", 0, &huboError);
	}
}

void atenderPeticionesDeConsola(){
	while(1){
		char* mensaje = malloc(100);
		//Aca no me interesa esta variable pero la necesita
		int huboError;
		do{
			printf("Mis subprocesos estan a la espera de su mensaje, usuario.\n");
			fgets(mensaje,100,stdin);
		}while(!strcmp(mensaje,"\n"));
		tomar_peticion(mensaje, 1, &huboError);
		free(mensaje);
	}
}

OPERACION tipo_de_peticion(char* peticion) {
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
			} else{
				if(!strcmp(peticion,"DESCRIBE")){
					free(peticion);
					return DESCRIBE;
				}else{
					if (!strcmp(peticion, "DROP")) {
						free(peticion);
						return DROP;
					} else{
						if (!strcmp(peticion, "JOURNAL")) {
							free(peticion);
							return JOURNAL;
						} else{
							if (!strcmp(peticion, "ADD")) {
								free(peticion);
								return ADD;
							} else{
								if (!strcmp(peticion, "RUN")) {
									free(peticion);
									return RUN;
								} else{
									if (!strcmp(peticion, "METRICS")) {
										free(peticion);
										return METRICS;
										} else{
											free(peticion);
											return OPERACIONINVALIDA;
										}
									}
								}
							}
						}
					}
				}
			}
		}
}

int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero) {
	int i = 0;
	while (puntero[i] != NULL) {
		i++;
	}
	//Uno mas porque tambien se incluye el NULL en el vector
	return ++i;
}

int cantidadValidaParametros(char** parametros,
		int cantidadDeParametrosNecesarios) {
	//Saco de la cuenta la peticion y el NULL
	int cantidadDeParametrosQueTengo =
			cantidadDeElementosDePunteroDePunterosDeChar(parametros) - 2;
	if (cantidadDeParametrosQueTengo != cantidadDeParametrosNecesarios) {
		//hay que arreglar esto para que en el caso de insert solo lo muestre si no se cumple con 4 ni con 3
		//printf("La cantidad de parametros no es valida\n");
		return 0;
	}
	return 1;
}

/*void configurar_kernel(){
	int IP = config_get_int_value(configuracion, "IP_MEMORIA");
	int PUERTO_MEMORIA = config_get_int_value(configuracion,"PUERTO_MEMORIA");
	int QUANTUM = config_get_int_value(configuracion,"QUANTUM");
	int MULTIPROCESAMIENTO = config_get_int_value(configuracion,"MULTIPROCESAMIENTO");
	int METADATA_REFRESH = config_get_int_value(configuracion,"METADATA_REFRESH");
	int SLEEP_EJECUCION = config_get_int_value(configuracion,"SLEEP_EJECUCION");
}*/
/*
void ejecutar(){
	int quantum = config_get_int_value(configuracion,"QUANTUM");
	int contador = 0;
	char * una_peticion = string_new();
	int error = 0;
	size_t tamanio_peticion = 0;

	while(!queue_is_empty(ready)){
		struct Script proceso;
		popear(&proceso,ready);
		char * nombre_del_archivo = proceso.peticiones;
		FILE* peticiones = fopen(nombre_del_archivo,"r");
		while(!feof(peticiones) && contador < quantum && error == 0 ){
			contador++;
			getline(&una_peticion,&tamanio_peticion,peticiones);
			tomar_peticion(una_peticion);

		}
	}
}
*/
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

int get_PID(){
	int PID = 1;
	int flag = 0;
	do{
		if(PID_usada(PID)){
			PID++;
		}
		else{
			flag = 1;
		}
	}while(flag == 0);
	list_add(PIDs, &PID);
	return PID;
}

int IP_en_lista(int ip_memoria){
	bool _IP_presente(void* IP){
			return (int)IP == ip_memoria;
		}
	return (list_find(listaDeMemorias, _IP_presente) != NULL);
}

//NOTA :XXX: numeroSinUsar devuelve numeros para asignar nombres distintos a archivos temporales
int numeroSinUsar(){
	//int n = 0;
	n++;
	return n;
}

//TODO falta sockets acá
void operacion_gossiping(){
	int IP = config_get_int_value(configuracion,"IP_MEMORIA");
	struct datosMemoria unaMemoria;
	//aca va algo con sockets para pedirle a la memoria principal, la IP de las demas memorias. Carga en unaMemoria la info y si no está en la lista de IPs, la coloca.
	if(!IP_en_lista(unaMemoria.ip)){
		list_add(listaDeMemorias,&unaMemoria);
	}
}

/*
void funcionLoca(){
	while(1){
		printf("aaaaaaaa\n");
	}
}

void funcionLoca2(){
	while(1){
		printf(":O\n");
	}
}
*/


//huboError se activa en 1 en caso de error
void tomar_peticion(char* mensaje, int es_request, int *huboError){
	char* value;
	char* noValue = string_new();
	char *posibleTimestamp = string_new();
	separarPorComillas(mensaje, &value, &noValue, &posibleTimestamp);
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	char** mensajeSeparadoConValue = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(noValue, " \n");
	int i = 0;
	while (mensajeSeparado[i] != NULL) {
		mensajeSeparadoConValue[i] = mensajeSeparado[i];
		i++;
	}
	mensajeSeparadoConValue[i] = value;
	if (value != NULL) {
		if(posibleTimestamp != NULL){
			mensajeSeparadoConValue[i + 1] = posibleTimestamp;
			mensajeSeparadoConValue[i + 2] = NULL;
		}
		else{
			mensajeSeparadoConValue[i + 1] = NULL;
		}
	}
		/*int j = 0;
		 while(mensajeSeparadoConValue[j]!=NULL){
		 printf("%s\n", mensajeSeparadoConValue[j]);
		 j++;
		 }*/
	realizar_peticion(mensajeSeparadoConValue, es_request, huboError);
	free(mensajeSeparado);

	//Fijarse despues cual seria la cantidad correcta de malloc
	/*char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(mensaje, " \n");
	realizar_peticion(mensajeSeparado,es_request, huboError); //NOTA :XXX: Agrego 'es_request' para que realizar_peticion sepa que es un request de usuario.
	free(mensajeSeparado);*/
}

//Esta funcion esta para separar la peticion del value del insert
void separarPorComillas(char* mensaje, char* *value, char* *noValue, char* *posibleTimestamp) {
	char** mensajeSeparado;
	mensajeSeparado = string_split(mensaje, "\"");
	string_append(noValue, mensajeSeparado[0]);
	if (mensajeSeparado[1] != NULL) {
		*value = string_new();
		string_append(value, "\"");
		string_append(value, mensajeSeparado[1]);
		string_append(value, "\"");

		//Evaluo si existe el posibleTimestamp
		if(mensajeSeparado[2] != NULL){
			if(strcmp(mensajeSeparado[2], "\n")){
				string_trim(&mensajeSeparado[2]);
				string_append(posibleTimestamp, mensajeSeparado[2]);
			}
			else{
				*posibleTimestamp = NULL;
			}
		}
		else{
			*posibleTimestamp = NULL;
		}


	} else {
		*value = NULL;
		*posibleTimestamp = NULL;
	}

}


//TODO Hay que hacer que el parser reconozca algunos comandos más
//huboError se activa en 1 en caso de error
void realizar_peticion(char** parametros, int es_request, int *huboError) {
	char *peticion = parametros[0];
	OPERACION instruccion = tipo_de_peticion(peticion);
	switch (instruccion) {
	case SELECT:
		printf("Seleccionaste Select\n");
		//Defino de que manera van a ser validos los parametros del select y luego paso el puntero de dicha funcion.
		//Los parametros son validos si el segundo (la key) es un numero, y la cantidadDeParametrosUsados solo se pasa para hacer
		//polimorfica la funcion criterioTiposCorrectos.
		int criterioSelect(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			char* tabla = parametros[1];
			string_to_upper(tabla);
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			if(!dictionary_has_key(tablas_conocidas, tabla)){
				char *mensajeALogear = string_new();
				string_append(&mensajeALogear, "No se tiene conocimiento de la tabla: ");
				string_append(&mensajeALogear, tabla);
				g_logger = log_create("./tablasNoEncontradas", "KERNEL", 1,	LOG_LEVEL_ERROR);
				log_error(g_logger, mensajeALogear);
				free(mensajeALogear);
			}
			return esUnNumero(key) && dictionary_has_key(tablas_conocidas, tabla);
		}

		if (parametrosValidos(2, parametros, (void*) criterioSelect)) {
			printf("Enviando SELECT a memoria.\n");
			//FIXME Una vez corroborado los parámetros, debo enviarlo a memoria.
			if(es_request){
				printf("Archivando SELECT para ir a cola de Ready.\n");
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo,"w");
				fprintf(temp,"%s %s %s" ,"SELECT",parametros[1],parametros[2]);
				fclose(temp);
				planificador(nombre_archivo);
			}
			else{
				//Aca lo manda por sockets y en caso de error modifica la variable huboError
				*huboError = 0;
			}
		}

		else{
			*huboError = 1;
		}

		break;

	case INSERT:
		printf("Seleccionaste Insert\n");
		int criterioInsert(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			char *tabla = parametros[1];
			string_to_upper(tabla);
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			if(!dictionary_has_key(tablas_conocidas, tabla)){
				char *mensajeALogear = string_new();
				string_append(&mensajeALogear, "No se tiene conocimiento de la tabla: ");
				string_append(&mensajeALogear, tabla);
				g_logger = log_create("./tablasNoEncontradas", "KERNEL", 1,	LOG_LEVEL_ERROR);
				log_error(g_logger, mensajeALogear);
				free(mensajeALogear);
			}

			if (cantidadDeParametrosUsados == 4) {
				char* timestamp = parametros[4];
				if (!esUnNumero(timestamp)) {
					printf("El timestamp debe ser un numero.\n");
				}
				return esUnNumero(key) && esUnNumero(timestamp) && dictionary_has_key(tablas_conocidas, tabla);
			}
			return esUnNumero(key) && dictionary_has_key(tablas_conocidas, tabla);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(4, parametros, (void *) criterioInsert)) {
			printf("Envio el comando INSERT a memoria\n");
			if(es_request){
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo,"w");
				fprintf(temp,"%s %s %s %s %s" ,"INSERT",parametros[1],parametros[2],parametros[3],parametros[4]);
				fclose(temp);
				planificador(nombre_archivo);
			}
			else{
				//Aca lo manda por sockets y en caso de error modifica la variable huboError
				*huboError = 0;
			}

		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			printf("Envio el comando INSERT a memoria");
			if(es_request){
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo,"w");
				fprintf(temp,"%s %s %s %s" ,"INSERT",parametros[1],parametros[2],parametros[3]);
				fclose(temp);
				planificador(nombre_archivo);
			}
			else{
				//Aca lo manda por sockets y en caso de error modifica la variable huboError
				*huboError = 0;
			}
		}
		else{
			*huboError = 1;
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
			printf("Enviando CREATE a memoria.\n");
			if(es_request){
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo,"w");
				fprintf(temp,"%s %s %s %s %s" ,"CREATE",parametros[1],parametros[2],parametros[3],parametros[4]);
				fclose(temp);
				planificador(nombre_archivo);
			}
			else{
				//Aca lo manda por sockets y en caso de error modifica la variable huboError
				*huboError = 0;
			}
		}
		break;
	case DESCRIBE:
			printf("Seleccionaste Describe\n");
			/*int criterioDescribeTodasLasTablas(char** parametros,
					int cantidadDeParametrosUsados) {
				char* tabla = parametros[1];
				return (tabla == NULL);
			}
			int criterioDescribeUnaTabla(char** parametros,
					int cantidadDeParametrosUsados) {
				char* tabla = parametros[1];
				if (tabla == NULL) {
					return 0;
				}
				string_to_upper(tabla);
				if (!dictionary_has_key(tablas_conocidas, tabla)) {
					printf("La tabla no existe\n");
				}
				return dictionary_has_key(tablas_conocidas, tabla);
			}*/
			//Solo esta para polimorfismo, el describe depende de si se encuentra la tabla en el LFS
			int criterioDescribe(char** parametros, int cantidadDeParametrosUsados) {
				return 0;
			}
			if (parametrosValidos(0, parametros,
					(void *) criterioDescribe)) {
				t_dictionary *diccionarioDeTablasTemporal;
				//diccionarioDeTablasTemporal = 	Le pido el diccionario de todas las tablas a memoria y actualizo el propio
				//dictionary_iterator(diccionarioDeTablasTemporal, (void*)actualizarDiccionarioDeTablas);
				dictionary_destroy(diccionarioDeTablasTemporal);
				*huboError = 0;
				if(es_request){
					describeTodasLasTablas();
				}
			}
			if (parametrosValidos(1, parametros,
					(void *) criterioDescribe)) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				//struct tabla *metadata = 	pido la metadata de 1 tabla a memoria y actualizo el diccionario de las tablas
				//actualizarDiccionarioDeTablas(tabla, metadata);
				if(es_request){
					describeUnaTabla(tabla);
				}
				*huboError = 0;
			}
			break;
			//TODO Falta que el drop sepa cuando es un request y cuando no
	case DROP:
			printf("Seleccionaste Drop\n");
			int criterioDrop(char** parametros, int cantidadDeParametrosUsados) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				if(!dictionary_has_key(tablas_conocidas, tabla)){
					char *mensajeALogear = string_new();
					string_append(&mensajeALogear, "No se tiene conocimiento de la tabla: ");
					string_append(&mensajeALogear, tabla);
					g_logger = log_create("./tablasNoEncontradas", "KERNEL", 1,	LOG_LEVEL_ERROR);
					log_error(g_logger, mensajeALogear);
					free(mensajeALogear);
				}
				return dictionary_has_key(tablas_conocidas, tabla);
			}
			if (parametrosValidos(1, parametros, (void *) criterioDrop)) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				drop(tabla);
				*huboError = 0;
			}
			else{
				*huboError = 1;
			}
		break;
	case JOURNAL: //falta continuarlo
	case ADD: //falta continuarlo

	case RUN:
		printf("Seleccionaste Run\n");
			if(parametros[1] == NULL){
				printf("No elegiste archivo.\n");
			}else{
				if(parametros[2] != NULL){
					printf("No deberia haber mas de un parametro, pero soy groso y puedo encolar de todas formas.\n"); //:FIXME: Debería cambiar esto?
				}
				char* nombre_del_archivo = parametros[1];
				string_trim(&nombre_del_archivo);
				/*if(!string_contains(nombre_del_archivo,".lql")){
					string_append(&nombre_del_archivo,".lql");
				}*/
				char *rutaDelArchivo = string_new();
				string_append(&rutaDelArchivo, "./");
				string_append(&rutaDelArchivo, nombre_del_archivo);
				FILE* archivo = fopen(nombre_del_archivo,"r");
				if(archivo==NULL){
					printf("El archivo no existe\n");
				}
				else{
					fclose(archivo);
					printf("Enviando Script a ejecutar.\n");
					planificador(nombre_del_archivo);
				}
		}
		break;

	case METRICS: //falta continuarlo

	default:
		printf("Error operacion invalida\n");
	}
}

//TODO Esto tiene dos ;; se me hace raro
int parametrosValidos(int cantidadDeParametrosNecesarios, char** parametros,
		int (*criterioTiposCorrectos)(char**, int)) {
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios)
			&& criterioTiposCorrectos(parametros,
					cantidadDeParametrosNecesarios);;
}

void describeUnaTabla(char* tabla){
	if(dictionary_has_key(tablas_conocidas, tabla)){
		struct tabla *metadataTabla = dictionary_get(tablas_conocidas, tabla);
		printf("%s: \n", tabla);
		printf("Particiones: %i\n", metadataTabla->PARTITIONS);
		printf("Consistencia: %s\n", metadataTabla->CONSISTENCY);
		printf("Tiempo de compactacion: %i\n\n", metadataTabla->COMPACTION_TIME);
	}

}

void describeTodasLasTablas(){
	dictionary_iterator(tablas_conocidas, (void*) describeUnaTabla);
}

//TODO socket para dropear la tabla en memoria y borrar la tabla de mi lista de tablas conocidas.
void drop(char* nombre_tabla){

}



/*
int main()
{
	printf("Soy Kernel \n");
	int sock_cliente_de_memoria;
	sock_cliente_de_memoria = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccion_server_memoria_kernel;
	direccion_server_memoria_kernel.sin_family = AF_INET;
	direccion_server_memoria_kernel.sin_port = htons(4441);
	direccion_server_memoria_kernel.sin_addr.s_addr = INADDR_ANY;

	if(connect(sock_cliente_de_memoria, (struct sockaddr *) &direccion_server_memoria_kernel, sizeof(direccion_server_memoria_kernel)) == -1)
	{
		perror("Hubo un error en la conexion");
		return -1;
	}

	char buffer[256];
	int leng = recv(sock_cliente_de_memoria, &buffer, sizeof(buffer), 0);
	buffer[leng] = '\0';

	printf("RECIBI INFORMACION DE LA MEMORIA: %s\n", buffer);

	//Mandar Mensajes
	while (1) {
		char* mensaje = malloc(1000);
		fgets(mensaje, 1024, stdin);
		send(sock_cliente_de_memoria, mensaje, strlen(mensaje), 0);
		free(mensaje);
	}

	close(sock_cliente_de_memoria);

	return 0;
}
*/

void ejecutarReady(){
	while(1){
		int multiprocesamiento = config_get_int_value(configuracion, "MULTIPROCESAMIENTO");
		if(enEjecucionActualmente<multiprocesamiento && !queue_is_empty(ready)){
			enEjecucionActualmente++;
			struct Script *unScript =  queue_pop(ready);
			//Creo los hilos de ejecucion bajo demanda
			pthread_t hiloScript;
			pthread_create(&hiloScript, NULL, (void*)ejecutor, (void*)unScript);
			pthread_detach(hiloScript);
			//ejecutor(unScript);
			enEjecucionActualmente--;
		}
	}
}

void actualizarDiccionarioDeTablas(char *tabla, struct tabla *metadata){
	if(dictionary_has_key(tablas_conocidas, tabla)){
		dictionary_remove(tablas_conocidas, tabla);
		dictionary_put(tablas_conocidas, tabla, metadata);
	}
	else{
		dictionary_put(tablas_conocidas, tabla, metadata);
	}
}

//TODO Inicializar queue_peek
void ejecutor(struct Script *ejecutando){
	printf("El script %s esta en ejecucion!\n", ejecutando->peticiones);
	char* caracter = (char *)malloc(sizeof(char)+1);
	//le copio algo adentro porque queda con basura sino
	strcpy(caracter, "0");
	int quantum = config_get_int_value(configuracion,"QUANTUM");
	int error = 0;
	int i = 0;
	FILE * lql = fopen(ejecutando->peticiones,"r");
	fseek(lql, ejecutando->posicionActual, 0);
	while(i<quantum && !feof(lql) && !error){
		char* lineaDeScript = string_new();
		fread(caracter, sizeof(char), 1, lql);
		//leo caracter a caracter hasta encontrar \n
		while(!feof(lql) && strcmp(caracter, "\n")){
			string_append(&lineaDeScript, caracter);
			fread(caracter, sizeof(char), 1, lql);
		}
		tomar_peticion(lineaDeScript,0, &error);
		free(lineaDeScript);
		i++;
	}
	//Si salio por quantum
	if(i>=quantum && !error){
		printf("---Fin q---\n");
		ejecutando->posicionActual = ftell(lql);
		queue_push(ready, ejecutando);
	}
	else{
		printf("el script %s paso a exit\n", ejecutando->peticiones);
	}
	fclose(lql);
	free(caracter);
}

char* tempSinAsignar(){
	char * nombre= string_new();
	char * strNumero = string_new();
	int numero = numeroSinUsar();

	strNumero = string_itoa(numero);

	string_append(&nombre,"temp");
	string_append(&nombre,strNumero);
	string_append(&nombre,".lql");
	return nombre;
}

void planificador(char* nombre_del_archivo){
	pasar_a_new(nombre_del_archivo);
	pasar_a_ready(new);
}

void popear(struct Script * proceso,t_queue* cola){
	proceso = queue_pop(cola);
}

void pasar_a_new(char* nombre_del_archivo){ //Prepara las estructuras necesarias y pushea el request a la cola de new
	 //printf("\tAgregando script a cola de New\n");
	 struct Script *proceso = malloc(sizeof(struct Script));
	 proceso->PID = get_PID(PIDs);
	 proceso->PC = 0;
	 proceso->peticiones = string_new();
	 proceso->posicionActual = 0;
	 string_append(&proceso->peticiones, "./");
	 string_append(&proceso->peticiones,nombre_del_archivo);
	 queue_push(new,proceso);
	 printf("El script %s paso a new\n", proceso->peticiones);
}



int PID_usada(int numPID){
	bool _PID_en_uso(void* PID){
		return (int)PID == numPID;
	}
	return (list_find(PIDs,_PID_en_uso) != NULL);
}

void pasar_a_ready(t_queue * colaAnterior){
	//printf("\tTrasladando script a Ready\n");
	struct Script *proceso = queue_pop(colaAnterior);
	queue_push(ready, proceso);
	printf("El script %s fue trasladado a Ready\n", proceso->peticiones);
}



//void gossiping()

//TAREA:
//Crear un planificador de RR con quantum configurable y que sea capaz de parsear los archivos LQL
//Lista de programas activos con PID cada uno, si alguno se termina de correr, el PID vuelve a estar libre para que otro programa entrante lo ocupe.*/
