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

struct Script{
	int PID;
	int PC;
	char* peticiones;
	int posicionActual;
};

struct datosMemoria{
	int ip;
};

struct tablas{
	char* nombre;
};

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL, ADD, RUN, METRICS, OPERACIONINVALIDA
} OPERACION;

OPERACION tipo_de_peticion(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char**);
int cantidadValidaParametros(char**, int);
void configurar_kernel();
void ejecutar();
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
void realizar_peticion(char**);
char* tempSinAsignar();
void tomar_peticion(char*);
void ejecutor(struct Script *);
void ejecutarReady();

/*
void funcionLoca();
void funcionLoca2();
*/
t_queue* new;
t_queue* ready;
t_list * PIDs;
t_list * listaDeMemorias;
t_list * tablas_conocidas;
t_config* configuracion;
int enEjecucionActualmente = 0;

int main(){
	/*
	pthread_t hola;
	pthread_t hola2;
	t_list* listaDeMemorias = list_create();
	pthread_create(&hola, NULL, (void*)funcionLoca, NULL);
	pthread_create(&hola2, NULL, (void*)funcionLoca2, NULL);
	pthread_join(hola,NULL);
	pthread_join(hola2,NULL);
	new = queue_create();

	printf("hola");
	*/
	configuracion = config_create("Kernel_config");
	PIDs = list_create();
	listaDeMemorias = list_create();
	tablas_conocidas = list_create();
	new = queue_create();
	ready = queue_create();

	printf("\tKERNEL OPERATIVO Y EN FUNCIONAMIENTO.\n");
	char* mensaje = malloc(100);
	//configurar_kernel();
	operacion_gossiping(); //Le pide a la memoria principal, las ip de las memorias conectadas y las escribe en el archivo IP_MEMORIAS
	pthread_t hiloEjecutarReady;
	pthread_create(&hiloEjecutarReady, NULL, (void*)ejecutarReady, NULL);
	do{
		printf("Mis subprocesos estan a la espera de su mensaje, usuario.\n");
		fgets(mensaje,100,stdin);
		tomar_peticion(mensaje);
	}while(strcmp(mensaje,"\n"));
	pthread_join(hiloEjecutarReady, NULL);
	return 0;
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
		printf("La cantidad de parametros no es valida\n");
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

//TODO WTF no me acordaba que estaba esto, se me hace que está mal codeado xd re negro jaja
int numeroSinUsar(){
	int n = 0;
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


void tomar_peticion(char* mensaje){
	//Fijarse despues cual seria la cantidad correcta de malloc
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(mensaje, " \n");
	realizar_peticion(mensajeSeparado);
	free(mensajeSeparado);
}

//TODO Hay que hacer que el parser reconozca algunos comandos más

void realizar_peticion(char** parametros) {
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
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			return esUnNumero(key);
		}

		if (parametrosValidos(2, parametros, (void*) criterioSelect)) {
			printf("Enviando SELECT a memoria.\n");
			char* nombre_archivo = tempSinAsignar();
			FILE* temp = fopen(nombre_archivo,"w");
			fprintf(temp,"%s %s %s" ,"SELECT",parametros[1],parametros[2]);
			fclose(temp);
			planificador(nombre_archivo);
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
			printf("Envio el comando INSERT a memoria");
			char* nombre_archivo = tempSinAsignar();
			FILE* temp = fopen(nombre_archivo,"w");
			fprintf(temp,"%s %s %s %s %s" ,"INSERT",parametros[1],parametros[2],parametros[3],parametros[4]);


		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			printf("Envio el comando INSERT a memoria");
			char* nombre_archivo = tempSinAsignar();
			FILE* temp = fopen(nombre_archivo,"w");
			fprintf(temp,"%s %s %s %s" ,"INSERT",parametros[1],parametros[2],parametros[3]);
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
			char* nombre_archivo = tempSinAsignar();
			FILE* temp = fopen(nombre_archivo,"w");
			fprintf(temp,"%s %s %s %s %s" ,"CREATE",parametros[1],parametros[2],parametros[3],parametros[4]);
			fclose(temp);
			planificador(nombre_archivo);
		}
		break;
	case DESCRIBE:
			printf("Seleccionaste Describe\n");
			int criterioDescribeTodasLasTablas(char** parametros,
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
				if (!existeLaTabla(tabla)) {
					printf("La tabla no existe\n");
				}
				return existeLaTabla(tabla);
			}
			if (parametrosValidos(0, parametros,
					(void *) criterioDescribeTodasLasTablas)) {
				describeTodasLasTablas();
			}
			if (parametrosValidos(1, parametros,
					(void *) criterioDescribeUnaTabla)) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				describeUnaTabla(tabla);
			}
			break;

	case DROP:
			printf("Seleccionaste Drop\n");
			int criterioDrop(char** parametros, int cantidadDeParametrosUsados) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				if (!existeLaTabla(tabla)) {
					printf("La tabla a borrar no existe\n");
				}
				return existeLaTabla(tabla);
			}
			if (parametrosValidos(1, parametros, (void *) criterioDrop)) {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				drop(tabla);
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
					printf("No deberia haber mas de un parametro, pero soy groso y puedo encolar de todas formas.\n");
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

void describeUnaTabla(){

}

void describeTodasLasTablas(){

}

int existeLaTabla(char *tabla){

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

//Esta funcion la llamaria desde un hilo
void ejecutarReady(){
	while(1){
		int multiprocesamiento = config_get_int_value(configuracion, "MULTIPROCESAMIENTO");
		if(enEjecucionActualmente<multiprocesamiento && !queue_is_empty(ready)){
			enEjecucionActualmente++;
			struct Script *unScript =  queue_pop(ready);
			//Ejecutor la llamaria desde un hilo detach (bajo demanda)
			ejecutor(unScript);
			enEjecucionActualmente--;
		}
	}
}

//TODO Inicializar queue_peek
void ejecutor(struct Script *ejecutando){
	char* caracter = malloc(sizeof(char));
	int quantum = config_get_int_value(configuracion,"QUANTUM");
	int error = 0;
	//if(!queue_is_empty(ready)){
			int i = 0;
			FILE * lql = fopen(ejecutando->peticiones,"r");
			while(i<quantum && !feof(lql) && !error){
				char* lineaDeScript = string_new();
				fread(caracter, 1, 1, lql);
				//leo caracter a caracter hasta encontrar \n
				while(!feof(lql) && strcmp(caracter, "\n")){
					string_append(&lineaDeScript, caracter);
					fread(caracter, sizeof(char), 1, lql);
				}
				printf("%s\n", lineaDeScript);
				//error = tomar_peticion(lineaDeScript);
				free(lineaDeScript);
				i++;
			}
			//Si salio por quantum
			if(i>=quantum){
				printf("---Fin q---\n");
				ejecutando->posicionActual = ftell(lql);
				queue_push(ready, ejecutando);
			}


	//}
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

//Este n aca esta raro
//int n=0;

void planificador(char* nombre_del_archivo){
	pasar_a_new(nombre_del_archivo);
	pasar_a_ready(new);
}

void popear(struct Script * proceso,t_queue* cola){
	proceso = queue_pop(cola);
}

void pasar_a_new(char* nombre_del_archivo){ //Prepara las estructuras necesarias y pushea el request a la cola de new
	 printf("\tAgregando script a cola de New\n");
	 struct Script *proceso = malloc(sizeof(struct Script));
	 proceso->PID = get_PID(PIDs);
	 proceso->PC = 0;
	 proceso->peticiones = string_new();
	 proceso->posicionActual = 0;
	 string_append(&proceso->peticiones, "./");
	 string_append(&proceso->peticiones,nombre_del_archivo);
	 queue_push(new,proceso);
	 printf("Script agregado\n");
}



int PID_usada(int numPID){
	bool _PID_en_uso(void* PID){
		return (int)PID == numPID;
	}
	return (list_find(PIDs,_PID_en_uso) != NULL);
}

void pasar_a_ready(t_queue * colaAnterior){
	printf("\tTrasladando script a Ready\n");
	queue_push(ready,queue_pop(colaAnterior));
	 printf("Script trasladado a Ready\n");
}



//void gossiping()

//TAREA:
//Crear un planificador de RR con quantum configurable y que sea capaz de parsear los archivos LQL
//Lista de programas activos con PID cada uno, si alguno se termina de correr, el PID vuelve a estar libre para que otro programa entrante lo ocupe.*/
