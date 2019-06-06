// KERNEL

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

struct Script{
	struct LQL peticion;
	struct Script*sig;
	struct Script*ant;
};

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL, ADD, RUN, METRICS, OPERACIONINVALIDA
} OPERACION;

void configurar_kernel();
void dispatcher(t_list*,t_queue*,t_queue*);
void newbie(FILE*,t_list *,t_queue*);
int get_PID(t_list *);
int PID_usada(int,t_list *);
void pasar_a_ready(t_queue*,t_queue*);
void tomar_peticion(char* mensaje);
void realizar_peticion(char**);
OPERACION tipo_de_peticion(char*);
int cantidadValidaParametros(char**, int);
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
int esUnNumero(char* cadena);
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero);
int esUnTipoDeConsistenciaValida(char*);

int main(){
	t_list * PIDs = list_create();
	t_queue* new = queue_create();
	t_queue* ready = queue_create();

	FILE* memorias =fopen("IP_MEMORIAS","w");
	printf("\tKERNEL OPERATIVO Y EN FUNCIONAMIENTO.\n");
	//configurar_kernel();
	//operacion_gossiping(memorias); //Le pide a la memoria principal, las ip de las memorias conectadas y las escribe en el archivo IP_MEMORIAS
	while(1){
		printf("Mis subprocesos estan a la espera de su mensaje, usuario.\n");
		char* mensaje = malloc(100);
		fgets(mensaje,100,stdin);
		tomar_peticion(mensaje);
	}
	dispatcher(PIDs,new,ready);
	fclose(memorias);
	return 0;
}

void tomar_peticion(char* mensaje){
	//Fijarse despues cual seria la cantidad correcta de malloc
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	mensajeSeparado = string_split(mensaje, " \n");
	realizar_peticion(mensajeSeparado);
	free(mensajeSeparado);
}

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

		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			printf("Envio el comando INSERT a memoria");
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
		}
		break;
	default:
		printf("Error operacion invalida\n");
	}
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

int esUnNumero(char* cadena) {
	for (int i = 0; i < strlen(cadena); i++) {
		if (!isdigit(cadena[i])) {
			return 0;
		}
	}
	return 1;
}

int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero) {
	int i = 0;
	while (puntero[i] != NULL) {
		i++;
	}
	//Uno mas porque tambien se incluye el NULL en el vector
	return ++i;
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


void configurar_kernel(){
	FILE* archivo_configuracion = fopen("Kernel_config","w");
	t_config* configuracion = config_create("Kernel_config");
	printf("\tQue bien! Parece que hoy van a configurarme\n");
	char * valor = string_new();
	printf("\tNecesito una direccion IP, asi podre comunicarme con mis preciosas memorias.\n Por favor ingresa mi IP\n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"IP",valor);
	printf("\tGracias, ahora me dieron ganas de hablar con las memorias... Por un canal privado. Podrias conseguirme un puerto?\n Por favor, ingresa el puerto para comunicarme con las memorias\n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"PUERTO_MEMORIA",valor);
	printf("\tExcelente! Pero hay otro problema: Esos request no se van a ejecutar en un FIFO arcaico, no. Tenemos un RoundRobin!\n Por favor, ingrese el numero de quantum: \n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"QUANTUM",valor);
	printf("\tLo siento, se que es engorroso... Pero para empezar, fuiste vos el que inicio mi ejecucion.\n Ahora necesito que me digas cuantos procesos van a estar ejecutandose a la vez en las memorias\n Ingresa el grado de multiprocesamiento: \n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"MULTIPROCESAMIENTO",valor);
	printf("\tTodavia no se ni que es eso, pero por las dudas pone algun numerito...\n Ingresa el numero de Fresh Metadata: \n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"METADATA_REFRESH",valor);
	printf("\tMuy bien, por ultimo necesito otro numero que se mide en milisegundos, que rapido!\n Ingresa el retardo del ciclo de ejecucion: \n");
	fgets(valor, 100, stdin);
	config_set_value(configuracion,"SLEEP_EJECUCION",valor);
	printf("\tBueno, eso es todo. Esperame que guardo estos datos en mi archivo de configuracion\n");
	config_save(configuracion);
	fclose(archivo_configuracion);
	config_destroy(configuracion);
	}

void dispatcher(t_list* PIDs,t_queue* new,t_queue* ready){
	char * nombre_del_archivo = malloc(100);
	printf("\tNecesito que ingreses el nombre del archivo a leer: ");
	fgets(nombre_del_archivo,100,stdin);
	printf("%s",nombre_del_archivo);
	string_trim(&nombre_del_archivo);
	if(!string_contains(nombre_del_archivo,".lql")){
		string_append(&nombre_del_archivo,".lql");
	printf("%s",nombre_del_archivo);
	FILE* archivo = fopen(nombre_del_archivo,"r");
	if(archivo==NULL){
		printf("El archivo no existe\n");
	}
	else{
			newbie(archivo,PIDs,new);
			pasar_a_ready(new,ready);
	}
	fclose(archivo);
	free(nombre_del_archivo);
	}
}


void newbie(FILE* archivo, t_list * PIDs,t_queue* new){ //Prepara las estructuras necesarias y pushea el request a la cola de new
	 printf("\tAgregando script a cola de New\n");
	 script* proceso = malloc(sizeof(script));
	 proceso->PID = get_PID(PIDs);
	 proceso->PC = 0;
	 proceso->peticiones = archivo;
	 queue_push(new,proceso);
	 printf("Script agregado\n");
}

int get_PID(t_list * PIDs){
	int PID = 1;
	int flag = 0;
	do{
		if(PID_usada(PID,PIDs)){
			PID++;
		}
		else{
			flag = 1;
		}
	}while(flag == 0);
	list_add(PIDs, &PID);
	return PID;
}



int PID_usada(int numPID,t_list * PIDs){
	bool _PID_en_uso(void* PID){
		return (int)PID == numPID;
	}
	return (list_find(PIDs,_PID_en_uso) != NULL);
}

void pasar_a_ready(t_queue* new,t_queue* ready){
	printf("\tTrasladando script de New a Ready\n");
	queue_push(ready,queue_pop(new));
	 printf("Script trasladado a Ready\n");
}

//void gossiping()

//TAREA:
//Crear un planificador de RR con quantum configurable y que sea capaz de parsear los archivos LQL
//Lista de programas activos con PID cada uno, si alguno se termina de correr, el PID vuelve a estar libre para que otro programa entrante lo ocupe.*/
