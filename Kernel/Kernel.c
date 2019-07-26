/* Falta:
 *
 * Si memoria esta haciendo Journaling esperar
 * Sincronizar con semaforos
 * Conectar el goissiping con memoria cada x tiempo para levantar las memorias
 * Describe global para pedir las tablas del FS
 * Agregar las tablas que se conectaron y las que no borrarlas (hecho probar que funcione)
 * Que memoria avise si hubo error en las request
 *
 *¿Esta bien que el describe se mande siempre a strongConsistency? - preguntar
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
#include <dirent.h>
#include <commons/temporal.h>

struct metricas {
	clock_t tiempoDeCreacion;
	double segundosDeEjecucion;
	int tipoDeMetric; //0 para SELECT, 1 para INSERT
	char* IPMemoria;
};

struct Script {
	int PID;
	int PC;
	char* peticiones;
	int posicionActual;
};

struct datosMemoria {
	int32_t socket;
	struct sockaddr_in direccionSocket;
	int32_t MEMORY_NUMBER;
};

struct tabla {
	int PARTITIONS;
	char *CONSISTENCY;
	int COMPACTION_TIME;
};

typedef enum {
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP,
	JOURNAL,
	ADD,
	RUN,
	METRICS,
	OPERACIONINVALIDA
} OPERACION;

//t_dictionary* diccionarioTemporal;

OPERACION tipo_de_peticion(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char**);
int cantidadValidaParametros(char**, int);
//void configurar_kernel();
//void ejecutar();
int esUnNumero(char*);
int esUnTipoDeConsistenciaValida(char*);
int get_PID();
/*int IP_en_lista(char*);
 void agregarAMiLista(struct datosMemoria * unaMemoria);*/
int numeroSinUsar();
void operacion_gossiping();
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
void pasar_a_new(char*);
void pasar_a_ready(t_queue*);
int PID_usada(int);
void planificador(char*);
void popear(struct Script *, t_queue*);
void realizar_peticion(char**, int, int*);
char* tempSinAsignar();
void tomar_peticion(char*, int, int*);
void separarPorComillas(char*, char* *, char* *, char* *);
void ejecutor(struct Script *);
void ejecutarReady();
void atenderPeticionesDeConsola();
void refreshMetadata();
void generarMetrica(clock_t, int, char *);
t_list * borrarObsoletos(clock_t);
void mostrarInserts();
void mostrarSelects();
void memoryLoad();
void metrics(int);
int32_t conectarUnaMemoria(struct datosMemoria *, char*, int);
int32_t conectarMemoriaRecibida(struct datosMemoria*);

void drop(char*);
void describeTodasLasTablas();
void describeUnaTabla(char*);
void actualizarDiccionarioDeTablas(char *, struct tabla *);
void quitarDelDiccionarioDeTablasLaTablaBorrada(char *);
void logearMetrics();

void borrarTodosLosTemps();
int funcionHash(int);
char* pedirValue(char* tabla, char* laKey, int socketMemoria);
struct tabla *pedirDescribeUnaTabla(char* tabla, int socketMemoria);
void mandarInsert(char* tabla, char* key, char* value, int socketMemoria);
void mandarDrop(char *tabla, int socketMemoria);
void mandarJournal(int socketMemoria);
void mandarCreate(char *, char *, char *, char *, int);
void guardarDiccionarioGlobal(int socketMemoria);

void evaluarMemoriaRecibida(struct datosMemoria*);
void quitarMemoriaDeSC(struct datosMemoria *);
void quitarMemoriaDe1Lista(struct datosMemoria *, t_list *);

void PRUEBA();

int multiprocesamiento;
t_list * metricasDeUltimos30Segundos;
t_list * listaMetricas; // Lista con info. de los últimos INSERT y SELECT, se irán borrando si exceden los 30 segundos. Se considera el tiempo de ejecución, el tiempo que tardan mientras se están ejecutando.
t_queue* new;
t_queue* ready;
t_list * PIDs;
t_list *listaDeMemorias;
//tablas_conocidas diccionario que tiene structs tabla
t_dictionary *tablas_conocidas;
t_dictionary *diccionarioDeTablasTemporal;
t_config* configuracion;
int enEjecucionActualmente = 0;
int n = 0;
int32_t socketMemoriaPrincipal;
t_log* g_logger;

struct datosMemoria* memoriaPrincipal;
struct datosMemoria* strongConsistency;
t_list *hashConsistency;
t_list *eventualConsistency;

struct metricas * unRegistro;
//pthread_t hiloLevantarConexion;
sem_t MAXIMOPROCESAMIENTO;

int main() {
	listaMetricas = list_create();
	metricasDeUltimos30Segundos = list_create();
	configuracion = config_create("Kernel_config");
	PIDs = list_create();
	//Memorias
	listaDeMemorias = list_create();
	strongConsistency = (struct datosMemoria*) malloc(
			sizeof(struct datosMemoria));
	hashConsistency = list_create();
	eventualConsistency = list_create();

	tablas_conocidas = dictionary_create();
	new = queue_create();
	ready = queue_create();
	printf("\tKERNEL OPERATIVO Y EN FUNCIONAMIENTO.\n");

	//PRUEBA();
	multiprocesamiento = config_get_int_value(configuracion,
			"MULTIPROCESAMIENTO");
	iniciarSemaforos();

	//diccionarioDeTablasTemporal = malloc(3000);
	diccionarioDeTablasTemporal = dictionary_create();

	//Conecto a la memoria principal
	//memoriaPrincipal = (struct datosMemoria*)malloc(sizeof(struct datosMemoria));
	memoriaPrincipal = malloc(sizeof(struct datosMemoria));
	char * IP_MEMORIA = config_get_string_value(configuracion, "IP_MEMORIA");
	int PUERTO_MEMORIA = config_get_int_value(configuracion, "PUERTO_MEMORIA");
	conectarUnaMemoria(memoriaPrincipal, IP_MEMORIA, PUERTO_MEMORIA);

	memoriaPrincipal->MEMORY_NUMBER = 1;
	list_add(listaDeMemorias, (void*) memoriaPrincipal);

	/*struct datosMemoria* unaMemoria2;
	 list_add(listaDeMemorias, unaMemoria2);*/

	borrarTodosLosTemps();
	pthread_t hiloEjecutarReady;
	pthread_t atenderPeticionesConsola;
	pthread_t describe;
	pthread_t metrics;
	pthread_t goissiping;
	pthread_create(&metrics, NULL, (void*) logearMetrics, NULL);
	pthread_create(&hiloEjecutarReady, NULL, (void*) ejecutarReady, NULL);
	pthread_create(&atenderPeticionesConsola, NULL,
			(void*) atenderPeticionesDeConsola, NULL);
	//pthread_create(&hiloLevantarConexion, NULL, (void*)iniciarConexion, NULL);
	pthread_create(&goissiping, NULL, (void*)operacion_gossiping, NULL);
	pthread_create(&describe, NULL, (void*) refreshMetadata, NULL);
	pthread_join(metrics, NULL);
	pthread_join(describe, NULL);
	pthread_join(atenderPeticionesConsola, NULL);
	pthread_join(hiloEjecutarReady, NULL);
	//pthread_join(hiloLevantarConexion, NULL);
	pthread_join(goissiping, NULL);
	return 0;
}

void iniciarSemaforos() {
	sem_init(&MAXIMOPROCESAMIENTO, 0, multiprocesamiento);
}

void PRUEBA() {
	struct tabla * unaTabla = malloc(sizeof(struct tabla *));
	unaTabla->CONSISTENCY = "SHC";
	unaTabla->COMPACTION_TIME = 10;
	unaTabla->PARTITIONS = 2;
	actualizarDiccionarioDeTablas("TABLA1", unaTabla);
	/*char* unaMemoria = malloc(40);
	 strcpy(unaMemoria,"192.168");
	 agregarAMiLista(unaMemoria);*/
}

void borrarTodosLosTemps() {
	DIR *directorio = opendir("./");
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABLAS existe alguna que tenga el mismo nombre
		if (string_starts_with(directorioALeer->d_name, "temp")) {
			char *archivoABorrar = string_new();
			string_append(&archivoABorrar, "./");
			string_append(&archivoABorrar, directorioALeer->d_name);
			remove(archivoABorrar);
		}
	}
	closedir(directorio);
}

/*void borrarTemps(){
 char * nombre= string_new();
 char * strNumero = string_new();
 int status;
 for (int i=0; i<=n;i++){
 strNumero = string_itoa(i);
 string_append(&nombre,"temp");
 string_append(&nombre,strNumero);
 string_append(&nombre,".lql");
 status = remove(nombre);
 if(status == 0){
 printf("%s file deleted successfully.\n", nombre);
 }
 }
 }*/

// METRICS tiene dos opciones... O logear automáticamente cada 30 segundos o informar por consola a pedido del usuario, sin logear.
void metrics(int opcion) {
	clock_t tiempoActual = clock();
	metricasDeUltimos30Segundos = borrarObsoletos(tiempoActual);
	mostrarInserts(opcion);
	mostrarSelects(opcion);
	memoryLoad(opcion);
}

void logearMetrics() {
	while (1) {
		//metrics(1);
		sleep(30);
	}

}

int funcionHash(int key) {
	//Le sumo 1 al resto porque no tengo memoria 0 por ej
	return (key % list_size(hashConsistency)) + 1;
}

void memoryLoad(int opcion) {
	t_list * soloMismaIP = list_create();
	t_list * soloInserts = list_create();
	t_list * soloSelect = list_create();
	int contador = 0;
	int operacionesTotales = list_size(listaMetricas);
	int cantidadDeIPS = list_size(listaDeMemorias);
	for (int i = 0; i < cantidadDeIPS; i++) {
		char* unaIP = list_get(listaDeMemorias, i);

		bool _filtrarMismaIP(void* elemento) {
			return !strcmp(unaIP, ((struct metricas *) elemento)->IPMemoria);
		}
		soloMismaIP = list_filter(listaMetricas, _filtrarMismaIP);

		bool _filtrarInsert(void* elemento) {
			return 1 == ((struct metricas*) elemento)->tipoDeMetric;
		}
		soloInserts = list_filter(soloMismaIP, _filtrarInsert);
		contador = list_size(soloInserts);
		if (opcion == 0) {
			printf(
					"La cantidad de INSERTS de la Memoria con IP %s es %i, respecto de %i operaciones totales.\n",
					unaIP, contador, operacionesTotales);
		} else {
			char* mensajeALogear = string_new();
			char* tiempo = temporal_get_string_time();
			char* tiempoCorchetes = string_new();
			char* info = string_new();
			string_append_with_format(&tiempoCorchetes, "[%s]\t", tiempo);
			string_append(&mensajeALogear, tiempoCorchetes);
			info =
					string_from_format(
							"La cantidad de INSERTS de la memoria con IP %s es %i, respecto de %i operaciones totales.\n",
							unaIP, contador, operacionesTotales);
			string_append(&mensajeALogear, info);
			t_log* g_logger;
			g_logger = log_create("./metricas.log", "Kernel", 0,
					LOG_LEVEL_INFO);
			log_info(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);
			free(tiempo);
			free(tiempoCorchetes);
			free(info);
		}

		bool _filtrarSELECT(void* tipo) {
			return 0 == tipo;
		}
		soloSelect = list_filter(soloMismaIP, _filtrarSELECT);
		contador = list_size(soloSelect);
		if (opcion == 0) {
			printf(
					"La cantidad de SELECTS de la memoria con IP %s es %i, respecto de %i operaciones totales.\n",
					unaIP, contador, operacionesTotales);
		} else {
			char* mensajeALogear = string_new();
			char* tiempo = temporal_get_string_time();
			char* tiempoCorchetes = string_new();
			char* info = string_new();
			string_append_with_format(&tiempoCorchetes, "[%s]\t", tiempo);
			string_append(&mensajeALogear, tiempoCorchetes);
			info =
					string_from_format(
							"La cantidad de SELECTS de la memoria con IP %s es %i, respecto de %i operaciones totales.\n",
							unaIP, contador, operacionesTotales);
			string_append(&mensajeALogear, info);
			t_log* g_logger;
			g_logger = log_create("./metricas.log", "Kernel", 0,
					LOG_LEVEL_INFO);
			log_info(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);
			free(tiempo);
			free(tiempoCorchetes);
			free(info);
		}
	}
}

void mostrarInserts(int opcion) {
	printf("mostrandoInserts =");
	double contador = 0;
	t_list * soloInserts = list_create();
	struct metricas* unaMetrica;
	bool _filtrarInsert(void* elemento) {
		printf("%d\n", ((struct metricas *) elemento)->tipoDeMetric);
		return 1 == ((struct metricas *) elemento)->tipoDeMetric;
	}

	//list_add_all(soloInserts,metricas);
	printf("%d", list_size(listaMetricas));
	soloInserts = list_filter(listaMetricas, _filtrarInsert);
	//struct metricas * prueba = malloc(sizeof(struct metricas));
	//prueba = list_get(metricas,0);

	int cantidadDeElementos = list_size(soloInserts);
	if (cantidadDeElementos != 0) {
		printf("asdasd");
		for (int i = 0; i < cantidadDeElementos; i++) {
			unaMetrica = list_get(soloInserts, i);
			contador += unaMetrica->segundosDeEjecucion;
		}
		contador = contador / cantidadDeElementos;
	} else {
		contador = 0;
	}

	if (opcion == 0) {
		printf("Promedio de tiempo de ejecucion para INSERT\n");
		printf("%f\n", contador);
		printf("Cantidad de INSERTS en los ultimos 30 segundos: %i\n",
				cantidadDeElementos);
	} else {
		char* mensajeALogear = string_new();
		char* tiempo = temporal_get_string_time();
		char* tiempoCorchetes = string_new();
		char * info = string_new();
		string_append_with_format(&tiempoCorchetes, "[%s]\t", tiempo);
		string_append(&mensajeALogear, tiempoCorchetes);
		info = string_from_format(
				"El promedio de tiempo de ejecucion para INSERT es %f\n",
				contador);
		string_append(&mensajeALogear, info);
		t_log* g_logger;
		g_logger = log_create("./metricas.log", "Kernel", 0, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		free(tiempo);
		free(tiempoCorchetes);
		free(info);
	}
}

void mostrarSelects(int opcion) {

	double contador = 0;
	t_list * soloSelects = list_create();
	struct metricas* unaMetrica;
	bool _filtrarSELECT(void* tipo) {
		return 0 == tipo;
	}
	soloSelects = list_filter(listaMetricas, _filtrarSELECT);
	int cantidadDeElementos = list_size(soloSelects);
	if (cantidadDeElementos != 0) {
		for (int i = 0; i < cantidadDeElementos; i++) {
			unaMetrica = list_get(soloSelects, i);
			contador += unaMetrica->segundosDeEjecucion;
		}
		contador = contador / cantidadDeElementos;
	} else {
		contador = 0;
	}

	if (opcion == 0) {
		printf("Promedio de tiempo de ejecucion para SELECT\n");
		printf("%f\n", contador);
		printf("Cantidad de SELECTS en los ultimos 30 segundos: %i\n",
				cantidadDeElementos);
	} else {
		char* mensajeALogear = string_new();
		char* tiempo = temporal_get_string_time();
		char* tiempoCorchetes = string_new();
		char * info = string_new();
		string_append_with_format(&tiempoCorchetes, "[%s]\t", tiempo);
		string_append(&mensajeALogear, tiempoCorchetes);
		info = string_from_format(
				"El promedio de tiempo de ejecucion para SELECT es %f\n",
				contador);
		string_append(&mensajeALogear, info);
		t_log* g_logger;
		g_logger = log_create("./metricas.log", "Kernel", 0, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		free(tiempo);
		free(tiempoCorchetes);
		free(info);
	}
}

t_list * borrarObsoletos(clock_t tiempoActual) {
	//struct metricas* unaMetrica = malloc(sizeof(struct metricas*));
	if (!list_is_empty(listaMetricas)) {
		bool _tiempoPasado(void*unaMetrica) {
			return (clock() / CLOCKS_PER_SEC - 30)
					< ((struct metricas *) unaMetrica)->tiempoDeCreacion
							/ CLOCKS_PER_SEC;
		}
		return list_filter(listaMetricas, _tiempoPasado);
	}
	return listaMetricas;
}

void refreshMetadata() {
	while (1) {
		int refreshMetadata = config_get_int_value(configuracion,
				"METADATA_REFRESH");
		sleep(refreshMetadata);
		//Aca no me interesa esta variable pero la necesita
		int huboError;
		tomar_peticion("DESCRIBE", 0, &huboError);
	}
}

void atenderPeticionesDeConsola() {
	while (1) {
		char* mensaje = malloc(100);
		//Aca no me interesa esta variable pero la necesita
		int huboError;
		do {
			fgets(mensaje, 100, stdin);
		} while (!strcmp(mensaje, "\n") || !strcmp(mensaje, " \n"));
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
			} else {
				if (!strcmp(peticion, "DESCRIBE")) {
					free(peticion);
					return DESCRIBE;
				} else {
					if (!strcmp(peticion, "DROP")) {
						free(peticion);
						return DROP;
					} else {
						if (!strcmp(peticion, "JOURNAL")) {
							free(peticion);
							return JOURNAL;
						} else {
							if (!strcmp(peticion, "ADD")) {
								//free(peticion);//ACA ROMPE
								return ADD;
							} else {
								if (!strcmp(peticion, "RUN")) {
									free(peticion);
									return RUN;
								} else {
									if (!strcmp(peticion, "METRICS")) {
										free(peticion);
										return METRICS;
									} else {
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

int get_PID() {
	int PID = 1;
	int flag = 0;
	do {
		if (PID_usada(PID)) {
			PID++;
		} else {
			flag = 1;
		}
	} while (flag == 0);
	list_add(PIDs, &PID);
	return PID;
}
//FIXME
/*int IP_en_lista(char* ip_memoria) {
 bool _IP_presente(struct datosMemoria * unaMemoria) {
 //return !strcmp(unaMemoria->direccionSocket.sin_addr.s_addr, ip_memoria);
 return unaMemoria->direccionSocket.sin_addr.s_addr == atoi(ip_memoria);
 }
 return (list_find(listaDeMemorias, (void*) _IP_presente) != NULL);
 }*/

//NOTA: numeroSinUsar devuelve numeros para asignar nombres distintos a archivos temporales
int numeroSinUsar() {
	//int n = 0;
	n++;
	return n;
}

void operacion_gossiping() {
	while (1) {
		printf("GOSSIPING\n");
		char* buffer = malloc(3 * sizeof(int));

		int peticion = 8;
		int tamanioPeticion = sizeof(int);
		int fin = 0;
		memcpy(buffer, &tamanioPeticion, sizeof(int));
		memcpy(buffer + sizeof(int), &peticion, sizeof(int));
		memcpy(buffer + 2 * sizeof(int), &fin, sizeof(int));
		send(memoriaPrincipal->socket, buffer, 3 * sizeof(int), 0);

		/*
		 int *tamanioMemoriasRecibidas = malloc(sizeof(int));
		 read(memoriaPrincipal->socket, tamanioMemoriasRecibidas, sizeof(int));
		 t_list *memoriasRecibidas = list_create();
		 recv(memoriaPrincipal->socket, memoriasRecibidas, sizeof(t_list*), 0);
		 list_iterate(memoriasRecibidas, (void*)evaluarMemoriaRecibida);

		 //Me fijo las memorias que se desconectaron
		 void evaluarMemoriaConocida(struct datosMemoria *memoriaConocida){
		 int seDesconectoLaMemoria(struct datosMemoria *memoriaConocida){
		 int esLaMemoriaConocida(struct datosMemoria *memoriaRecibida){
		 //Revisar de que manera se puede verificar que sean la misma memoria
		 return memoriaRecibida->MEMORY_NUMBER == memoriaConocida->MEMORY_NUMBER;
		 }

		 return !list_any_satisfy(memoriasRecibidas, (void*)esLaMemoriaConocida);
		 }

		 if(seDesconectoLaMemoria(memoriaConocida)){
		 quitarMemoriaDeSC(memoriaConocida);
		 quitarMemoriaDe1Lista(memoriaConocida, hashConsistency);
		 quitarMemoriaDe1Lista(memoriaConocida, eventualConsistency);
		 }
		 }

		 */

		//list_iterate(listaDeMemorias, (void*)evaluarMemoriaConocida);
		//free(memoriasRecibidas);
		int* socket = malloc(sizeof(int));
		recv(memoriaPrincipal->socket, socket, sizeof(int), 0);

		while (*socket != 0) {
			struct sockaddr_in *direccion = malloc(sizeof(struct sockaddr_in));
			recv(memoriaPrincipal->socket, direccion, sizeof(struct sockaddr_in), 0);

			int32_t* num = malloc(sizeof(int));
			recv(memoriaPrincipal->socket, num, sizeof(int32_t), 0);

			printf("Num: %d\n", *num);
			printf("Puerto: %d\n", direccion->sin_port);

			struct datosMemoria* unaM = malloc(sizeof(struct datosMemoria));

			unaM->MEMORY_NUMBER = *num;
			unaM->direccionSocket = *direccion;
			unaM->socket = *socket;

			//Aca la agregas a tu lista

			free(direccion);
			free(num);
			free(socket);

			socket = malloc(sizeof(int));
			recv(memoriaPrincipal->socket, socket, sizeof(int), 0);
		}
		sleep(120);
	}
}

void evaluarMemoriaRecibida(struct datosMemoria* memoriaRecibida) {
	int yaSeEncuentraLaMemoria(struct datosMemoria* memoriaConocida) {
		//Fijarse porque se pueden comparar
		return memoriaConocida->MEMORY_NUMBER == memoriaRecibida->MEMORY_NUMBER;
	}
	if (list_any_satisfy(listaDeMemorias, (void*) yaSeEncuentraLaMemoria)) {
		list_add(listaDeMemorias, memoriaRecibida);
	}
}

int sonLaMismaMemoria(struct datosMemoria *memoria1,
		struct datosMemoria *memoria2) {
	return memoria1->MEMORY_NUMBER == memoria2->MEMORY_NUMBER;
}

void quitarMemoriaDeSC(struct datosMemoria *unaMemoria) {
	if (sonLaMismaMemoria(unaMemoria, strongConsistency)) {
		free(strongConsistency);
		strongConsistency = (struct datosMemoria*) malloc(
				sizeof(struct datosMemoria));
	}
}

void quitarMemoriaDe1Lista(struct datosMemoria *unaMemoria, t_list *unaLista) {
	int esLaMemoria(struct datosMemoria *unaMemoriaConocida) {
		return unaMemoriaConocida->MEMORY_NUMBER == unaMemoria->MEMORY_NUMBER;
	}
	list_remove_by_condition(unaLista, (void*) esLaMemoria);
}

//En esta funcion la IP, el puerto y el socket ya fueron llenados
int32_t conectarMemoriaRecibida(struct datosMemoria* unaMemoria) {
	if (connect(unaMemoria->socket,
			(struct sockaddr *) &unaMemoria->direccionSocket,
			sizeof(unaMemoria->direccionSocket)) == -1) {
		perror("Hubo un error en la conexion");
		return -1;
	}
	//Mando un numero distinto de cero a memoria para que sepa que se conecto kernel
	//send(unaMemoria->socket, "1", 2, 0);
	return 0;
}

/*void agregarAMiLista(struct datosMemoria * unaMemoria) {
 if (!IP_en_lista(string_itoa(unaMemoria->direccionSocket.sin_addr.s_addr))) {
 list_add(listaDeMemorias, unaMemoria);
 }
 }*/

//huboError se activa en 1 en caso de error
void tomar_peticion(char* mensaje, int es_request, int *huboError) {
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
	//mensajeSeparadoConValue[i] = value;
	if (value != NULL) {
		mensajeSeparadoConValue[i] = (char*) malloc(strlen(value) + 1);
		string_append(&mensajeSeparadoConValue[i], value);
	} else {
		mensajeSeparadoConValue[i] = (char*) malloc(20);
		mensajeSeparadoConValue[i] = NULL;
	}
	if (value != NULL) {
		if (posibleTimestamp != NULL) {
			mensajeSeparadoConValue[i + 1] = posibleTimestamp;
			mensajeSeparadoConValue[i + 2] = NULL;
		} else {
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
	//free(value);
	free(noValue);
	free(posibleTimestamp);

	//Fijarse despues cual seria la cantidad correcta de malloc
	/*char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	 mensajeSeparado = string_split(mensaje, " \n");
	 realizar_peticion(mensajeSeparado,es_request, huboError); //NOTA: Agrego 'es_request' para que realizar_peticion sepa que es un request de usuario.
	 free(mensajeSeparado);*/
}

//Esta funcion esta para separar la peticion del value del insert
void separarPorComillas(char* mensaje, char* *value, char* *noValue,
		char* *posibleTimestamp) {
	char** mensajeSeparado;
	mensajeSeparado = string_split(mensaje, "\"");
	string_append(noValue, mensajeSeparado[0]);
	if (mensajeSeparado[1] != NULL) {
		*value = string_new();
		string_append(value, "\"");
		string_append(value, mensajeSeparado[1]);
		string_append(value, "\"");

		//Evaluo si existe el posibleTimestamp
		if (mensajeSeparado[2] != NULL) {
			if (strcmp(mensajeSeparado[2], "\n")) {
				string_trim(&mensajeSeparado[2]);
				string_append(posibleTimestamp, mensajeSeparado[2]);
			} else {
				*posibleTimestamp = NULL;
			}
		} else {
			*posibleTimestamp = NULL;
		}

	} else {
		*value = NULL;
		*posibleTimestamp = NULL;
	}

}

//huboError se activa en 1 en caso de error
void realizar_peticion(char** parametros, int es_request, int *huboError) {

	char *peticion = parametros[0];
	OPERACION instruccion = tipo_de_peticion(peticion);
	switch (instruccion) {
	case SELECT:
		;
		//printf("Seleccionaste Select\n");
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
			if (!dictionary_has_key(tablas_conocidas, tabla)) {
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
			if (es_request) {
				printf("Archivando SELECT para ir a cola de Ready.\n");
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo, "w");
				fprintf(temp, "%s %s %s", "SELECT", parametros[1], parametros[2]);
				fclose(temp);
				planificador(nombre_archivo);
			} else {
				clock_t tiempoSelect = clock();
				//generarMetrica(tiempoSelect,0,IPMemoria);

				char* tabla = parametros[1];
				char *key = parametros[2];
				struct tabla *unaTabla = dictionary_get(tablas_conocidas, tabla);
				//Aca lo manda por sockets a la memoria correspondiente y en caso de error modifica la variable huboError
				if (!strcmp(unaTabla->CONSISTENCY, "SC")) {
					//strongConsistency->socket
					char *value = pedirValue(tabla, key,
							strongConsistency->socket);
					//printf("El value es: %s\n", value);
				} else if (!strcmp(unaTabla->CONSISTENCY, "SHC")) {
					if (list_size(hashConsistency) != 0) {
						char* key = parametros[2];
						int numeroMemoria = funcionHash(atoi(key));

						int esLaMemoriaBuscada(struct datosMemoria* unaMemoria) {
							return unaMemoria->MEMORY_NUMBER == numeroMemoria;
						}
						struct datosMemoria *unaMemoria = list_find(
								hashConsistency, (void*) esLaMemoriaBuscada);

						char * value = pedirValue(tabla, key,
								unaMemoria->socket);
						printf("El value es: %s\n", value);
					} else {
						char *value = pedirValue(tabla, key,
								strongConsistency->socket);
						printf("El value es: %s\n", value);
					}
				} else { //EC
					if (list_size(eventualConsistency) != 0) {
						struct datosMemoria *unaMemoria = list_remove(
								eventualConsistency, 0);
						char* value = pedirValue(tabla, key,
								unaMemoria->socket);
						printf("El value es: %s\n", value);
						list_add(eventualConsistency, unaMemoria);
					} else {
						char *value = pedirValue(tabla, key,
								strongConsistency->socket);
						printf("El value es: %s\n", value);
					}
				}

				*huboError = 0;
			}
		}

		else {
			*huboError = 1;
		}
		break;

	case INSERT:
		;
		//printf("Seleccionaste Insert\n");
		int criterioInsert(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			char *tabla = parametros[1];
			string_to_upper(tabla);
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			if (!dictionary_has_key(tablas_conocidas, tabla)) {
				char *mensajeALogear = string_new();
				string_append(&mensajeALogear,
						"No se tiene conocimiento de la tabla: ");
				string_append(&mensajeALogear, tabla);
				g_logger = log_create("./tablasNoEncontradas", "KERNEL", 1,
						LOG_LEVEL_ERROR);
				log_error(g_logger, mensajeALogear);
				free(mensajeALogear);
			}

			return esUnNumero(key)
					&& dictionary_has_key(tablas_conocidas, tabla);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			printf("Envio el comando INSERT a memoria\n");
			if (es_request) {
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo, "w");
				fprintf(temp, "%s %s %s %s", "INSERT", parametros[1],
						parametros[2], parametros[3]);
				fclose(temp);
				planificador(nombre_archivo);
			} else {
				char *tabla = parametros[1];
				char* key = parametros[2];
				char* value = parametros[3];

				struct tabla *unaTabla = dictionary_get(tablas_conocidas,
						tabla);
				if (!strcmp(unaTabla->CONSISTENCY, "SC")) {
					//strongConsistency->socket
					//printf("HOLA\n");
					//printf("SC socket: %d\n", strongConsistency->socket);
					mandarInsert(tabla, key, value, strongConsistency->socket);
				} else if (!strcmp(unaTabla->CONSISTENCY, "SHC")) {
					if (list_size(hashConsistency) != 0) {
						char* key = parametros[2];
						int numeroMemoria = funcionHash(atoi(key));

						int esLaMemoriaBuscada(struct datosMemoria* unaMemoria) {
							return unaMemoria->MEMORY_NUMBER == numeroMemoria;
						}
						struct datosMemoria *unaMemoria = list_find(
								hashConsistency, (void*) esLaMemoriaBuscada);

						clock_t tiempoInsert = clock();
						generarMetrica(tiempoInsert, 1,
								unaMemoria->direccionSocket.sin_addr.s_addr);
						mandarInsert(tabla, key, value, unaMemoria->socket);
					} else {
						mandarInsert(tabla, key, value,
								strongConsistency->socket);
					}
				} else { //EC
					if (list_size(eventualConsistency) != 0) {
						struct datosMemoria *memoriaRandom = list_remove(
								eventualConsistency, 0);
						mandarInsert(tabla, key, value, memoriaRandom->socket);
						list_add(eventualConsistency, memoriaRandom);
					} else {
						mandarInsert(tabla, key, value,
								strongConsistency->socket);
					}
				}

				*huboError = 0;
			}

		} else {
			*huboError = 1;
		}
		break;
	case CREATE:
		;
		//printf("Seleccionaste Create\n");
		int criterioCreate(char** parametros, int cantidadDeParametrosUsados) {
			char* tabla = parametros[1];
			char* tiempoCompactacion = parametros[4];
			char* cantidadParticiones = parametros[3];
			char* consistencia = parametros[2];
			if (!esUnNumero(cantidadParticiones)) {
				printf("La cantidad de particiones debe ser un numero.\n");
			}
			if (!esUnNumero(tiempoCompactacion)) {
				printf("El tiempo de compactacion debe ser un numero.\n");
			}

			if (dictionary_has_key(tablas_conocidas, tabla)) {
				char *mensajeALogear = string_new();
				string_append(&mensajeALogear, "Ya existe la tabla: ");
				string_append(&mensajeALogear, tabla);
				g_logger = log_create("./erroresCreate", "KERNEL", 1,
						LOG_LEVEL_ERROR);
				log_error(g_logger, mensajeALogear);
				free(mensajeALogear);
			}

			return esUnNumero(cantidadParticiones)
					&& esUnNumero(tiempoCompactacion)
					&& esUnTipoDeConsistenciaValida(consistencia)
					&& !dictionary_has_key(tablas_conocidas, tabla);
		}
		if (parametrosValidos(4, parametros, (void *) criterioCreate)) {
			printf("Enviando CREATE a memoria.\n");
			if (es_request) {
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo, "w");
				fprintf(temp, "%s %s %s %s %s", "CREATE", parametros[1],
						parametros[2], parametros[3], parametros[4]);
				fclose(temp);
				planificador(nombre_archivo);
			} else {
				char* tabla = parametros[1];
				string_to_upper(tabla);
				char* tiempoCompactacion = parametros[4];
				char* cantidadParticiones = parametros[3];
				char* consistencia = parametros[2];

				if (!strcmp(consistencia, "SC")) {
					//strongConsistency->socket
					mandarCreate(tabla, consistencia, cantidadParticiones,
							tiempoCompactacion, strongConsistency->socket);
				} else if (!strcmp(consistencia, "SHC")) {
					if (list_size(hashConsistency) != 0) {
						char* cantidadDeParticiones = parametros[3];
						int numeroMemoria = funcionHash(
								atoi(cantidadDeParticiones));

						int esLaMemoriaBuscada(struct datosMemoria* unaMemoria) {
							return unaMemoria->MEMORY_NUMBER == numeroMemoria;
						}
						struct datosMemoria *unaMemoria = list_find(
								hashConsistency, (void*) esLaMemoriaBuscada);

						mandarCreate(tabla, consistencia, cantidadParticiones,
								tiempoCompactacion, unaMemoria->socket);
					} else {
						mandarCreate(tabla, consistencia, cantidadParticiones,
								tiempoCompactacion, strongConsistency->socket);
					}
				} else { //EC
					if (list_size(eventualConsistency) != 0) {
						struct datosMemoria* memoriaRandom = list_remove(
								eventualConsistency, 0);
						mandarCreate(tabla, consistencia, cantidadParticiones,
								tiempoCompactacion, memoriaRandom->socket);
						list_add(eventualConsistency, memoriaRandom);
					} else {
						mandarCreate(tabla, consistencia, cantidadParticiones,
								tiempoCompactacion, strongConsistency->socket);
					}
				}
				*huboError = 0;
				struct tabla *unaTabla = malloc(sizeof(struct tabla));
				unaTabla->COMPACTION_TIME = atoi(tiempoCompactacion);
				unaTabla->CONSISTENCY = consistencia;
				unaTabla->PARTITIONS = atoi(cantidadParticiones);
				dictionary_put(tablas_conocidas, tabla, unaTabla);
			}
		} else {
			*huboError = 1;
		}
		break;
	case DESCRIBE:
		;
		//printf("Seleccionaste Describe\n");
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
			if (!dictionary_has_key(tablas_conocidas, tabla)) {
				printf("La tabla no existe\n");
			}
			return dictionary_has_key(tablas_conocidas, tabla);
		}
		if (parametrosValidos(0, parametros,
				(void *) criterioDescribeTodasLasTablas)) {
			//strongConsistency->socket
			guardarDiccionarioGlobal(strongConsistency->socket);//Pido las nuevas tablas y las guardo en el diccionario temporal
			dictionary_iterator(diccionarioDeTablasTemporal,
					(void*) actualizarDiccionarioDeTablas); //actualizo el propio
			dictionary_iterator(tablas_conocidas,
					(void*) quitarDelDiccionarioDeTablasLaTablaBorrada);
			*huboError = 0;
			if (es_request) {
				describeTodasLasTablas();
			}
		} else if (parametrosValidos(1, parametros,
				(void *) criterioDescribeUnaTabla)) {
			char* tabla = parametros[1];
			string_to_upper(tabla);
			struct tabla *metadata = pedirDescribeUnaTabla(tabla,
					strongConsistency->socket);

			/*printf("%s: \n", tabla);
			 printf("Particiones: %i\n", metadata->PARTITIONS);
			 printf("Consistencia: %s\n", metadata->CONSISTENCY);
			 printf("Tiempo de compactacion: %i\n\n",
			 metadata->COMPACTION_TIME);*/

			actualizarDiccionarioDeTablas(tabla, metadata);
			if (es_request) {
				describeUnaTabla(tabla);
			}
			*huboError = 0;
		} else {
			*huboError = 1;
		}
		break;
	case DROP:
		;
		//printf("Seleccionaste Drop\n");
		int criterioDrop(char** parametros, int cantidadDeParametrosUsados) {
			char* tabla = parametros[1];
			string_to_upper(tabla);

			/*void imprimirTabla(char *tabla){
			 printf("\t%s", tabla);
			 }
			 dictionary_iterator(tablas_conocidas, (void*)imprimirTabla);*/

			if (!dictionary_has_key(tablas_conocidas, tabla)) {
				char *mensajeALogear = string_new();
				string_append(&mensajeALogear,
						"No se tiene conocimiento de la tabla: ");
				string_append(&mensajeALogear, tabla);
				g_logger = log_create("./tablasNoEncontradas", "KERNEL", 1,
						LOG_LEVEL_ERROR);
				log_error(g_logger, mensajeALogear);
				free(mensajeALogear);
			}
			return dictionary_has_key(tablas_conocidas, tabla);
		}
		if (parametrosValidos(1, parametros, (void *) criterioDrop)) {
			char* tabla = parametros[1];
			string_to_upper(tabla);
			*huboError = 0;
			if (es_request) {
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo, "w");
				fprintf(temp, "%s %s", "DROP", parametros[1]);
				fclose(temp);
				planificador(nombre_archivo);
			} else {
				char *tabla = parametros[1];
				struct tabla *unaTabla = dictionary_get(tablas_conocidas,
						tabla);
				if (!strcmp(unaTabla->CONSISTENCY, "SC")) {
					//strongConsistency->socket
					mandarDrop(tabla, strongConsistency->socket);
				} else if (!strcmp(unaTabla->CONSISTENCY, "SHC")) {
					if (list_size(hashConsistency) != 0) {
						//le sumo 1 por la manera en que se enumeran las memorias
						int max = list_size(hashConsistency);
						int numeroMemoria = (rand() % max) + 1;
						//printf("MEMORIA: %i\n", numeroMemoria);
						int esLaMemoriaBuscada(struct datosMemoria* unaMemoria) {
							return unaMemoria->MEMORY_NUMBER == numeroMemoria;
						}
						struct datosMemoria *unaMemoria = list_find(
								hashConsistency, (void*) esLaMemoriaBuscada);

						mandarDrop(tabla, unaMemoria->socket);
					} else {
						mandarDrop(tabla, strongConsistency->socket);
					}
				} else { //EC
					if (list_size(eventualConsistency) != 0) {
						struct datosMemoria *memoriaRandom = list_remove(
								eventualConsistency, 0);
						mandarDrop(tabla, memoriaRandom->socket);
						list_add(eventualConsistency, memoriaRandom);
					} else {
						mandarDrop(tabla, strongConsistency->socket);
					}
				}
				dictionary_remove(tablas_conocidas, tabla);
			}
		} else {
			*huboError = 1;
		}
		break;
	case JOURNAL:
		if (es_request) {
			char* nombre_archivo = tempSinAsignar();
			FILE* temp = fopen(nombre_archivo, "w");
			fprintf(temp, "%s", "JOURNAL");
			fclose(temp);
			planificador(nombre_archivo);
		}

		else {
			//strongConsistency->socket
			list_iterate(listaDeMemorias, (void*) mandarJournal); ///revisar a quien se lo mando
		}
		break;
	case ADD:
		;
		//printf("Seleccionaste Add\n");
		int criterioAdd(char** parametros, int cantidadDeParametrosUsados) {
			char *consistencia = parametros[4];
			char* to = parametros[3];
			char* numeroMemoria = parametros[2];
			char* memoria = parametros[1];
			string_to_upper(memoria);
			string_to_upper(to);
			if (strcmp(memoria, "MEMORY")) {
				printf(
						"El primer parametro de add tiene que ser \"MEMORY\".\n");
			}
			if (strcmp(to, "TO")) {
				printf("El tercer parametro de add tiene que ser \"TO\".\n");
			}
			if (!esUnNumero(numeroMemoria)) {
				printf(
						"El segundo parametro tiene que ser un numero de memoria valido.\n");
			}

			int seEncuentraLaMemoria(struct datosMemoria* unaMemoria) {
				return atoi(numeroMemoria) == unaMemoria->MEMORY_NUMBER;
			}
			if (!list_any_satisfy(listaDeMemorias,
					(void*) seEncuentraLaMemoria)) {
				printf("No existe tal memoria.\n");
			}

			return !strcmp(memoria, "MEMORY") && !strcmp(to, "TO")
					&& esUnTipoDeConsistenciaValida(consistencia)
					&& esUnNumero(numeroMemoria)
					&& list_any_satisfy(listaDeMemorias,
							(void*) seEncuentraLaMemoria);
		}
		if (parametrosValidos(4, parametros, (void *) criterioAdd)) {
			char *consistencia = parametros[4];
			string_to_upper(consistencia);
			if (es_request) {
				char* nombre_archivo = tempSinAsignar();
				FILE* temp = fopen(nombre_archivo, "w");
				fprintf(temp, "%s %s %s %s %s", "ADD", parametros[1],
						parametros[2], parametros[3], parametros[4]);
				fclose(temp);
				planificador(nombre_archivo);
			}

			else {
				int numeroMemoria = atoi(parametros[2]);

				int esLaMemoriaBuscada(struct datosMemoria* unaMemoria) {
					return numeroMemoria == unaMemoria->MEMORY_NUMBER;
				}

				if (!strcmp(consistencia, "SC")) {
					strongConsistency->direccionSocket =
							((struct datosMemoria*) list_find(listaDeMemorias,
									(void*) esLaMemoriaBuscada))->direccionSocket; //unaMemoria->direccionSocket;
					strongConsistency->socket =
							((struct datosMemoria*) list_find(listaDeMemorias,
									(void*) esLaMemoriaBuscada))->socket; //unaMemoria->socket;
					strongConsistency->MEMORY_NUMBER =
							((struct datosMemoria*) list_find(listaDeMemorias,
									(void*) esLaMemoriaBuscada))->MEMORY_NUMBER;
				} else if (!strcmp(consistencia, "SHC")) {
					list_add(hashConsistency,
							list_find(listaDeMemorias,
									(void*) esLaMemoriaBuscada));
				} else if (!strcmp(consistencia, "EC")) {
					list_add(eventualConsistency,
							list_find(listaDeMemorias,
									(void*) esLaMemoriaBuscada));
				}
			}
			*huboError = 0;
		} else {
			*huboError = 1;
		}
		break;
	case RUN:
		;
		//printf("Seleccionaste Run\n");
		if (parametros[1] == NULL) {
			printf("No elegiste archivo.\n");
		} else {
			if (parametros[2] != NULL) {
				printf(
						"No deberia haber mas de un parametro, pero soy groso y puedo encolar de todas formas.\n");
			}
			char* nombre_del_archivo = parametros[1];
			string_trim(&nombre_del_archivo);
			/*if(!string_contains(nombre_del_archivo,".lql")){
			 string_append(&nombre_del_archivo,".lql");
			 }*/
			char *rutaDelArchivo = string_new();
			string_append(&rutaDelArchivo, "./scripts/");
			string_append(&rutaDelArchivo, nombre_del_archivo);
			//printf("%s", rutaDelArchivo);
			FILE* archivo = fopen(rutaDelArchivo, "r");
			if (archivo == NULL) {
				printf("El archivo no existe\n");
			} else {
				fclose(archivo);
				printf("Enviando Script a ejecutar.\n");
				planificador(rutaDelArchivo);
			}
		}
		break;

	case METRICS:
		metrics(0);
		break;

	default:
		*huboError = 1;
		printf("Error operacion invalida\n");
	}
}

void mandarDrop(char *tabla, int socketMemoria) {
	void* buffer = malloc(sizeof(int) + sizeof(int) + strlen(tabla));
	int peticion = 5;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla) + 1;
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, tamanioTabla);

	send(socketMemoria, buffer, 3 * sizeof(int) + tamanioTabla, 0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(socketMemoria, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(socketMemoria, ok, *tamanioRespuesta);

	if (*ok == 0) {
		char* mensajeALogear = malloc(
		strlen(" No se pudo realizar DROP de tabla : ") + strlen(tabla)	+ 1);
		strcpy(mensajeALogear, " No se pudo realizar DROP de tabla : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc( strlen(" Se realizo DROP de tabla : ") + strlen(tabla) + 1);
		strcpy(mensajeALogear, " Se realizo DROP de tabla : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
}

void mandarJournal(int socketMemoria) {
	void* buffer = malloc(sizeof(int));

	int peticion = 7;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	send(socketMemoria, buffer, 2 * sizeof(int), 0);
}

void guardarDiccionarioGlobal(int socketMemoria) {
	dictionary_clean(diccionarioDeTablasTemporal);

	void* buffer = malloc(sizeof(int));

	int peticion = 6;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));
	send(socketMemoria, buffer, 2 * sizeof(int), 0);

	// deserializo lo que me devuelve memoria (que a su vez se lo mando el fs)
	// deserializo
	int *tamanioTabla = malloc(sizeof(int));
	read(socketMemoria, tamanioTabla, sizeof(int));
	while (*tamanioTabla != 0) {
		char *tabla = malloc(*tamanioTabla);
		read(socketMemoria, tabla, *tamanioTabla);
		char *tablaCortada = string_substring_until(tabla, *tamanioTabla);
		//printf("%s\n", tablaCortada);

		int *tamanioConsistencia = malloc(sizeof(int));
		read(socketMemoria, tamanioConsistencia, sizeof(int));
		char *tipoConsistencia = malloc(*tamanioConsistencia);
		read(socketMemoria, tipoConsistencia, *tamanioConsistencia);
		char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia,
				*tamanioConsistencia);
		//printf("%s\n", tipoConsistenciaCortada);

		int* tamanioNumeroParticiones = malloc(sizeof(int));
		read(socketMemoria, tamanioNumeroParticiones, sizeof(int));
		int* numeroParticiones = malloc(*tamanioNumeroParticiones);
		read(socketMemoria, numeroParticiones, *tamanioNumeroParticiones);
		//printf("%i\n", *numeroParticiones);

		int* tamanioTiempoCompactacion = malloc(sizeof(int));
		read(socketMemoria, tamanioTiempoCompactacion, sizeof(int));
		int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
		read(socketMemoria, tiempoCompactacion, *tamanioTiempoCompactacion);
		//printf("%i\n", *tiempoCompactacion);

		char* mensajeALogear = malloc(
				strlen(" [DESCRIBE GLOBAL]:    ") + strlen(tablaCortada)
						+ strlen(tipoConsistenciaCortada) + 2 * sizeof(int)
						+ 1);
		strcpy(mensajeALogear, " [DESCRIBE GLOBAL]: ");
		strcat(mensajeALogear, tablaCortada);
		strcat(mensajeALogear, " ");
		strcat(mensajeALogear, tipoConsistenciaCortada);
		strcat(mensajeALogear, " ");
		strcat(mensajeALogear, string_itoa(*numeroParticiones));
		strcat(mensajeALogear, " ");
		strcat(mensajeALogear, string_itoa(*tiempoCompactacion));
		t_log* g_logger;
		g_logger = log_create("./logs.log", "KERNEL", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);

		//lo guardo en el diccionario
		struct tabla* data = malloc(8 + 4);    // 2 int = 2*4 bytes
		data->CONSISTENCY = malloc(*tamanioConsistencia + 1);
		memcpy(&data->PARTITIONS, numeroParticiones, sizeof(int));
		memcpy(data->CONSISTENCY, tipoConsistenciaCortada,
				*tamanioConsistencia + 1);
		memcpy(&data->COMPACTION_TIME, tiempoCompactacion, sizeof(int));
		dictionary_put(diccionarioDeTablasTemporal, tablaCortada, data);

		/* para probar que funciona
		 struct tabla* metadata2;
		 metadata2 = dictionary_get(diccionarioDeTablasTemporal, tablaCortada);
		 printf("--%i", metadata2->COMPACTION_TIME);
		 printf("--%i", metadata2->PARTITIONS);
		 printf("--%s", metadata2->CONSISTENCY);
		 */
		read(socketMemoria, tamanioTabla, sizeof(int));
	}
}

struct tabla *pedirDescribeUnaTabla(char* tabla, int socketMemoria) {
	void* buffer = malloc(sizeof(int) + sizeof(int) + strlen(tabla));

	int peticion = 4;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla) + 1;
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, tamanioTabla);

	send(socketMemoria, buffer, 3 * sizeof(int) + tamanioTabla, 0);

	//Deserializo
	int *tamanioConsistencia = malloc(sizeof(int));
	read(socketMemoria, tamanioConsistencia, sizeof(int));
	char *tipoConsistencia = malloc(*tamanioConsistencia);
	read(socketMemoria, tipoConsistencia, *tamanioConsistencia);

	int* tamanioNumeroParticiones = malloc(sizeof(int));
	read(socketMemoria, tamanioNumeroParticiones, sizeof(int));
	int* numeroParticiones = malloc(*tamanioNumeroParticiones);
	read(socketMemoria, numeroParticiones, *tamanioNumeroParticiones);

	int* tamanioTiempoCompactacion = malloc(sizeof(int));
	read(socketMemoria, tamanioTiempoCompactacion, sizeof(int));
	int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
	read(socketMemoria, tiempoCompactacion, *tamanioTiempoCompactacion);
	// aca ya tengo toda la metadata, falta guardarla en struct

	char* mensajeALogear = malloc(	strlen(" [DESCRIBE x tabla]:  ") + strlen(tabla) + strlen(tipoConsistencia) + 2 * sizeof(int)	+ 1);
	strcpy(mensajeALogear, " [DESCRIBE x tabla]:  ");
	strcat(mensajeALogear, tabla);
	strcat(mensajeALogear, " ");
	strcat(mensajeALogear, tipoConsistencia);
	strcat(mensajeALogear, " ");
	strcat(mensajeALogear, string_itoa(*numeroParticiones));
	strcat(mensajeALogear, " ");
	strcat(mensajeALogear, string_itoa(*tiempoCompactacion));
	t_log* g_logger;
	g_logger = log_create("./logs.log", "KERNEL", 1, LOG_LEVEL_INFO);
	log_info(g_logger, mensajeALogear);
	log_destroy(g_logger);
	free(mensajeALogear);

	struct tabla* data = malloc(8 + 4);    // 2 int = 2*4 bytes
	data->CONSISTENCY = malloc(*tamanioConsistencia);
	memcpy(&data->PARTITIONS, numeroParticiones, sizeof(int));
	memcpy(data->CONSISTENCY, tipoConsistencia, *tamanioConsistencia);
	memcpy(&data->COMPACTION_TIME, tiempoCompactacion, sizeof(int));

	return data;
}

void mandarInsert(char* tabla, char* key, char* value, int socketMemoria) {
	void* buffer = malloc(
			strlen(tabla) + 1 + strlen(key) + 1 + strlen(value) + 1
					+ 5 * sizeof(int));

	int peticion = 2;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla) + 1;
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, tamanioTabla);

	int tamanioKey = strlen(key) + 1;
	memcpy(buffer + 3 * sizeof(int) + tamanioTabla, &tamanioKey, sizeof(int));
	memcpy(buffer + 4 * sizeof(int) + tamanioTabla, key, tamanioKey);

	int tamanioValue = strlen(value) + 1;
	memcpy(buffer + 4 * sizeof(int) + tamanioTabla + tamanioKey, &tamanioValue,
			sizeof(int));
	memcpy(buffer + 5 * sizeof(int) + tamanioTabla + tamanioKey, value,
			tamanioValue);

	//No mandar el timestamp (no hay en kernel)
	send(socketMemoria, buffer,
			tamanioTabla + 5 * sizeof(int) + tamanioKey + tamanioValue, 0);

}

void mandarCreate(char *tabla, char *consistencia, char *cantidadParticiones,
		char *tiempoCompactacion, int socketMemoria) {
	int peticion = 3;
	void* buffer = malloc(
			strlen(tabla) + 1 + 6 * sizeof(int) + strlen(consistencia) + 1
					+ strlen(cantidadParticiones) + 1
					+ strlen(tiempoCompactacion) + 1);

	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla) + 1;
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, tamanioTabla);

	int tamanioConsistencia = strlen(consistencia) + 1;
	memcpy(buffer + 3 * sizeof(int) + tamanioTabla, &tamanioConsistencia,
			sizeof(int));
	memcpy(buffer + 4 * sizeof(int) + tamanioTabla, consistencia,
			tamanioConsistencia);

	int tamanioCantidadParticiones = strlen(cantidadParticiones) + 1;
	memcpy(buffer + 4 * sizeof(int) + tamanioTabla + tamanioConsistencia,
			&tamanioCantidadParticiones, sizeof(int));
	memcpy(buffer + 5 * sizeof(int) + tamanioTabla + tamanioConsistencia,
			cantidadParticiones, tamanioCantidadParticiones);

	int tamanioTiempoCompactacion = strlen(tiempoCompactacion) + 1;
	memcpy(
			buffer + 5 * sizeof(int) + tamanioTabla + tamanioConsistencia
					+ tamanioCantidadParticiones, &tamanioTiempoCompactacion,
			sizeof(int));
	memcpy(
			buffer + 6 * sizeof(int) + tamanioTabla + tamanioConsistencia
					+ tamanioCantidadParticiones, tiempoCompactacion,
			tamanioTiempoCompactacion);
	send(socketMemoria, buffer,	tamanioTabla + 6 * sizeof(int) + tamanioConsistencia + tamanioCantidadParticiones + tamanioTiempoCompactacion, 0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(socketMemoria, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(socketMemoria, ok, *tamanioRespuesta);

	if (*ok == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se pudo realizar create en FS de tabla : ") + strlen(tabla)	+ 1);
		strcpy(mensajeALogear, " No se pudo realizar create en FS de tabla : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc(
				strlen(" Se realizo create en FS de : ") + strlen(tabla) + 1);
		strcpy(mensajeALogear, " Se realizo create en FS de : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}

}

char* pedirValue(char* tabla, char* laKey, int socketMemoria) {
	int* key = malloc(sizeof(int));
	*key = atoi(laKey);

	char* buffer = malloc(strlen(tabla) + sizeof(int) + 2 * sizeof(int) + 2 * sizeof(int));
	// primeros dos terminos para TABLA; anteultimo termino para KEY; ultimo para peticion

	int peticion = 1;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla);
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, strlen(tabla));

	int tamanioKey = sizeof(int);
	memcpy(buffer + 3 * sizeof(int) + strlen(tabla), &tamanioKey, sizeof(int));
	memcpy(buffer + 4 * sizeof(int) + strlen(tabla), key, sizeof(int));

	send(socketMemoria, buffer, strlen(tabla) + 5 * sizeof(int), 0);

	//deserializo value
	int *tamanioValue = malloc(sizeof(int));
	recv(socketMemoria, tamanioValue, sizeof(int), 0);

	if (*tamanioValue == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se encontro ni en MM ni en FS la key : ")
						+ strlen(laKey) + 1);
		strcpy(mensajeALogear, " No se encontro ni en MM ni en FS la key : ");
		strcat(mensajeALogear, laKey);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		return NULL;
	} else {
		char *value = malloc(*tamanioValue);
		recv(socketMemoria, value, *tamanioValue, 0);
		char *valueCortado = string_substring_until(value, *tamanioValue); //corto value

		char* mensajeALogear = malloc(
				strlen(" Llego select con VALUE : ") + strlen(valueCortado)
						+ 1);
		strcpy(mensajeALogear, " Llego select con VALUE : ");
		strcat(mensajeALogear, valueCortado);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Kernel", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		free(value);
		return valueCortado;
	}
}

void generarMetrica(clock_t tiempoInicial, int tipoDeMetrica, char* IPMemoria) {
	struct metricas * prueba;
	unRegistro = malloc(sizeof(struct metricas));
	clock_t tiempoFinal = clock() - tiempoInicial;
	unRegistro->segundosDeEjecucion = ((double) tiempoFinal) / CLOCKS_PER_SEC;
	unRegistro->tipoDeMetric = tipoDeMetrica;
	unRegistro->tiempoDeCreacion = clock();
	unRegistro->IPMemoria = string_new();
	strcpy(unRegistro->IPMemoria, IPMemoria);
	list_add(listaMetricas, unRegistro);
	printf("\n ");
	prueba = list_get(listaMetricas, 0);
	printf("%i", prueba->tipoDeMetric);
	printf("%s", prueba->IPMemoria);
}

int parametrosValidos(int cantidadDeParametrosNecesarios, char** parametros,
		int (*criterioTiposCorrectos)(char**, int)) {
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios)
			&& criterioTiposCorrectos(parametros,
					cantidadDeParametrosNecesarios);;
}

void quitarDelDiccionarioDeTablasLaTablaBorrada(char *tabla) {
	if (!dictionary_has_key(diccionarioDeTablasTemporal, tabla)) {
		dictionary_remove(tablas_conocidas, tabla);
	}
}

void describeUnaTabla(char* tabla) {
	if (dictionary_has_key(tablas_conocidas, tabla)) {
		struct tabla *metadataTabla = dictionary_get(tablas_conocidas, tabla);
		printf("%s: \n", tabla);
		printf("Particiones: %i\n", metadataTabla->PARTITIONS);
		printf("Consistencia: %s\n", metadataTabla->CONSISTENCY);
		printf("Tiempo de compactacion: %i\n\n",
				metadataTabla->COMPACTION_TIME);
	}

}

void describeTodasLasTablas() {
	dictionary_iterator(tablas_conocidas, (void*) describeUnaTabla);
}

int32_t conectarUnaMemoria(struct datosMemoria *unaMemoria, char* IP_MEMORIA,
		int PUERTO_MEMORIA) {

	//socketMemoriaPrincipal = socket(AF_INET, SOCK_STREAM, 0);

	/*struct sockaddr_in direccion_server_memoria_kernel;
	 direccion_server_memoria_kernel.sin_family = AF_INET;
	 direccion_server_memoria_kernel.sin_port = htons(PUERTO_MEMORIA);
	 direccion_server_memoria_kernel.sin_addr.s_addr = INADDR_ANY;*/

	/*strongConsistency->direccionSocket.sin_family = AF_INET;
	 strongConsistency->direccionSocket.sin_port = htons(PUERTO_MEMORIA);
	 strongConsistency->direccionSocket.sin_addr.s_addr = INADDR_ANY;*/
	unaMemoria->socket = socket(AF_INET, SOCK_STREAM, 0);

	unaMemoria->direccionSocket.sin_family = AF_INET;
	unaMemoria->direccionSocket.sin_port = htons(PUERTO_MEMORIA);
	//unaMemoria->direccionSocket.sin_addr.s_addr = inet_addr(IP_MEMORIA); XXX
	unaMemoria->direccionSocket.sin_addr.s_addr = INADDR_ANY;

	if (connect(unaMemoria->socket,
			(struct sockaddr *) &unaMemoria->direccionSocket,
			sizeof(unaMemoria->direccionSocket)) == -1) {
		perror("Hubo un error en la conexion");
		return -1;
	}

	// strongConsistency = &unaMemoriaStrongConsistency;

	/*if(connect(socketMemoriaPrincipal, (struct sockaddr *) &direccion_server_memoria_kernel, sizeof(direccion_server_memoria_kernel)) == -1)
	 {
	 perror("Hubo un error en la conexion");
	 return -1;
	 }*/
	//strongConsistency->direccionSocket = direccion_server_memoria_kernel;
	//strongConsistency->socket = socketMemoriaPrincipal;
	//Mando un numero distinto de cero a memoria para que sepa que se conecto kernel
	//send(unaMemoria->socket, "1", 2, 0);

	/*char buffer[256];
	 int leng = recv(unaMemoriaStrongConsistency.socket, &buffer, sizeof(buffer), 0);
	 buffer[leng] = '\0';

	 printf("RECIBI INFORMACION DE LA MEMORIA: %s\n", buffer);*/

	//Mandar Mensajes
	/*while (1) {
	 char* mensaje = malloc(1000);
	 fgets(mensaje, 1024, stdin);
	 send(socketMemoriaPrincipal, mensaje, strlen(mensaje), 0);
	 free(mensaje);
	 }*/

	//close(socketMemoriaPrincipal);
	return 0;
}

//revisar esto si se puede mejorar
void ejecutarReady() {
	while (1) {

		if (enEjecucionActualmente < multiprocesamiento
				&& !queue_is_empty(ready)) {
			enEjecucionActualmente++;
			struct Script *unScript = queue_pop(ready);
			//Creo los hilos de ejecucion bajo demanda
			pthread_t hiloScript;
			pthread_create(&hiloScript, NULL, (void*) ejecutor,
					(void*) unScript);
			pthread_detach(hiloScript);
			//ejecutor(unScript);
			enEjecucionActualmente--;
		}
	}
}

void actualizarDiccionarioDeTablas(char *tabla, struct tabla *metadata) {
	if (dictionary_has_key(tablas_conocidas, tabla)) {
		dictionary_remove(tablas_conocidas, tabla);
		dictionary_put(tablas_conocidas, tabla, metadata);
	} else {
		dictionary_put(tablas_conocidas, tabla, metadata);
	}
}

void ejecutor(struct Script *ejecutando) {
	printf("El script %s esta en ejecucion!\n", ejecutando->peticiones);
	char* caracter = (char *) malloc(sizeof(char) + 1);
	//le copio algo adentro porque queda con basura sino
	strcpy(caracter, "0");
	int quantum = config_get_int_value(configuracion, "QUANTUM");
	int error = 0;
	int i = 0;
	FILE * lql = fopen(ejecutando->peticiones, "r");
	fseek(lql, ejecutando->posicionActual, 0);

	/*char* lineaDeScript = string_new();
	 fread(caracter, sizeof(char), 1, lql);*/

	while (i < quantum && !feof(lql) && !error) {
		char* lineaDeScript = string_new();
		fread(caracter, sizeof(char), 1, lql);
		//leo caracter a caracter hasta encontrar \n
		while (!feof(lql) && strcmp(caracter, "\n")) {
			string_append(&lineaDeScript, caracter);
			fread(caracter, sizeof(char), 1, lql);
		}
		//printf("\t%s\n", lineaDeScript);
		tomar_peticion(lineaDeScript, 0, &error);
		free(lineaDeScript);
		i++;
	}
	//printf("\t%i\n", feof(lql));
	//Si salio por quantum
	if (i >= quantum && !error && !feof(lql)) {
		printf("---Fin q---\n");
		ejecutando->posicionActual = ftell(lql);

		int sleepEjecucion = config_get_int_value(configuracion,
				"SLEEP_EJECUCION");
		sleep(sleepEjecucion);
		queue_push(ready, ejecutando);
	} else {
		sem_post(&MAXIMOPROCESAMIENTO);
		printf("el script %s paso a exit\n", ejecutando->peticiones);
	}
	fclose(lql);
	free(caracter);
}

char* tempSinAsignar() {
	char * nombre = string_new();
	char * strNumero = string_new();
	int numero = numeroSinUsar();

	strNumero = string_itoa(numero);

	string_append(&nombre, "temp");
	string_append(&nombre, strNumero);
	string_append(&nombre, ".lql");
	return nombre;
}

void planificador(char* nombre_del_archivo) {
	pasar_a_new(nombre_del_archivo);
	pasar_a_ready(new);
}

void popear(struct Script * proceso, t_queue* cola) {
	proceso = queue_pop(cola);
}

void pasar_a_new(char* nombre_del_archivo) { //Prepara las estructuras necesarias y pushea el request a la cola de new
	//printf("\tAgregando script a cola de New\n");
	struct Script *proceso = malloc(sizeof(struct Script));
	proceso->PID = get_PID(PIDs);
	proceso->PC = 0;
	proceso->peticiones = string_new();
	proceso->posicionActual = 0;
	string_append(&proceso->peticiones, nombre_del_archivo);
	queue_push(new, proceso);
	printf("El script %s paso a new\n", proceso->peticiones);
}

int PID_usada(int numPID) {
	bool _PID_en_uso(void* PID) {
		return (int) PID == numPID;
	}
	return (list_find(PIDs, _PID_en_uso) != NULL);
}

void pasar_a_ready(t_queue * colaAnterior) {
	//printf("\tTrasladando script a Ready\n");
	sem_wait(&MAXIMOPROCESAMIENTO);
	struct Script *proceso = queue_pop(colaAnterior);
	queue_push(ready, proceso);
	printf("El script %s fue trasladado a Ready\n", proceso->peticiones);
}
