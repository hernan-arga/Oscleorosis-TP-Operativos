#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<commons/config.h>
#include<unistd.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<pthread.h>
#include<commons/log.h>
#include<commons/collections/list.h>
#include<commons/temporal.h>
#include <commons/string.h>
#include <ctype.h>
#include<time.h>
#include<semaphore.h>
#include<errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, JOURNAL, OPERACIONINVALIDA
} OPERACION;

typedef struct {
	char* nombreTabla;
	t_list* paginas;
} segmento;

typedef struct {
	int numeroPag;
	bool modificado;
	int numeroFrame;
	long int timeStamp;
} pagina;

typedef struct {
	int particiones;
	char* consistencia;
	int tiempoCompactacion;
} metadataTabla;

typedef struct {
	int32_t PUERTO;
	int32_t PUERTO_FS;
	int32_t RETARDO_MEM;
	int32_t RETARDO_FS;
	int32_t TAM_MEM;
	int32_t RETARDO_JOURNAL;
	int32_t RETARDO_GOSSIPING;
	int32_t MEMORY_NUMBER;
	char** IP_SEEDS;
	char** PUERTO_SEEDS;
} archivoConfiguracion;

typedef struct {
	int socket;
	struct sockaddr_in direccionSocket;
	int32_t MEMORY_NUMBER;
} datosMemoria;

t_dictionary* tablaSegmentos;
char* memoriaPrincipal;
int tamanoFrame;
int tamanoValue;
int* frames;
t_list* clientes;

//Sockets
struct sockaddr_in serverAddress;
struct sockaddr_in serverAddressFS;
struct sockaddr_in direccionCliente;
int32_t server;
int32_t clienteFS;
uint32_t tamanoDireccion;

//Config
archivoConfiguracion t_archivoConfiguracion;
t_config *config;
int32_t activado = 1;

//Semaforos
sem_t sem;
sem_t sem2;
pthread_mutex_t SEMAFORODECONEXIONFS;
pthread_mutex_t SEMAFORODETABLASEGMENTOS;


//Hilos
pthread_t threadKernel;
pthread_t threadFS;

int hizoJ = 0;

void analizarInstruccion(char* instruccion);
void realizarComando(char** comando);
OPERACION tipoDePeticion(char* peticion);
char* realizarSelect(char* tabla, char* key);
int realizarInsert(char* tabla, char* key, char* value);
int frameLibre();
char* pedirValue(char* tabla, char* key);
int ejecutarLRU();
void ejecutarJournaling();
int realizarCreate(char* tabla, char* tipoConsistencia,
		char* numeroParticiones, char* tiempoCompactacion);
int realizarDrop(char* tabla);
void realizarDescribeGlobal();
metadataTabla* realizarDescribe(char* tabla);
void consola();
int serServidor();
void conectarseAFS();
void gossiping(int cliente);
void conectar();

void tomarPeticionSelect(int);
void tomarPeticionInsert(int);
void tomarPeticionCreate(int);
void tomarPeticionDescribe1Tabla(int);
void tomarPeticionDescribeGlobal(int);
void tomarPeticionDrop(int);

int main(int argc, char *argv[]) {

	sem_init(&sem, 1, 0);
	sem_init(&sem, 2, 0);

	clientes = list_create();

	config = config_create(argv[1]);

	t_archivoConfiguracion.PUERTO = config_get_int_value(config, "PUERTO");
	t_archivoConfiguracion.PUERTO_FS = config_get_int_value(config,
			"PUERTO_FS");
	t_archivoConfiguracion.IP_SEEDS = config_get_array_value(config,
			"IP_SEEDS");
	t_archivoConfiguracion.PUERTO_SEEDS = config_get_array_value(config,
			"PUERTO_SEEDS");
	t_archivoConfiguracion.RETARDO_MEM = config_get_int_value(config,
			"RETARDO_MEM");
	t_archivoConfiguracion.RETARDO_FS = config_get_int_value(config,
			"RETARDO_FS");
	t_archivoConfiguracion.TAM_MEM = config_get_int_value(config, "TAM_MEM");
	t_archivoConfiguracion.RETARDO_JOURNAL = config_get_int_value(config,
			"RETARDO_JOURNAL");
	t_archivoConfiguracion.RETARDO_GOSSIPING = config_get_int_value(config,
			"RETARDO_GOSSIPING");
	t_archivoConfiguracion.MEMORY_NUMBER = config_get_int_value(config,
			"MEMORY_NUMBER");

	pthread_t threadFS;
	int32_t idThreadFS = pthread_create(&threadFS, NULL, conectarseAFS, NULL);

	sem_wait(&sem);

	pthread_t threadSerServidor;
	int32_t idThreadSerServidor = pthread_create(&threadSerServidor, NULL,
			serServidor, NULL);

	tablaSegmentos = dictionary_create();

	memoriaPrincipal = malloc(t_archivoConfiguracion.TAM_MEM);

	tamanoFrame = sizeof(int) + sizeof(long int) + tamanoValue;
	//Key , TimeStamp, Value

	int tablaFrames[t_archivoConfiguracion.TAM_MEM / tamanoFrame];
	frames = tablaFrames;

	for (int i = 0; i < t_archivoConfiguracion.TAM_MEM / tamanoFrame; i++) {
		*(frames + i) = 0;
	}

	pthread_t threadConsola;
	int32_t idthreadConsola = pthread_create(&threadConsola, NULL, consola,
	NULL);

	pthread_t tConectar;
	int32_t idTConectar = pthread_create(&tConectar, NULL, conectar, NULL);

	pthread_join(threadConsola, NULL);
	pthread_join(threadSerServidor, NULL);
	pthread_join(threadFS, NULL);
}

void analizarInstruccion(char* instruccion) {
	char** comando = malloc(strlen(instruccion) + 1);
	comando = string_split(instruccion, " \n");
	realizarComando(comando);
	free(comando);
}

void realizarComando(char** comando) {
	char *peticion = comando[0];
	OPERACION accion = tipoDePeticion(peticion);
	char* tabla;
	char* key;
	char* value;
	switch (accion) {
	case SELECT:
		;
		//printf("SELECT\n");
		tabla = comando[1];
		key = comando[2];
		realizarSelect(tabla, key);
		break;

	case INSERT:
		;
		//printf("INSERT\n");
		tabla = comando[1];
		key = comando[2];
		value = comando[3];
		int i = 4;
		while (comando[i] != NULL) {
			string_append_with_format(&value, " %s", comando[i]);
			i++;
		}
		realizarInsert(tabla, key, value);
		break;

	case CREATE:
		;
		//printf("CREATE\n");
		tabla = comando[1];
		char* tipoConsistencia = comando[2];
		char* numeroParticiones = comando[3];
		char* tiempoCompactacion = comando[4];
		realizarCreate(tabla, tipoConsistencia, numeroParticiones,
				tiempoCompactacion);
		break;

		//Describe recibe un diccionario con (nombreTabla - struct(con la info de la metadata)

	case DESCRIBE:
		;
		//printf("DESCRIBE");

		if (comando[1] == NULL) {
			printf("GLOBAL\n");
			realizarDescribeGlobal();
		} else {
			printf("NORMAL\n");
			tabla = comando[1];
			realizarDescribe(tabla);
		}
		break;

	case DROP:
		;
		//printf("DROP\n");
		tabla = comando[1];
		realizarDrop(tabla);
		break;

	case JOURNAL:
		;
		//printf("JOURNAL\n");
		ejecutarJournaling();
		break;

	case OPERACIONINVALIDA:
		printf("OPERACION INVALIDA\n");
		break;
	}
}

OPERACION tipoDePeticion(char* peticion) {
	if (!strcmp(peticion, "SELECT")) {
		free(peticion);
		return SELECT;
	} else if (!strcmp(peticion, "INSERT")) {
		free(peticion);
		return INSERT;
	} else if (!strcmp(peticion, "CREATE")) {
		free(peticion);
		return CREATE;
	} else if (!strcmp(peticion, "DESCRIBE")) {
		free(peticion);
		return DESCRIBE;
	} else if (!strcmp(peticion, "DROP")) {
		free(peticion);
		return DROP;
	} else if (!strcmp(peticion, "JOURNAL")) {
		free(peticion);
		return JOURNAL;
	} else {
		free(peticion);
		return OPERACIONINVALIDA;
	}
}

char* realizarSelect(char* tabla, char* key) {
	if (dictionary_has_key(tablaSegmentos, tabla)) {
		sem_wait(&sem2);

		t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

		for (int i = 0; i < list_size(tablaPag); i++) {
			pagina* pag = list_get(tablaPag, i);

			int* laKey = malloc(sizeof(int));

			memcpy(laKey, (memoriaPrincipal + pag->numeroFrame * tamanoFrame),
					sizeof(int));

			if (*laKey == atoi(key)) {

				char* value = malloc(*(frames + pag->numeroFrame));

				memcpy(value,
						(memoriaPrincipal + pag->numeroFrame * tamanoFrame
								+ sizeof(int) + sizeof(long int)),
						*(frames + pag->numeroFrame) + 1);

				long int* timeStamp = malloc(sizeof(long int));
				*timeStamp = (long int) time(NULL);

				memcpy(
						(memoriaPrincipal + pag->numeroFrame * tamanoFrame
								+ sizeof(int)), timeStamp, sizeof(long int));

				free(timeStamp);

				//printf("Value: %s\n", value);

				pag->timeStamp = *timeStamp;

				//free(value);
				free(laKey);

				sem_post(&sem2);

				return value;
			}
			free(laKey);
		}
		int frameNum = frameLibre();

		long int* timeStamp = malloc(sizeof(long int));
		*timeStamp = (long int) time(NULL);
		pagina* pagp = malloc(sizeof(pagina));
		pagp->modificado = false;
		pagp->numeroFrame = frameNum;
		if (dictionary_has_key(tablaSegmentos, tabla)) {
			pagp->numeroPag = list_size(tablaPag);
		} else {
			pagp->numeroPag = 0;
		}
		pagp->timeStamp = *timeStamp;

		if (hizoJ == 1) {
			t_list* pagis = list_create();
			list_add(pagis, pagp);
			pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);
			dictionary_put(tablaSegmentos, tabla, pagis);
			pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);
			hizoJ = 0;
		} else {
			list_add(tablaPag, pagp);
		}

		char* value = malloc(tamanoValue);
		value = pedirValue(tabla, key);

		//printf("Value: %s", value);

		if (value == NULL) {
			sem_post(&sem2);

			return NULL;
		}

		*(frames + frameNum) = strlen(value);

		//printf("Value: %s\n", value);

		int* laKey = malloc(sizeof(int));
		*laKey = atoi(key);

		memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame), laKey,
				sizeof(int));
		memcpy(
				(memoriaPrincipal + pagp->numeroFrame * tamanoFrame
						+ sizeof(int) + sizeof(long int)), value,
				strlen(value));
		memcpy(
				(memoriaPrincipal + pagp->numeroFrame * tamanoFrame
						+ sizeof(int)), timeStamp, sizeof(long int));

		free(timeStamp);
		//free(value);

		sem_post(&sem2);
		return value;
	}

	char* value = pedirValue(tabla, key);

	//printf("Value: %s", value);

	if (value == NULL) {
		return NULL;
	}

	int frameNum = frameLibre();
	*(frames + frameNum) = strlen(value);

	pagina* pagp = malloc(sizeof(pagina));
	t_list* paginasp = list_create();

	long int* timeStamp = malloc(sizeof(long int));
	*timeStamp = (long int) time(NULL);

	pagp->modificado = false;
	pagp->numeroFrame = frameNum;
	pagp->numeroPag = 0;
	pagp->timeStamp = *timeStamp;

	list_add(paginasp, pagp);
	pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);

	dictionary_put(tablaSegmentos, tabla, paginasp);
	pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);

	int* laKey = malloc(sizeof(int));
	*laKey = atoi(key);

	memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame), laKey,
			sizeof(int));
	memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)),
			timeStamp, sizeof(long int));
	memcpy(
			(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)
					+ sizeof(long int)), value, strlen(value) + 1);

	free(timeStamp);
	//free(value);

	//printf("Value: %s", value);

	sem_post(&sem2);

	return value;
}

int realizarInsert(char* tabla, char* key, char* value) {

	sem_wait(&sem2);

	if (dictionary_has_key(tablaSegmentos, tabla)) {
		t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

		for (int i = 0; i < list_size(tablaPag); i++) {
			pagina* pagy = list_get(tablaPag, i);

			int* laKey = malloc(sizeof(int));

			memcpy(laKey, (memoriaPrincipal + pagy->numeroFrame * tamanoFrame),
					sizeof(int));

			if (*laKey == atoi(key)) {
				long int* timeStamp = malloc(sizeof(long int));

				*timeStamp = (long int) time(NULL);

				memcpy(
						(memoriaPrincipal + (pagy->numeroFrame * tamanoFrame)
								+ sizeof(int) + sizeof(long int)), value,
						strlen(value) + 1);
				memcpy(
						memoriaPrincipal + pagy->numeroFrame * tamanoFrame
								+ sizeof(int), timeStamp, sizeof(long int));

				*(frames + pagy->numeroFrame) = strlen(value);

				pagy->timeStamp = *timeStamp;

				free(laKey);
				free(timeStamp);

				sem_post(&sem2);

				return 0;
			}
		}

		int frameNum = frameLibre();
		*(frames + frameNum) = strlen(value);

		long int* timeStamp = malloc(sizeof(long int));
		*timeStamp = (long int) time(NULL);

		pagina* pagp = malloc(sizeof(pagina));

		pagp->modificado = true;
		pagp->numeroFrame = frameNum;
		if (dictionary_has_key(tablaSegmentos, tabla)) {
			pagp->numeroPag = list_size(tablaPag);
		} else {
			pagp->numeroPag = 0;
		}
		pagp->timeStamp = *timeStamp;

		if (hizoJ == 1) {
			t_list* pagis = list_create();
			list_add(pagis, pagp);
			pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);
			dictionary_put(tablaSegmentos, tabla, pagis);
			pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);

			hizoJ = 0;
		} else {
			list_add(tablaPag, pagp);
		}

		int* laKey = malloc(sizeof(int));
		*laKey = atoi(key);

		memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame, laKey,
				sizeof(int));
		memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int),
				timeStamp, sizeof(long int));
		memcpy(
				memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)
						+ sizeof(long int), value, strlen(value) + 1);

		free(timeStamp);

		sem_post(&sem2);

		return 0;
	}

	int frameNum = frameLibre();
	*(frames + frameNum) = strlen(value);

	long int* timeStamp = malloc(sizeof(long int));
	*timeStamp = (long int) time(NULL);

	pagina* pagp = malloc(sizeof(pagina));
	pagp->modificado = true;
	pagp->numeroFrame = frameNum;
	pagp->numeroPag = 0;
	pagp->timeStamp = *timeStamp;

	t_list* paginas = list_create();
	list_add(paginas, pagp);
	pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);

	dictionary_put(tablaSegmentos, tabla, paginas);
	pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);

	int* laKey = malloc(sizeof(int));
	*laKey = atoi(key);

	memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame, laKey,
			sizeof(int));
	memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int),
			timeStamp, sizeof(long int));
	memcpy(
			memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)
					+ sizeof(long int), value, strlen(value) + 1);

	free(timeStamp);

	sem_post(&sem2);

	return 0;
}

int frameLibre() {
	for (int i = 0; i < t_archivoConfiguracion.TAM_MEM / tamanoFrame; i++) {
		if (*(frames + i) == 0) {
			return i;
		}
	}
	printf("Ejecutar LRU");
	int frameLib = ejecutarLRU();

	return frameLib;
}

char* pedirValue(char* tabla, char* laKey) {
	// Serializo peticion, tabla y key
	int* key = malloc(sizeof(int));
	*key = atoi(laKey);

	char* buffer = malloc(
			strlen(tabla) + sizeof(int) + 2 * sizeof(int) + 2 * sizeof(int));
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

	pthread_mutex_lock(&SEMAFORODECONEXIONFS);

	send(clienteFS, buffer, strlen(tabla) + 5 * sizeof(int), 0);

	//deserializo value
	int *tamanioValue = malloc(sizeof(int));
	recv(clienteFS, tamanioValue, sizeof(int), 0);
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);

	//printf("Tamanio value: %d\n", *tamanioValue);

	if (*tamanioValue == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se encontro la key : ") + sizeof(key) + 1);
		strcpy(mensajeALogear, " No se encontro la key : ");
		strcat(mensajeALogear, laKey);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "LFS", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		perror("El value no estaba en el FS");
		return NULL;
	} else {
		char *value = malloc(*tamanioValue);
		recv(clienteFS, value, *tamanioValue, 0);
		char *valueCortado = string_substring_until(value, *tamanioValue); //corto value

		char* mensajeALogear = malloc(
				strlen(" Llego select con VALUE : ") + strlen(valueCortado)
						+ 1);
		strcpy(mensajeALogear, " Llego select con VALUE : ");
		strcat(mensajeALogear, valueCortado);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "Memoria", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		free(value);
		return valueCortado;
	}
}

int ejecutarLRU() {
	long int timeStamp = 0;
	int numF;
	int target;
	t_list* objetivo;

	void elMenor(char* tabla, t_list* paginas) {
		for (int i = 0; i < list_size(paginas); i++) {
			pagina* pag = list_get(paginas, i);
			if (timeStamp == 0 && !pag->modificado) {
				timeStamp = pag->timeStamp;
				numF = pag->numeroFrame;
				target = i;
				objetivo = paginas;
			} else {
				if (pag->timeStamp < timeStamp && !pag->modificado) {
					timeStamp = pag->timeStamp;
					numF = pag->numeroFrame;
					target = i;
					objetivo = paginas;
				}
			}
		}
	}
	dictionary_iterator(tablaSegmentos, elMenor);
	if (timeStamp == 0) {

		sem_post(&sem2);

		ejecutarJournaling();
		numF = 0;
	} else {
		void elemento_distroyer(void* elemento) {
			free(elemento);
		}
		list_remove_and_destroy_element(objetivo, target, elemento_distroyer);
	}
	printf("Remplazo: %d\n", numF);
	return numF;
}

void ejecutarJournaling() {
	sem_wait(&sem2);

	void journal(char* tabla, void* valor) {
		t_list* paginas = valor;
		for (int i = 0; i < list_size(paginas); i++) {
			pagina* pag = list_get(paginas, i);
			if (pag->modificado) {
				int* unaKey = malloc(sizeof(int));
				memcpy(unaKey,
						(memoriaPrincipal + pag->numeroFrame * tamanoFrame),
						sizeof(int));

				char* value = malloc(tamanoValue);
				memcpy(value,
						(memoriaPrincipal + pag->numeroFrame * tamanoFrame
								+ sizeof(int) + sizeof(long int)),
						*(frames + pag->numeroFrame) + 1);
				// Serializo peticion, tabla, key, value (el timestamp lo agrega el fs y siempre es el ACTUAL)
				char* buffer = malloc(
						6 * sizeof(int) + strlen(tabla) + strlen(value));

				int peticion = 2;
				int tamanioPeticion = sizeof(int);
				memcpy(buffer, &tamanioPeticion, sizeof(int));
				memcpy(buffer + sizeof(int), &peticion, sizeof(int));

				int tamanioTabla = strlen(tabla);
				memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
				memcpy(buffer + 3 * sizeof(int), tabla, strlen(tabla));

				int tamanioKey = sizeof(int);
				memcpy(buffer + 3 * sizeof(int) + strlen(tabla), &tamanioKey,
						sizeof(int));
				memcpy(buffer + 4 * sizeof(int) + strlen(tabla), unaKey,
						sizeof(int));

				int tamanioValue = strlen(value);
				memcpy(buffer + 5 * sizeof(int) + strlen(tabla), &tamanioValue,
						sizeof(int));
				memcpy(buffer + 6 * sizeof(int) + strlen(tabla), value,
						strlen(value));
				pthread_mutex_lock(&SEMAFORODECONEXIONFS);
				send(clienteFS, buffer,
						6 * sizeof(int) + strlen(tabla) + strlen(value), 0);

				// Deserializo respuesta
				int* tamanioRespuesta = malloc(sizeof(int));
				read(clienteFS, tamanioRespuesta, sizeof(int));
				int* ok = malloc(*tamanioRespuesta);
				read(clienteFS, ok, *tamanioRespuesta);
				pthread_mutex_unlock(&SEMAFORODECONEXIONFS);
				if (*ok == 0) {
					char* mensajeALogear = malloc( strlen(" NO se pudo realizar insert en FS en tabla :  con value : ") + strlen(tabla) + strlen(value) + 1);
					strcpy(mensajeALogear, " NO se pudo realizar insert en FS en tabla : ");
					strcat(mensajeALogear, tabla);
					strcat(mensajeALogear, " con value : ");
					strcat(mensajeALogear, value);
					t_log* g_logger;
					g_logger = log_create("./logs.log", "MEMORIA", 1,
							LOG_LEVEL_ERROR);
					log_error(g_logger, mensajeALogear);
					log_destroy(g_logger);
					free(mensajeALogear);
				}
				if (*ok == 1) {
					char* mensajeALogear = malloc( strlen(" Se realizo insert en FS en tabla :  con value : ") +strlen(tabla) + strlen(value) + 1);
					strcpy(mensajeALogear, " Se realizo insert en FS en tabla : ");
					strcat(mensajeALogear, tabla);
					strcat(mensajeALogear, " con value : ");
					strcat(mensajeALogear, value);
					t_log* g_logger;
					g_logger = log_create("./logs.log", "MEMORIA", 1,
							LOG_LEVEL_INFO);
					log_info(g_logger, mensajeALogear);
					log_destroy(g_logger);
					free(mensajeALogear);
				}
			}
		}
	}
	pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);

	dictionary_iterator(tablaSegmentos, journal);
	pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);

	for (int i = 0; i < t_archivoConfiguracion.TAM_MEM / tamanoFrame; i++) {
		*(frames + i) = 0;
	}
	void data_destroyer(void* data) {
		free(data);
	}
	pthread_mutex_lock(&SEMAFORODETABLASEGMENTOS);
	dictionary_clean_and_destroy_elements(tablaSegmentos, data_destroyer);
	pthread_mutex_unlock(&SEMAFORODETABLASEGMENTOS);

	free(memoriaPrincipal);

	memoriaPrincipal = malloc(t_archivoConfiguracion.TAM_MEM);

	hizoJ = 1;

	sem_post(&sem2);
}

int realizarCreate(char* tabla, char* tipoConsistencia, char* numeroParticiones, char* tiempoCompactacion) {

	sem_wait(&sem2);

	// Serializo Peticion, Tabla y Metadata
	char* buffer = malloc(
			strlen(tabla) + 6 * sizeof(int) + strlen(tipoConsistencia)
					+ strlen(numeroParticiones) + strlen(tiempoCompactacion));

	int peticion = 3;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla);
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, strlen(tabla));

	int tamanioMetadataConsistency = strlen(tipoConsistencia);
	memcpy(buffer + 3 * sizeof(int) + strlen(tabla),
			&tamanioMetadataConsistency, sizeof(int));
	memcpy(buffer + 4 * sizeof(int) + strlen(tabla), tipoConsistencia,
			strlen(tipoConsistencia));

	int tamanioParticiones = strlen(numeroParticiones);
	memcpy(buffer + 4 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia),
			&tamanioParticiones, sizeof(int));
	memcpy(buffer + 5 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia),
			numeroParticiones, tamanioParticiones);

	int tamanioCompactacion = strlen(tiempoCompactacion);
	memcpy(
			buffer + 5 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia)
					+ tamanioParticiones, &tamanioCompactacion, sizeof(int));
	memcpy(
			buffer + 6 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia)
					+ tamanioParticiones, tiempoCompactacion,
			tamanioCompactacion);

	pthread_mutex_lock(&SEMAFORODECONEXIONFS);
	send(clienteFS, buffer,
			strlen(tabla) + 6 * sizeof(int) + strlen(tipoConsistencia)
					+ strlen(numeroParticiones) + strlen(tiempoCompactacion),
			0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(clienteFS, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(clienteFS, ok, *tamanioRespuesta);
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);

	if (*ok == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se pudo realizar create en el FS ") + 1);
		strcpy(mensajeALogear, " No se pudo realizar create en el FS ");
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		sem_post(&sem2);
		return 0;
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc(
				strlen(" Se realizo create en el FS : ") + 2 * strlen("  con ")
						+ strlen(tabla) + strlen(tipoConsistencia)
						+ strlen(numeroParticiones) + strlen(tiempoCompactacion)
						+ 1);
		strcpy(mensajeALogear, " Se realizo create en el FS : ");
		strcat(mensajeALogear, tabla);
		strcat(mensajeALogear, " con ");
		strcat(mensajeALogear, tipoConsistencia);
		strcat(mensajeALogear, " ");
		strcat(mensajeALogear, numeroParticiones);
		strcat(mensajeALogear, " ");
		strcat(mensajeALogear, tiempoCompactacion);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		sem_post(&sem2);
		return 1;
	}
	return 0;
}

int realizarDrop(char* tabla) {

	sem_wait(&sem2);

	if (dictionary_has_key(tablaSegmentos, tabla)) {
		void* elemento = dictionary_remove(tablaSegmentos, tabla);
		free(elemento);
	}

	// Serializo peticion y tabla
	void* buffer = malloc(strlen(tabla) + 3 * sizeof(int));

	int peticion = 5;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla);
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, strlen(tabla));
	pthread_mutex_lock(&SEMAFORODECONEXIONFS);
	send(clienteFS, buffer, strlen(tabla) + 3 * sizeof(int), 0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(clienteFS, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(clienteFS, ok, *tamanioRespuesta);
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);


	if (*ok == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se pudo realizar drop en FS de : ") + strlen(tabla)
						+ 1);
		strcpy(mensajeALogear, " No se pudo realizar drop en FS de : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		sem_post(&sem2);
		return 0;
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc(
				strlen(" Se realizo drop en FS de : ") + strlen(tabla) + 1);
		strcpy(mensajeALogear, " Se realizo drop en FS de : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		sem_post(&sem2);
		return 1;
	}
	return 0;
}

metadataTabla* realizarDescribe(char* tabla) {

	sem_wait(&sem2);

	// Serializo peticion y tabla
	void* buffer = malloc(strlen(tabla) + 3 * sizeof(int));

	int peticion = 4;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	int tamanioTabla = strlen(tabla);
	memcpy(buffer + 2 * sizeof(int), &tamanioTabla, sizeof(int));
	memcpy(buffer + 3 * sizeof(int), tabla, strlen(tabla));
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);

	send(clienteFS, buffer, strlen(tabla) + 3 * sizeof(int), 0);

	//deserializo metadata
	int *tamanioConsistencia = malloc(sizeof(int));
	read(clienteFS, tamanioConsistencia, sizeof(int));
	char *tipoConsistencia = malloc(*tamanioConsistencia);
	read(clienteFS, tipoConsistencia, *tamanioConsistencia);
	char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia,
			*tamanioConsistencia);

	int* tamanioNumeroParticiones = malloc(sizeof(int));
	read(clienteFS, tamanioNumeroParticiones, sizeof(int));
	int* numeroParticiones = malloc(*tamanioNumeroParticiones);
	read(clienteFS, numeroParticiones, *tamanioNumeroParticiones);

	int* tamanioTiempoCompactacion = malloc(sizeof(int));
	read(clienteFS, tamanioTiempoCompactacion, sizeof(int));
	int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
	read(clienteFS, tiempoCompactacion, *tamanioTiempoCompactacion);
	// aca ya tengo toda la metadata, falta guardarla en struct
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);

	metadataTabla* data = malloc(8 + 4);    // 2 int = 2*4 bytes
	data->consistencia = malloc(strlen(tipoConsistenciaCortada));

	memcpy(&data->particiones, numeroParticiones, sizeof(int));
	memcpy(data->consistencia, tipoConsistenciaCortada,
			strlen(tipoConsistenciaCortada) + 1);
	memcpy(&data->tiempoCompactacion, tiempoCompactacion, sizeof(int));

	printf("Particiones: %d\n", data->particiones);
	printf("T de Comp: %d\n", data->tiempoCompactacion);
	printf("Consistencia: %s\n", data->consistencia);

	//free(metadata);
	sem_post(&sem2);

	return data;
}

void realizarDescribeGlobal() {

	sem_wait(&sem2);

	// serializo peticion
	void* buffer = malloc(2 * sizeof(int));
	int peticion = 6;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	pthread_mutex_lock(&SEMAFORODECONEXIONFS);
	send(clienteFS, buffer, 2 * sizeof(int), 0);

	// deserializo
	int *tamanioTabla = malloc(sizeof(int));
	read(clienteFS, tamanioTabla, sizeof(int));
	while (*tamanioTabla != 0) {
		char *tabla = malloc(*tamanioTabla);
		read(clienteFS, tabla, *tamanioTabla);
		char *tablaCortada = string_substring_until(tabla, *tamanioTabla);

		int *tamanioConsistencia = malloc(sizeof(int));
		read(clienteFS, tamanioConsistencia, sizeof(int));
		char *tipoConsistencia = malloc(*tamanioConsistencia);
		read(clienteFS, tipoConsistencia, *tamanioConsistencia);
		char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia,
				*tamanioConsistencia);

		int* tamanioNumeroParticiones = malloc(sizeof(int));
		read(clienteFS, tamanioNumeroParticiones, sizeof(int));
		int* numeroParticiones = malloc(*tamanioNumeroParticiones);
		read(clienteFS, numeroParticiones, *tamanioNumeroParticiones);

		int* tamanioTiempoCompactacion = malloc(sizeof(int));
		read(clienteFS, tamanioTiempoCompactacion, sizeof(int));
		int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
		read(clienteFS, tiempoCompactacion, *tamanioTiempoCompactacion);

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
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);

		read(clienteFS, tamanioTabla, sizeof(int));
	}

	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);
	sem_post(&sem2);
}

void consola() {
	while (1) {
		char* instruccion = malloc(1000);

		do {

			fgets(instruccion, 1000, stdin);

		} while (!strcmp(instruccion, "\n"));

		analizarInstruccion(instruccion);

		free(instruccion);
	}
}

int serServidor() {
	int opt = 1;
	int master_socket, addrlen, new_socket, client_socket[30], max_clients = 30,
			activity, i, sd, valread;
	int max_sd;
	struct sockaddr_in address;

	//set of socket descriptors
	fd_set readfds;

	//initialise all client_socket[] to 0 so not checked
	for (i = 0; i < max_clients; i++) {
		client_socket[i] = 0;
	}

	//create a master socket
	if ((master_socket = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
		perror("socket failed");
		exit(EXIT_FAILURE);
	}

	//set master socket to allow multiple connections ,
	//this is just a good habit, it will work without this
	if (setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *) &opt,
			sizeof(opt)) < 0) {
		perror("setsockopt");
		exit(EXIT_FAILURE);
	}

	int option = 1;
	setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, &option,
			sizeof(option));

	//type of socket created
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(t_archivoConfiguracion.PUERTO);

	//bind the socket to localhost port 8888
	if (bind(master_socket, (struct sockaddr *) &address, sizeof(address))
			< 0) {
		perror("Bind fallo en el FS");
		return 1;
	}

	datosMemoria* unMem = malloc(sizeof(datosMemoria));

	unMem->MEMORY_NUMBER = t_archivoConfiguracion.MEMORY_NUMBER;
	unMem->direccionSocket = address;
	unMem->socket = master_socket;

	list_add(clientes, unMem);

	printf("Escuchando en el puerto: %d \n", t_archivoConfiguracion.PUERTO);
	printf("El puerto del address es: %d\n", address.sin_port);

	listen(master_socket, 100);

	//accept the incoming connection
	addrlen = sizeof(address);
	puts("Esperando conexiones ...\n");

	while (1) {
		//clear the socket set
		FD_ZERO(&readfds);

		//add master socket to set
		FD_SET(master_socket, &readfds);
		max_sd = master_socket;

		//add child sockets to set
		for (i = 0; i < max_clients; i++) {
			//socket descriptor
			sd = client_socket[i];

			//if valid socket descriptor then add to read list
			if (sd > 0)
				FD_SET(sd, &readfds);

			//highest file descriptor number, need it for the select function
			if (sd > max_sd)
				max_sd = sd;
		}

		//wait for an activity on one of the sockets , timeout is NULL ,
		//so wait indefinitely
		activity = select(max_sd + 1, &readfds, NULL, NULL, NULL);

		if ((activity < 0) && (errno != EINTR)) {
			printf("select error");
		}

		//If something happened on the master socket ,
		//then its an incoming connection
		if (FD_ISSET(master_socket, &readfds)) {
			new_socket = accept(master_socket, (struct sockaddr *) &address,
					(socklen_t*) &addrlen);
			if (new_socket < 0) {
				perror("accept");
				exit(EXIT_FAILURE);
			}

			//inform user of socket number - used in send and receive commands
			printf("Nueva Conexion , socket fd: %d , ip: %s , puerto: %d 	\n",
					new_socket, inet_ntoa(address.sin_addr),
					ntohs(address.sin_port));

			//add new socket to array of sockets
			for (i = 0; i < max_clients; i++) {
				//if position is empty
				if (client_socket[i] == 0) {
					client_socket[i] = new_socket;
					printf("Agregado a la lista de sockets como: %d\n", i);

					break;
				}
			}
		}

		//else its some IO operation on some other socket
		for (i = 0; i < max_clients; i++) {
			sd = client_socket[i];

			if (FD_ISSET(sd, &readfds)) {
				//Check if it was for closing , and also read the
				//incoming message

				int *tamanio = malloc(sizeof(int));
				if ((valread = read(sd, tamanio, sizeof(int))) == 0) {
					getpeername(sd, (struct sockaddr *) &address,
							(socklen_t *) &addrlen);
					//printf("Host disconected, ip: %s, port: %d\n", inet_ntoa(address.sin_addr), ntohs(address.sin_port));
					close(sd);
					client_socket[i] = 0;
				} else {
					int *operacion = malloc(*tamanio);
					read(sd, operacion, sizeof(int));

					switch (*operacion) {
					case 1:
						//Select
						printf("Kernel pidio un Select\n");
						tomarPeticionSelect(sd);
						break;
					case 2:
						//Insert
						printf("Kernel pidio un Insert\n");
						tomarPeticionInsert(sd);
						break;
					case 3:
						//Create
						printf("Kernel pidio un Create\n");
						tomarPeticionCreate(sd);
						break;
					case 4:
						//Describe
						printf("Kernel pidio un Describe\n");
						tomarPeticionDescribe1Tabla(sd);
						break;
					case 5:
						//Drop
						printf("Kernel pidio un Drop\n");
						tomarPeticionDrop(sd);
						break;
					case 6:
						//Describe global
						printf("Kernel pidio un Describe Global\n");
						tomarPeticionDescribeGlobal(sd);
						break;
					case 7:
						//Journal
						printf("Kernel pidio un Journal\n");
						ejecutarJournaling();
						break;
					case 8:
						//Gossiping
						printf("Se pidio un Gossiping\n");

						int* socket = malloc(sizeof(int));
						recv(sd, socket, sizeof(int), 0);

						while (*socket != 0) {
							struct sockaddr_in *direccion = malloc(
									sizeof(struct sockaddr_in));
							recv(sd, direccion, sizeof(struct sockaddr_in), 0);

							int32_t* num = malloc(sizeof(int));
							recv(sd, num, sizeof(int32_t), 0);

							printf("Num: %d\n", *num);
							printf("Puerto: %d\n", direccion->sin_port);

							datosMemoria* unaM = malloc(sizeof(datosMemoria));

							unaM->MEMORY_NUMBER = *num;
							unaM->direccionSocket = *direccion;
							unaM->socket = *socket;

							bool estaEnLaLista(datosMemoria* elemento) {
								return elemento->MEMORY_NUMBER == *num;
							}

							if (!list_any_satisfy(clientes, estaEnLaLista)) {
								list_add(clientes, unaM);
							}

							free(direccion);
							free(num);
							free(socket);

							socket = malloc(sizeof(int));
							recv(sd, socket, sizeof(int), 0);

						}
						int i = 0;
						char* buffer;

						while (i < list_size(clientes)) {
							datosMemoria* unaM = list_get(clientes, i);

							buffer = malloc(
									sizeof(int) + sizeof(struct sockaddr_in)
											+ sizeof(int32_t));

							memcpy(buffer, &unaM->socket, sizeof(int));
							memcpy(buffer + sizeof(int), &unaM->direccionSocket,
									sizeof(struct sockaddr_in));
							memcpy(
									buffer + sizeof(int)
											+ sizeof(struct sockaddr_in),
									&unaM->MEMORY_NUMBER, sizeof(int32_t));

							send(sd, buffer,
									sizeof(int) + sizeof(struct sockaddr_in)
											+ sizeof(int32_t), 0);

							free(buffer);

							i++;
						}

						buffer = malloc(sizeof(int));
						int* fin = 0;

						memcpy(buffer, &fin, sizeof(int));

						send(sd, buffer, sizeof(int), 0);

						free(buffer);

						break;
					default:
						break;
					}
				}
			}
		}
	}
}

void conectarseAFS() {
	clienteFS = socket(AF_INET, SOCK_STREAM, 0);
	serverAddressFS.sin_family = AF_INET;
	serverAddressFS.sin_port = htons(t_archivoConfiguracion.PUERTO_FS);
	serverAddress.sin_addr.s_addr = INADDR_ANY;

	connect(clienteFS, (struct sockaddr *) &serverAddressFS,
			sizeof(serverAddressFS));

	int *tamanioValue = malloc(sizeof(int));
	recv(clienteFS, tamanioValue, sizeof(int), 0);

	memcpy(&tamanoValue, tamanioValue, sizeof(int));

	sem_post(&sem);
	sem_post(&sem2);

	sleep(30);

	sem_wait(&sem2);

	printf("Cerrar Socket\n");

	close(clienteFS);

	conectarseAFS();
}

void tomarPeticionSelect(int kernel) {
	//deserializo lo que me pide el kernel
	int *tamanioTabla = malloc(sizeof(int));
	read(kernel, tamanioTabla, sizeof(int));
	char *tabla = malloc(*tamanioTabla);
	read(kernel, tabla, *tamanioTabla);
	char *tablaCortada = string_substring_until(tabla, *tamanioTabla);

	int *tamanioKey = malloc(sizeof(int));
	read(kernel, tamanioKey, sizeof(int));
	int *key = malloc(*tamanioKey);
	read(kernel, key, *tamanioKey);
	char* keyString = string_itoa(*key);

	//Lo busca en memoria y sino la misma memoria se lo pide al FS
	char* value = realizarSelect(tablaCortada, keyString);

	//serializo y se lo mando al kernel
	if (value == NULL) {
		int ok = 0;
		void* buffer = malloc(4);
		memcpy(buffer, &ok, 4);
		send(kernel, buffer, 4, 0);
	} else {
		void *buffer = malloc(strlen(value) + sizeof(int));
		int tamanio = strlen(value);
		memcpy(buffer, &tamanio, sizeof(int));
		memcpy(buffer + sizeof(int), value, tamanio);
		send(kernel, buffer, strlen(value) + sizeof(int), 0);
	}
	free(value);
	free(key);
	free(tamanioKey);
	free(tamanioTabla);
}

void tomarPeticionInsert(int kernel) {
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	printf("tabla: %s\n", tabla);

	int *tamanioKey = malloc(sizeof(int));
	recv(kernel, tamanioKey, sizeof(int), 0);
	char *key = malloc(*tamanioKey);
	recv(kernel, key, *tamanioKey, 0);
	printf("key: %s\n", key);

	int *tamanioValue = malloc(sizeof(int));
	recv(kernel, tamanioValue, sizeof(int), 0);
	char* value = malloc(*tamanioValue);
	recv(kernel, value, *tamanioValue, 0);
	//printf("value: %s\n", value);

	realizarInsert(tabla, key, value);
	free(value);
	free(key);
	free(tamanioKey);
	free(tamanioValue);
	free(tamanioTabla);
}

void tomarPeticionCreate(int kernel) {
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	//printf("tabla: %s\n", tabla);

	int *tamanioConsistencia = malloc(sizeof(int));
	recv(kernel, tamanioConsistencia, sizeof(int), 0);
	char *consistencia = malloc(*tamanioConsistencia);
	recv(kernel, consistencia, *tamanioConsistencia, 0);
	//printf("consistencia: %s\n", consistencia);

	int *tamanionumeroParticiones = malloc(sizeof(int));
	recv(kernel, tamanionumeroParticiones, sizeof(int), 0);
	char *numeroDeParticiones = malloc(*tamanionumeroParticiones);
	recv(kernel, numeroDeParticiones, *tamanionumeroParticiones, 0);
	//printf("cantidad de particiones: %s\n", numeroDeParticiones);

	int* tamanioTiempoCompactacion = malloc(sizeof(int));
	recv(kernel, tamanioTiempoCompactacion, sizeof(int), 0);
	char *tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
	recv(kernel, tiempoCompactacion, *tamanioTiempoCompactacion, 0);
	//printf("tiempo de compactacion: %s\n", tiempoCompactacion);

	int respuesta = realizarCreate(tabla, consistencia, numeroDeParticiones, tiempoCompactacion);

	char* buffer = malloc(2 * sizeof(int));
	int tamanioRespuesta = sizeof(int);
	memcpy(buffer, &tamanioRespuesta, sizeof(int));
	memcpy(buffer + sizeof(int), &respuesta, sizeof(int));

	send(kernel, buffer, 2 * sizeof(int), 0);
}

void tomarPeticionDescribe1Tabla(int kernel) {
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	//printf("tabla: %s\n", tabla);

	metadataTabla* metadata = realizarDescribe(tabla);

	// serializo paquete
	int tamanioBuffer = strlen(metadata->consistencia) + 1 + 5 * sizeof(int);
	void* buffer = malloc(tamanioBuffer);

	int tamanioMetadataConsistency = strlen(metadata->consistencia) + 1;
	memcpy(buffer, &tamanioMetadataConsistency, sizeof(int));
	memcpy(buffer + sizeof(int), metadata->consistencia,
			tamanioMetadataConsistency);

	int tamanioParticiones = sizeof(int);
	memcpy(buffer + sizeof(int) + tamanioMetadataConsistency,
			&tamanioParticiones, sizeof(int));
	memcpy(buffer + 2 * sizeof(int) + tamanioMetadataConsistency,
			&metadata->particiones, sizeof(int));

	int tamanioCompactacion = sizeof(int);
	memcpy(buffer + 3 * sizeof(int) + tamanioMetadataConsistency,
			&tamanioCompactacion, sizeof(int));
	memcpy(buffer + 4 * sizeof(int) + tamanioMetadataConsistency,
			&metadata->tiempoCompactacion, sizeof(int));

	send(kernel, buffer, tamanioBuffer, 0);
}

void tomarPeticionDescribeGlobal(int kernel) {
	// primer paso : pedirselo al fs
	// serializo peticion
	void* buffer = malloc(2 * sizeof(int));
	int peticion = 6;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));
	pthread_mutex_lock(&SEMAFORODECONEXIONFS);

	send(clienteFS, buffer, 2 * sizeof(int), 0);

	// deserializo
	int *tamanioTabla2 = malloc(sizeof(int));
	read(clienteFS, tamanioTabla2, sizeof(int));
	while (*tamanioTabla2 != 0) {
		char *tabla = malloc(*tamanioTabla2);
		read(clienteFS, tabla, *tamanioTabla2);
		char *tablaCortada = string_substring_until(tabla, *tamanioTabla2);

		int *tamanioConsistencia = malloc(sizeof(int));
		read(clienteFS, tamanioConsistencia, sizeof(int));
		char *tipoConsistencia = malloc(*tamanioConsistencia);
		read(clienteFS, tipoConsistencia, *tamanioConsistencia);
		char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia,
				*tamanioConsistencia);

		int* tamanioNumeroParticiones = malloc(sizeof(int));
		read(clienteFS, tamanioNumeroParticiones, sizeof(int));
		int* numeroParticiones = malloc(*tamanioNumeroParticiones);
		read(clienteFS, numeroParticiones, *tamanioNumeroParticiones);

		int* tamanioTiempoCompactacion = malloc(sizeof(int));
		read(clienteFS, tamanioTiempoCompactacion, sizeof(int));
		int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
		read(clienteFS, tiempoCompactacion, *tamanioTiempoCompactacion);

		// segundo paso : se lo envia al kernel
		// serializo tabla y metadata

		void* buffer2 = malloc(
				strlen(tablaCortada) + strlen(tipoConsistenciaCortada)
						+ 6 * sizeof(int));

		int tamanioTabla = strlen(tablaCortada);
		memcpy(buffer2, &tamanioTabla, sizeof(int));
		memcpy(buffer2 + sizeof(int), tablaCortada, strlen(tablaCortada));

		int tamanioMetadataConsistency = strlen(tipoConsistenciaCortada);
		memcpy(buffer2 + sizeof(int) + strlen(tablaCortada),
				&tamanioMetadataConsistency, sizeof(int));
		memcpy(buffer2 + 2 * sizeof(int) + strlen(tablaCortada),
				tipoConsistenciaCortada, strlen(tipoConsistenciaCortada));

		int tamanioParticiones = sizeof(int);
		memcpy(
				buffer2 + 2 * sizeof(int) + strlen(tablaCortada)
						+ strlen(tipoConsistenciaCortada), &tamanioParticiones,
				sizeof(int));
		memcpy(
				buffer2 + 3 * sizeof(int) + strlen(tablaCortada)
						+ strlen(tipoConsistenciaCortada), numeroParticiones,
				sizeof(int));

		int tamanioCompactacion = sizeof(int);
		memcpy(
				buffer2 + 4 * sizeof(int) + strlen(tablaCortada)
						+ strlen(tipoConsistenciaCortada), &tamanioCompactacion,
				sizeof(int));
		memcpy(
				buffer2 + 5 * sizeof(int) + strlen(tablaCortada)
						+ strlen(tipoConsistenciaCortada), tiempoCompactacion,
				sizeof(int));

		send(kernel, buffer2,
				strlen(tablaCortada) + strlen(tipoConsistenciaCortada)
						+ 6 * sizeof(int), 0);

		read(clienteFS, tamanioTabla2, sizeof(int));
	}
	pthread_mutex_unlock(&SEMAFORODECONEXIONFS);

	char* buffer2 = malloc(4);
	int respuesta = 0;
	memcpy(buffer2, &respuesta, sizeof(int));
	send(kernel, buffer2, sizeof(int), 0);

}

void tomarPeticionDrop(int kernel) {
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	//printf("tabla: %s\n", tabla);

	int respuesta = realizarDrop(tabla);

	char* buffer = malloc(2 * sizeof(int));
	int tamanioRespuesta = sizeof(int);
	memcpy(buffer, &tamanioRespuesta, sizeof(int));
	memcpy(buffer + sizeof(int), &respuesta, sizeof(int));

	send(kernel, buffer, 2 * sizeof(int), 0);
}

void conectar() {
	while (1) {
		int i = 0;
		//printf("%s\n", t_archivoConfiguracion.PUERTO_SEEDS[i]);
		while (t_archivoConfiguracion.PUERTO_SEEDS[i] != NULL) {
			int32_t clienteSeed = socket(AF_INET, SOCK_STREAM, 0);
			direccionCliente.sin_family = AF_INET;
			direccionCliente.sin_port = htons(
					atoi(t_archivoConfiguracion.PUERTO_SEEDS[i]));
			direccionCliente.sin_addr.s_addr = INADDR_ANY;

			int res = -1;
			res = connect(clienteSeed, (struct sockaddr *) &direccionCliente,
					sizeof(direccionCliente));

			if (res >= 0) {
				pthread_t tGosiping;
				int32_t idTGosiping = pthread_create(&tGosiping, NULL,
						gossiping, clienteSeed);
			}

			i++;
		}
		sleep(t_archivoConfiguracion.RETARDO_GOSSIPING/1000);
	}
}

void gossiping(int cliente) {

	char* buffer = malloc(2 * sizeof(int));

	int peticion = 8;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));

	send(cliente, buffer, 2 * sizeof(int), 0);

	free(buffer);

	int i = 0;
	while (i < list_size(clientes)) {
		datosMemoria* unaM = list_get(clientes, i);

		buffer = malloc(
				sizeof(int) + sizeof(struct sockaddr_in) + sizeof(int32_t));

		memcpy(buffer,  &unaM->socket,  sizeof(int));
		memcpy(buffer + sizeof(int),  &unaM->direccionSocket,   sizeof(struct sockaddr_in));
		memcpy(buffer + sizeof(int) + sizeof(struct sockaddr_in),  &unaM->MEMORY_NUMBER,   sizeof(int32_t));

		send(cliente, buffer,	sizeof(int) + sizeof(struct sockaddr_in) + sizeof(int32_t),   0);

		free(buffer);

		i++;
	}

	//fin
	buffer = malloc(sizeof(int));
	int* fin = 0;
	memcpy(buffer, &fin, sizeof(int));
	send(cliente, buffer, sizeof(int), 0);
	free(buffer);

	int* socket = malloc(sizeof(int));
	recv(cliente, socket, sizeof(int), 0);
	while (*socket != 0) {
		struct sockaddr_in *direccion = malloc(sizeof(struct sockaddr_in));
		recv(cliente, direccion, sizeof(struct sockaddr_in), 0);

		int32_t* num = malloc(sizeof(int));
		recv(cliente, num, sizeof(int32_t), 0);

		printf("Num: %d\n", *num);
		printf("Puerto: %d\n", direccion->sin_port);

		datosMemoria* unaM = malloc(sizeof(datosMemoria));

		unaM->MEMORY_NUMBER = *num;
		unaM->direccionSocket = *direccion;
		unaM->socket = *socket;

		bool estaEnLaLista(datosMemoria* elemento) {
			return elemento->MEMORY_NUMBER == *num;
		}

		if (!list_any_satisfy(clientes, estaEnLaLista)) {
			list_add(clientes, unaM);


		}

		free(direccion);
		free(num);
		free(socket);

		socket = malloc(sizeof(int));
		recv(cliente, socket, sizeof(int), 0);
	}
	close(cliente);
}

