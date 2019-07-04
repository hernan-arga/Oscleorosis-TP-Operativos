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
	char* IP_FS;
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

struct datosMemoria {
	int socket;
	struct sockaddr_in direccionSocket;
	int32_t MEMORY_NUMBER;
};

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

//Hilos
pthread_t threadKernel;
pthread_t threadFS;

void analizarInstruccion(char* instruccion);
void realizarComando(char** comando);
OPERACION tipoDePeticion(char* peticion);
char* realizarSelect(char* tabla, char* key);
int realizarInsert(char* tabla, char* key, char* value);
int frameLibre();
char* pedirValue(char* tabla, char* key);
int ejecutarLRU();
void ejecutarJournaling();
void realizarCreate(char* tabla, char* tipoConsistencia,
		char* numeroParticiones, char* tiempoCompactacion);
void realizarDrop(char* tabla);
void realizarDescribeGlobal();
metadataTabla* realizarDescribe(char* tabla);
void consola();
void serServidor();
void conectarseAFS();
void tratarKernel(int kernel);
void gossiping(int cliente);
void tratarCliente(int cliente);
void aceptar();

void tomarPeticionSelect(int);
void tomarPeticionInsert(int);
void tomarPeticionCreate(int);
void tomarPeticionDescribe1Tabla(int);
void tomarPeticionDescribeGlobal(int);
void tomarPeticionDrop(int);

int main(int argc, char *argv[]) {
	sem_init(&sem, 1, 0);
	sem_init(&sem2, 1, 1);

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

	pthread_t threadSerServidor;
	int32_t idThreadSerServidor = pthread_create(&threadSerServidor, NULL,
			serServidor, NULL);

	//pthread_t threadKernel;
	//int32_t idThreadKernel = pthread_create(&threadKernel, NULL, (void*)aceptar, NULL);

	tablaSegmentos = dictionary_create();

	printf("%d", t_archivoConfiguracion.PUERTO);

	memoriaPrincipal = malloc(t_archivoConfiguracion.TAM_MEM);
	//memoriaPrincipal = malloc(1000);

	sem_wait(&sem);

	//tamanoValue = 100;

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

	pthread_join(threadConsola, NULL);
	pthread_join(threadSerServidor, NULL);
	pthread_join(threadFS, NULL);
	//pthread_join(aceptar, NULL);
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
		//printf("SELECT");
		tabla = comando[1];
		key = comando[2];
		realizarSelect(tabla, key);
		break;

	case INSERT:
		//printf("INSERT");
		tabla = comando[1];
		key = comando[2];
		value = comando[3];
		realizarInsert(tabla, key, value);
		break;

	case CREATE:
		//printf("CREATE");
		tabla = comando[1];
		char* tipoConsistencia = comando[2];
		char* numeroParticiones = comando[3];
		char* tiempoCompactacion = comando[4];
		realizarCreate(tabla, tipoConsistencia, numeroParticiones,
				tiempoCompactacion);
		break;

		//Describe recibe un diccionario con (nombreTabla - struct(con la info de la metadata)

	case DESCRIBE:
		//printf("\nDESCRIBE");

		if (comando[1] == NULL) {
			//printf("GLOBAL");
			realizarDescribeGlobal();
		} else {
			//printf("Normal");
			tabla = comando[1];
			realizarDescribe(tabla);
		}
		break;

	case DROP:
		//printf("\nDROP");
		tabla = comando[1];
		realizarDrop(tabla);
		break;

	case JOURNAL:
		//printf("\nJOURNAL");
		ejecutarJournaling();
		break;

	case OPERACIONINVALIDA:
		//printf("OPERACION INVALIDA");
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
			t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

			for (int i = 0; i < list_size(tablaPag); i++) {
				pagina* pag = list_get(tablaPag, i);

				int* laKey = malloc(sizeof(int));

				memcpy(laKey, (memoriaPrincipal + pag->numeroFrame * tamanoFrame), sizeof(int));

				if (*laKey == atoi(key)) {

					char* value = malloc(*(frames + pag->numeroFrame));

					memcpy(value, (memoriaPrincipal + pag->numeroFrame * tamanoFrame + sizeof(int) + sizeof(long int)),	*(frames + pag->numeroFrame)+1);

					long int* timeStamp = malloc(sizeof(long int));
					*timeStamp = (long int) time(NULL);

					memcpy((memoriaPrincipal + pag->numeroFrame * tamanoFrame + sizeof(int)), timeStamp, sizeof(long int));

					free(timeStamp);

					printf("Value: %s\n", value);

					pag->timeStamp = *timeStamp;

					//free(value);
					free(laKey);

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
			pagp->numeroPag = list_size(tablaPag);
			pagp->timeStamp = *timeStamp;

			list_add(tablaPag, pagp);

			char* value = malloc(tamanoValue);
			value = pedirValue(tabla, key);

			if (value == NULL) {
				return 0;
			}

			*(frames + frameNum) = strlen(value);

			printf("Value: %s\n", value);

			int* laKey = malloc(sizeof(int));
			*laKey = atoi(key);

			memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame), laKey, sizeof(int));
			memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int) + sizeof(long int)), value, strlen(value)+1);
			memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)), timeStamp, sizeof(long int));

			free(timeStamp);
			//free(value);

			return value;
		}

		char* value = pedirValue(tabla, key);

		if (value == NULL) {
			return 0;
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
		dictionary_put(tablaSegmentos, tabla, paginasp);

		int* laKey = malloc(sizeof(int));
		*laKey = atoi(key);

		memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame), laKey, sizeof(int));
		memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int)), timeStamp, sizeof(long int));
		memcpy((memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int) + sizeof(long int)), value, strlen(value)+1);

		free(timeStamp);
		//free(value);

		return value;
	}

int realizarInsert(char* tabla, char* key, char* value) {
	if (dictionary_has_key(tablaSegmentos, tabla)) {
		t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

		for (int i = 0; i < list_size(tablaPag); i++) {
			pagina* pagy = list_get(tablaPag, i);

			int* laKey = malloc(sizeof(int));

			memcpy(laKey, (memoriaPrincipal + pagy->numeroFrame * tamanoFrame),sizeof(int));

			if (*laKey == atoi(key))
			{
				long int* timeStamp = malloc(sizeof(long int));

				*timeStamp = (long int) time(NULL);

				memcpy((memoriaPrincipal + (pagy->numeroFrame * tamanoFrame) + sizeof(int) + sizeof(long int)), value, strlen(value)+1);
				memcpy(memoriaPrincipal + pagy->numeroFrame * tamanoFrame + sizeof(int), timeStamp, sizeof(long int));

				*(frames + pagy->numeroFrame) = strlen(value);

				pagy->timeStamp = *timeStamp;

				free(laKey);
				free(timeStamp);

				return 0;
			}
		}

		int frameNum = frameLibre();
		*(frames + frameNum) = strlen(value);

		char* timeStamp = malloc(sizeof(long int));
		*timeStamp = (long int) time(NULL);

		pagina* pagp = malloc(sizeof(pagina));

		pagp->modificado = true;
		pagp->numeroFrame = frameNum;
		pagp->numeroPag = list_size(tablaPag);
		pagp->timeStamp = *timeStamp;

		list_add(tablaPag, pagp);

		int* laKey = malloc(sizeof(int));
		*laKey = atoi(key);

		memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame, laKey, sizeof(int));
		memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int), timeStamp, sizeof(long int));
		memcpy(	memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int) + sizeof(long int), value, strlen(value)+1);

		free(timeStamp);

		return 0;
	}

	int frameNum = frameLibre();
	*(frames + frameNum) = strlen(value);

	char* timeStamp = malloc(sizeof(long int));
	*timeStamp = (long int) time(NULL);

	pagina* pagp = malloc(sizeof(pagina));
	pagp->modificado = true;
	pagp->numeroFrame = frameNum;
	pagp->numeroPag = 0;
	pagp->timeStamp = *timeStamp;

	t_list* paginas = list_create();
	list_add(paginas, pagp);

	dictionary_put(tablaSegmentos, tabla, paginas);

	int* laKey = malloc(sizeof(int));
	*laKey = atoi(key);

	memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame, laKey, sizeof(int));
	memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int), timeStamp, sizeof(long int));
	memcpy(memoriaPrincipal + pagp->numeroFrame * tamanoFrame + sizeof(int) + sizeof(long int), value, strlen(value)+1);

	free(timeStamp);

	return 0;
}

int frameLibre() {
	for (int i = 0; i < 1000 / tamanoFrame; i++) {
		if (*(frames + i) == 0) {
			return i;
		}
	}
	printf("Ejecutar LRU");
	return ejecutarLRU();
}

char* pedirValue(char* tabla, char* laKey) {
	// Serializo peticion, tabla y key
	int key = atoi(laKey);

	void* buffer = malloc(
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
	memcpy(buffer + 4 * sizeof(int) + strlen(tabla), &key, sizeof(int));

	send(clienteFS, buffer, strlen(tabla) + 5 * sizeof(int), 0);

	//deserializo value
	int *tamanioValue = malloc(sizeof(int));
	recv(clienteFS, tamanioValue, sizeof(int), 0);

	if (*tamanioValue == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se encontro la key : ") + sizeof(key) + 1);
		strcpy(mensajeALogear, " No se encontro la key : ");
		strcat(mensajeALogear, string_itoa(key));
		t_log* g_logger;
		g_logger = log_create("./logs.log", "LFS", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		return NULL;
	} else {
		char *value = malloc(*tamanioValue);
		recv(clienteFS, value, *tamanioValue, 0);
		char *valueCortado = string_substring_until(value, *tamanioValue); //corto value

		char* mensajeALogear = malloc(
				strlen(" Llego select con VALUE : ") + strlen(valueCortado) + 1);
		strcpy(mensajeALogear, " Llego select con VALUE : ");
		strcat(mensajeALogear, valueCortado);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "LFS", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);

		return valueCortado;
	}
}

int ejecutarLRU() {
	long int timeStamp = 0;
	int numF;
	int target;
	t_list* objetivo;

	void elMenor(char* key, void* value) {
		t_list* paginas = value;
		for (int i = 0; i < list_size(paginas); i++) {
			pagina* pag = list_get(paginas, i);
			if (timeStamp == 0 && !pag->modificado) {
				timeStamp = pag->timeStamp;
				numF = pag->numeroFrame;
				target = pag->numeroPag;
				objetivo = paginas;
			} else {
				if (pag->timeStamp < timeStamp && !pag->modificado) {
					timeStamp = pag->timeStamp;
					numF = pag->numeroFrame;
					target = pag->numeroPag;
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
		list_remove_and_destroy_element(objetivo, target, NULL);
	}
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
						*(frames + pag->numeroFrame)+1);

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

				int key = atoi(unaKey);
				int tamanioKey = sizeof(int);
				memcpy(buffer + 3 * sizeof(int) + strlen(tabla), &tamanioKey,
						sizeof(int));
				memcpy(buffer + 4 * sizeof(int) + strlen(tabla), &key,
						sizeof(int));

				int tamanioValue = strlen(value);
				memcpy(buffer + 5 * sizeof(int) + strlen(tabla), &tamanioValue,
						sizeof(int));
				memcpy(buffer + 6 * sizeof(int) + strlen(tabla), value,
						strlen(value));

				send(clienteFS, buffer,
						6 * sizeof(int) + strlen(tabla) + strlen(value), 0);

				// Deserializo respuesta
				int* tamanioRespuesta = malloc(sizeof(int));
				read(clienteFS, tamanioRespuesta, sizeof(int));
				int* ok = malloc(*tamanioRespuesta);
				read(clienteFS, ok, *tamanioRespuesta);

				if (*ok == 0) {
					char* mensajeALogear = malloc(
							strlen(" No se pudo realizar insert en FS ") + 1);
					strcpy(mensajeALogear,
							" No se pudo realizar insert en FS ");
					t_log* g_logger;
					g_logger = log_create("./logs.log", "MEMORIA", 1,
							LOG_LEVEL_ERROR);
					log_error(g_logger, mensajeALogear);
					log_destroy(g_logger);
					free(mensajeALogear);
				}
				if (*ok == 1) {
					char* mensajeALogear = malloc(
							strlen(" Se realizo insert en FS ") + 1);
					strcpy(mensajeALogear, " Se realizo insert en FS ");
					t_log* g_logger;
					g_logger = log_create("./logs.log", "MEMORIA", 1,
							LOG_LEVEL_INFO);
					log_error(g_logger, mensajeALogear);
					log_destroy(g_logger);
					free(mensajeALogear);
				}
				//free(mensaje);
			}
		}
	}

	dictionary_iterator(tablaSegmentos, journal);

	for (int i = 0; i < 1000 / tamanoFrame; i++) {
		*(frames + i) = 0;
	}
	dictionary_clean_and_destroy_elements(tablaSegmentos, NULL);

	free(memoriaPrincipal);

	memoriaPrincipal = malloc(t_archivoConfiguracion.TAM_MEM);

	sem_post(&sem2);
}

void realizarCreate(char* tabla, char* tipoConsistencia,
		char* numeroParticiones, char* tiempoCompactacion) {
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
	memcpy( buffer + 5 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia) + tamanioParticiones, &tamanioCompactacion, sizeof(int));
	memcpy( buffer + 6 * sizeof(int) + strlen(tabla) + strlen(tipoConsistencia)	+ tamanioParticiones, tiempoCompactacion, tamanioCompactacion);

	send(clienteFS, buffer, strlen(tabla) + 6 * sizeof(int) + strlen(tipoConsistencia) + strlen(numeroParticiones) + strlen(tiempoCompactacion),
			0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(clienteFS, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(clienteFS, ok, *tamanioRespuesta);

	if (*ok == 0) {
		char* mensajeALogear = malloc( strlen(" No se pudo realizar create en el FS ") + 1);
		strcpy(mensajeALogear, " No se pudo realizar create en el FS ");
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc( strlen(" Se realizo create en el FS : ") + 2*strlen("  con ") + strlen(tabla)+ strlen(tipoConsistencia) + strlen(numeroParticiones) + strlen(tiempoCompactacion) + 1);
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
	}

	sem_post(&sem2);
}

void realizarDrop(char* tabla) {
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

	send(clienteFS, buffer, strlen(tabla) + 3 * sizeof(int), 0);

	// Deserializo respuesta
	int* tamanioRespuesta = malloc(sizeof(int));
	read(clienteFS, tamanioRespuesta, sizeof(int));
	int* ok = malloc(*tamanioRespuesta);
	read(clienteFS, ok, *tamanioRespuesta);

	if (*ok == 0) {
		char* mensajeALogear = malloc(
				strlen(" No se pudo realizar drop en FS de : ") + strlen(tabla) + 1);
		strcpy(mensajeALogear, " No se pudo realizar drop en FS de : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	if (*ok == 1) {
		char* mensajeALogear = malloc(strlen(" Se realizo drop en FS de : ") + strlen(tabla) + 1);
		strcpy(mensajeALogear, " Se realizo drop en FS de : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create("./logs.log", "MEMORIA", 1, LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}

	sem_post(&sem2);
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

	send(clienteFS, buffer, strlen(tabla) + 3 * sizeof(int), 0);

	//deserializo metadata
	int *tamanioConsistencia = malloc(sizeof(int));
	read(clienteFS, tamanioConsistencia, sizeof(int));
	char *tipoConsistencia = malloc(*tamanioConsistencia);
	read(clienteFS, tipoConsistencia, *tamanioConsistencia);
	char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia, *tamanioConsistencia);

	int* tamanioNumeroParticiones = malloc(sizeof(int));
	read(clienteFS, tamanioNumeroParticiones, sizeof(int));
	int* numeroParticiones = malloc(*tamanioNumeroParticiones);
	read(clienteFS, numeroParticiones, *tamanioNumeroParticiones);

	int* tamanioTiempoCompactacion = malloc(sizeof(int));
	read(clienteFS, tamanioTiempoCompactacion, sizeof(int));
	int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
	read(clienteFS, tiempoCompactacion, *tamanioTiempoCompactacion);
	// aca ya tengo toda la metadata, falta guardarla en struct

	metadataTabla* data = malloc(8 + 4);    // 2 int = 2*4 bytes
	data->consistencia = malloc(strlen(tipoConsistenciaCortada));

	memcpy(&data->particiones, numeroParticiones, sizeof(int));
	memcpy(data->consistencia, tipoConsistenciaCortada,
			strlen(tipoConsistenciaCortada));
	memcpy(&data->tiempoCompactacion, tiempoCompactacion, sizeof(int));


	//free(metadata);

	sem_post(&sem2);
	return data;
}

void realizarDescribeGlobal() {
//	sem_wait(&sem2); todo esto bloquea, lo comento

	// serializo peticion
	void* buffer = malloc(2 * sizeof(int));
	int peticion = 6;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));
	send(clienteFS, buffer, 2 * sizeof(int), 0);

	// deserializo
	int *tamanioTabla = malloc(sizeof(int));
	read(clienteFS, tamanioTabla, sizeof(int));
	while(*tamanioTabla != 0){
		char *tabla = malloc(*tamanioTabla);
		read(clienteFS, tabla, *tamanioTabla);
		char *tablaCortada = string_substring_until(tabla, *tamanioTabla);

		int *tamanioConsistencia = malloc(sizeof(int));
		read(clienteFS, tamanioConsistencia, sizeof(int));
		char *tipoConsistencia = malloc(*tamanioConsistencia);
		read(clienteFS, tipoConsistencia, *tamanioConsistencia);
		char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia, *tamanioConsistencia);

		int* tamanioNumeroParticiones = malloc(sizeof(int));
		read(clienteFS, tamanioNumeroParticiones, sizeof(int));
		int* numeroParticiones = malloc(*tamanioNumeroParticiones);
		read(clienteFS, numeroParticiones, *tamanioNumeroParticiones);

		int* tamanioTiempoCompactacion = malloc(sizeof(int));
		read(clienteFS, tamanioTiempoCompactacion, sizeof(int));
		int* tiempoCompactacion = malloc(*tamanioTiempoCompactacion);
		read(clienteFS, tiempoCompactacion, *tamanioTiempoCompactacion);

		char* mensajeALogear = malloc( strlen(" [DESCRIBE GLOBAL]:    ") + strlen(tablaCortada) + strlen(tipoConsistenciaCortada) + 2*sizeof(int) + 1);
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

void serServidor() {
	serverAddress.sin_family = AF_INET;
	serverAddress.sin_addr.s_addr = INADDR_ANY;
	serverAddress.sin_port = htons(t_archivoConfiguracion.PUERTO);
	//serverAddress.sin_port = htons(4092);

	server = socket(AF_INET, SOCK_STREAM, 0);

	setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if (bind(server, (void*) &serverAddress, sizeof(serverAddress)) != 0) {
		perror("Fallo el bind");
	}
	printf("Estoy escuchando\n");
	listen(server, 100);

	pthread_t threadKernel;
	int32_t idThreadKernel = pthread_create(&threadKernel, NULL, (void*)aceptar, NULL);

	//conectarseAKernel();
	int i = 0;
	while (t_archivoConfiguracion.PUERTO_SEEDS[i] != NULL) {
		int cliente = socket(AF_INET, SOCK_STREAM, 0);
		direccionCliente.sin_family = AF_INET;
		direccionCliente.sin_port = htons(
				atoi(t_archivoConfiguracion.PUERTO_SEEDS[i]));
		direccionCliente.sin_addr.s_addr = atoi(
				t_archivoConfiguracion.IP_SEEDS[i]);

		connect(cliente, (struct sockaddr *) &direccionCliente,
				sizeof(direccionCliente));
		//Mandar un 0 para q se sepa si es memoria o kernel

		char* buffer = malloc(1);
		strcpy(buffer, "0");

		send(cliente, &buffer, strlen(buffer), 0);
		struct datosMemoria *unaMemoria = malloc(sizeof(struct datosMemoria*));
		unaMemoria->socket = cliente;
		unaMemoria->direccionSocket = direccionCliente;
		unaMemoria->MEMORY_NUMBER = t_archivoConfiguracion.MEMORY_NUMBER;
		list_add(clientes, unaMemoria);

		pthread_t threadCliente;
		int32_t idThreadCliente = pthread_create(&threadCliente, NULL,
				gossiping, cliente);

		i++;
	}

	pthread_join(threadKernel, NULL);
}

void conectarseAFS() {
	clienteFS = socket(AF_INET, SOCK_STREAM, 0);
	serverAddressFS.sin_family = AF_INET;
	serverAddressFS.sin_port = htons(t_archivoConfiguracion.PUERTO_FS);
	serverAddress.sin_addr.s_addr = INADDR_ANY;

	connect(clienteFS, (struct sockaddr *) &serverAddressFS, sizeof(serverAddressFS));
	
	int *tamanioValue = malloc(sizeof(int));
	recv(clienteFS, tamanioValue, sizeof(int), 0);

	memcpy(&tamanoValue, tamanioValue, sizeof(int));

	sem_post(&sem);
}

void tomarPeticionSelect(int kernel) {
	//Recibe lo que le manda el kernel
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);

	int *tamanioKey = malloc(sizeof(int));
	recv(kernel, tamanioKey, sizeof(int), 0);
	char *key = malloc(*tamanioKey);
	recv(kernel, key, *tamanioKey, 0);

	//Se lo pide al FS
	char* value = malloc(tamanoValue);
	value = realizarSelect(tabla, key);
	int tamanioValue = strlen(value);
	char *buffer = malloc(sizeof(int) + strlen(value));
	memcpy(buffer, &tamanioValue, sizeof(int));
	memcpy(buffer + sizeof(int), value, strlen(value));
	send(kernel, buffer, strlen(value), 0);
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
	printf("value: %s\n", value);

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

	realizarCreate(tabla, consistencia, numeroDeParticiones,
			tiempoCompactacion);
}

void tomarPeticionDescribe1Tabla(int kernel){
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	//printf("tabla: %s\n", tabla);

	metadataTabla* metadata = realizarDescribe(tabla);

	// serializo paquete
	int tamanioBuffer = strlen(metadata->consistencia)+1 + 5*sizeof(int);
	void* buffer = malloc(tamanioBuffer);

	int tamanioMetadataConsistency = strlen(metadata->consistencia)+1;
	memcpy(buffer, &tamanioMetadataConsistency, sizeof(int));
	memcpy(buffer + sizeof(int), metadata->consistencia, tamanioMetadataConsistency);

	int tamanioParticiones = sizeof(int);
	memcpy(buffer + sizeof(int) + tamanioMetadataConsistency, &tamanioParticiones, sizeof(int));
	memcpy(buffer + 2*sizeof(int) + tamanioMetadataConsistency, &metadata->particiones, sizeof(int));

	int tamanioCompactacion = sizeof(int);
	memcpy(buffer + 3*sizeof(int) + tamanioMetadataConsistency, &tamanioCompactacion, sizeof(int));
	memcpy(buffer + 4*sizeof(int) + tamanioMetadataConsistency, &metadata->tiempoCompactacion, sizeof(int));

	send(kernel, buffer, tamanioBuffer, 0);
}

void tomarPeticionDescribeGlobal(int kernel){
	// primer paso : pedirselo al fs
	// serializo peticion
	void* buffer = malloc(2 * sizeof(int));
	int peticion = 6;
	int tamanioPeticion = sizeof(int);
	memcpy(buffer, &tamanioPeticion, sizeof(int));
	memcpy(buffer + sizeof(int), &peticion, sizeof(int));
	send(clienteFS, buffer, 2 * sizeof(int), 0);

	// deserializo
	int *tamanioTabla2 = malloc(sizeof(int));
	read(clienteFS, tamanioTabla2, sizeof(int));
	while(*tamanioTabla2 != 0){
		char *tabla = malloc(*tamanioTabla2);
		read(clienteFS, tabla, *tamanioTabla2);
		char *tablaCortada = string_substring_until(tabla, *tamanioTabla2);

		int *tamanioConsistencia = malloc(sizeof(int));
		read(clienteFS, tamanioConsistencia, sizeof(int));
		char *tipoConsistencia = malloc(*tamanioConsistencia);
		read(clienteFS, tipoConsistencia, *tamanioConsistencia);
		char *tipoConsistenciaCortada = string_substring_until(tipoConsistencia, *tamanioConsistencia);

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

		void* buffer2 = malloc( strlen(tablaCortada) + strlen(tipoConsistenciaCortada) + 6*sizeof(int) );

		int tamanioTabla = strlen(tablaCortada);
		memcpy(buffer2, &tamanioTabla, sizeof(int));
		memcpy(buffer2 + sizeof(int), tablaCortada, strlen(tablaCortada));

		int tamanioMetadataConsistency = strlen(tipoConsistenciaCortada);
		memcpy(buffer2 + sizeof(int) + strlen(tablaCortada), &tamanioMetadataConsistency, sizeof(int));
		memcpy(buffer2 + 2*sizeof(int) + strlen(tablaCortada), tipoConsistenciaCortada, strlen(tipoConsistenciaCortada));

		int tamanioParticiones = sizeof(int);
		memcpy(buffer2 + 2*sizeof(int) + strlen(tablaCortada) + strlen(tipoConsistenciaCortada), &tamanioParticiones, sizeof(int));
		memcpy(buffer2 + 3*sizeof(int) + strlen(tablaCortada) + strlen(tipoConsistenciaCortada), numeroParticiones, sizeof(int));

		int tamanioCompactacion = sizeof(int);
		memcpy(buffer2 + 4*sizeof(int)+ strlen(tablaCortada) + strlen(tipoConsistenciaCortada), &tamanioCompactacion, sizeof(int));
		memcpy(buffer2 + 5*sizeof(int)+ strlen(tablaCortada) + strlen(tipoConsistenciaCortada), tiempoCompactacion, sizeof(int));

		send(kernel, buffer2, strlen(tablaCortada) + strlen(tipoConsistenciaCortada) + 6*sizeof(int), 0);

		read(clienteFS, tamanioTabla2, sizeof(int));
	}

	char* buffer2 = malloc(4);
	int respuesta = 0;
	memcpy(buffer2, &respuesta, sizeof(int));
	send(kernel, buffer2, sizeof(int), 0);

}

void tomarPeticionDrop(int kernel){
	int *tamanioTabla = malloc(sizeof(int));
	recv(kernel, tamanioTabla, sizeof(int), 0);
	char *tabla = malloc(*tamanioTabla);
	recv(kernel, tabla, *tamanioTabla, 0);
	//printf("tabla: %s\n", tabla);
	realizarDrop(tabla);
}

void tratarKernel(int kernel) {
	while (1) {
		int *tamanio = malloc(sizeof(int));
		read(kernel, tamanio, sizeof(int));
		int *operacion = malloc(*tamanio);
		read(kernel, operacion, sizeof(int));
		switch (*operacion) {
			case 0: //Select
				tomarPeticionSelect(kernel);
				break;
			case 1: //Insert
				tomarPeticionInsert(kernel);
				break;
			case 2: //create
				tomarPeticionCreate(kernel);
				break;
			case 3: //Describe una tabla
				tomarPeticionDescribe1Tabla(kernel);
				break;
			case 4: //Describe Global
				tomarPeticionDescribeGlobal(kernel);
				break;
			case 5: //Drop
				tomarPeticionDrop(kernel);
				break;
			case 6: //JOURNAL
				ejecutarJournaling();
				break;

		}

	}
}}

void aceptar() {
	while (1) {
		int cliente = accept(server, (void*) &serverAddress, &tamanoDireccion);
		char* buf = malloc(2);
		recv(cliente, buf, 2, 0);
		if (atoi(buf) == 0) {
			pthread_t threadTratarCliente;
			int32_t idThreadTratarCliente = pthread_create(&threadTratarCliente,
					NULL, tratarCliente, cliente);
		} else {
			printf("BIEN\n");
			pthread_t threadTratarCliente;
			int32_t idThreadTratarCliente = pthread_create(&threadTratarCliente,
					NULL, tratarKernel, cliente);
			/*struct datosMemoria *unaMemoria = malloc(
					sizeof(struct datosMemoria*));
			unaMemoria->direccionSocket = serverAddress;
			unaMemoria->socket = cliente;
			pthread_t threadCliente;
			int32_t idThreadCliente = pthread_create(&threadCliente, NULL,
					gossiping, cliente);
			list_add(clientes, unaMemoria);*/
			pthread_join(threadTratarCliente, NULL);
		}
	}
}

void gossiping(int cliente) {
	while (1) {
		sleep(t_archivoConfiguracion.RETARDO_GOSSIPING);
		send(cliente, clientes, sizeof(clientes), 0);
	}
}

void tratarCliente(int cliente) {
	while (1) {
		t_list* lista;
		recv(cliente, lista, sizeof(lista), 0);
		list_add_all(clientes, lista);
	}
}

