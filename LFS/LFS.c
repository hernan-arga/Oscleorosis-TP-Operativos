// FS
/*
 * FALTANTES POR PRIORIDADES :
 *
 * - AGREGAR VARIABLE EN EL SELECT PARA QUE IMPRIMA POR CONSOLA SOLO CUANDO SE USE LA MISMA
 * - ARREGLO DE RETORNO EN SELECT -> comunicacion entre modulos -> OJO
 * - VERIFICAR CON VALGRIND QUE NO PIERDA MEMORIA EN NINGUN LADO
 * - CAMBIAR LOS PRINTF A LOGS PARA QUE NO TARDE AL IMPRIMIR EN PANTALLA (si alcanza el tiempo)
 *
 */
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
#include <pthread.h>
#include <commons/collections/dictionary.h>
#include <commons/config.h>
#include <commons/log.h>
#include <commons/string.h>
#include <commons/bitarray.h>
#include <commons/collections/queue.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <math.h>
#include <commons/log.h>
#include <semaphore.h>

//#define TRUE 1
//#define FALSE 0
//#define PORT 4444

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, OPERACIONINVALIDA
} OPERACION;

typedef struct {
	int timestamp;
	int key;
	char* value;
} t_registro;

typedef struct {
	char *PUNTO_MONTAJE;
	int PUERTO_ESCUCHA;
	int RETARDO;
	int TAMANIO_VALUE;
	int TIEMPO_DUMP;
} configuracionLFS;

typedef struct {
	int PARTITIONS;
	char *CONSISTENCY;
	int COMPACTION_TIME;
} metadataTabla;

typedef struct {
	char *registros;
	char *tablaALaQuePertenece;
} binarioCompactacion;


 int32_t iniciarConexion();
 void tomarPeticionSelect(int sd);
 void tomarPeticionCreate(int sd);
 void tomarPeticionInsert(int sd);


void atenderPeticionesDeConsola();
void dameSemaforo(char *tabla, sem_t **semaforoTabla);
void levantarHiloCompactacion(char *);
void levantarHilosCompactacionParaTodasLasTablas();
void actualizarTiempoDeRetardo();

t_dictionary * memtable; // creacion de memtable : diccionario que tiene las tablas como keys y su data es un array de p_registro 's.
//t_dictionary *tablasQueTienenTMPs; //guardo las tablas que tienen tmps para que en la compactacion solo revise esas
t_dictionary *binariosParaCompactar;
t_dictionary *diccionarioDeSemaforos;

metadataTabla describeUnaTabla(char *, int);
t_dictionary *describeTodasLasTablas(int);
void dump();
void dumpPorTabla(char*);
int contarLosDigitos(int);
void crearArchivoDeBloquesConRegistros(int, char*);
void crearArchivoConBloques(char*, char*, int);
int cuantosDumpeosHuboEnLaTabla(char *);

void compactacion(char*);
void verificarCompactacion(char *);
void actualizarRegistros(char *);
void renombrarTodosLosTMPATMPC(char*);
void actualizarRegistrosCon1TMPC(char *, char *);
char *levantarRegistros(char *);
void levantarRegistroDe1Bloque(char *, char **);
void tomarLosTmpc(char *, t_queue *);
void evaluarRegistro(char *, char *, t_list **);
void compararRegistros(int, int, char *, binarioCompactacion *, char*);
void comparar1RegistroBinarioCon1NuevoRegistro(int, int, char *, char **, int *);
void actualizarBin(char *);
void liberarBloques(char *);
void desasignarBloqueDelBitarray(int);

void drop(char*);

void insert(char*, char*, char*, char*);
int existeUnaListaDeDatosADumpear();

char* realizarSelect(char*, char*);

void create(char*, char*, char*, char*);
void crearMetadata(char*, char*, char*, char*);
void crearBinarios(char*, int);
int asignarBloque();
void crearArchivoDeBloquesVacio(char*, int);

//Funciones Auxiliares
int existeCarpeta(char*);
int existeLaTabla(char*);
void tomarPeticion(char*);
void separarPorComillas(char*, char* *, char* *, char* *);
void realizarPeticion(char**);
OPERACION tipoDePeticion(char*);
int cantidadValidaParametros(char**, int);
int parametrosValidos(int, char**, int (*criterioTiposCorrectos)(char**, int));
int esUnNumero(char* cadena);
int esUnTipoDeConsistenciaValida(char*);
int cantidadDeElementosDePunteroDePunterosDeChar(char** puntero);
//t_registro** obtenerDatosParaKeyDeseada(FILE *, int);
void obtenerDatosParaKeyDeseada(FILE *archivoBloque, int key,
		t_registro*vectorStructs[], int *cant, char* charProximoBloque,
		char* charAnteriorBloque);
void crearMetadataBloques();
int tamanioEnBytesDelBitarray();
//void actualizarBitArray();
void verBitArray();
void levantarFileSystem();
void levantarConfiguracionLFS();
void crearArchivoBitmap();
void iniciarMmap();
void crearArrayPorKeyMemtable(t_registro** arrayPorKeyDeseadaMemtable,
		t_list*entradaTabla, int key, int *cant, char* tabla);
int estaEntreComillas(char*);

t_config* configLFS;
configuracionLFS structConfiguracionLFS;
t_bitarray* bitarrayBloques;
char *mmapDeBitmap;
t_dictionary* diccionarioDescribe;
pthread_t hiloLevantarConexion;



int main(int argc, char *argv[]) {
	//tablasQueTienenTMPs = dictionary_create();
	binariosParaCompactar = dictionary_create();
	diccionarioDeSemaforos = dictionary_create();
	pthread_t hiloDump;
	pthread_t atenderPeticionesConsola;
	levantarConfiguracionLFS();
	levantarFileSystem();
	iniciarMmap();
	bitarrayBloques = bitarray_create(mmapDeBitmap,
			tamanioEnBytesDelBitarray());
	//verBitArray();
	pthread_create(&hiloLevantarConexion, NULL, iniciarConexion, NULL);
	//pthread_create(&hiloDump, NULL, (void*) dump, NULL);
	pthread_create(&atenderPeticionesConsola, NULL,
			(void*) atenderPeticionesDeConsola, NULL);
	//levantarHilosCompactacionParaTodasLasTablas();
	memtable = malloc(4);
	memtable = dictionary_create();

	diccionarioDescribe = malloc(4000);
	diccionarioDescribe = dictionary_create();

	//Se queda esperando a que termine el hilo de escuchar peticiones
	pthread_join(hiloLevantarConexion, NULL);
	//pthread_join(hiloDump, NULL);
	pthread_join(atenderPeticionesConsola, NULL);
	//Aca se destruye el bitarray?
	//bitarray_destroy(bitarrayBloques);
	return 0;
}


/*
int main(int argc, char *argv[]) {
	//tablasQueTienenTMPs = dictionary_create();
	binariosParaCompactar = dictionary_create();
	diccionarioDeSemaforos = dictionary_create();
	pthread_t hiloLevantarConexion;
	pthread_t hiloDump;
	pthread_t atenderPeticionesConsola;
	levantarConfiguracionLFS();
	levantarFileSystem();
	iniciarMmap();
	bitarrayBloques = bitarray_create(mmapDeBitmap,
			tamanioEnBytesDelBitarray());
	//verBitArray();
	pthread_create(&hiloLevantarConexion, NULL, (void*) iniciarConexion, NULL);
	pthread_create(&hiloDump, NULL, (void*) dump, NULL);
	pthread_create(&atenderPeticionesConsola, NULL,
			(void*) atenderPeticionesDeConsola, NULL);
	levantarHilosCompactacionParaTodasLasTablas();
	memtable = malloc(4);
	memtable = dictionary_create();

	diccionarioDescribe = malloc(4000);
	diccionarioDescribe = dictionary_create();

	//Se queda esperando a que termine el hilo de escuchar peticiones
	pthread_join(hiloLevantarConexion, NULL);
	pthread_join(hiloDump, NULL);
	pthread_join(atenderPeticionesConsola, NULL);
	//Aca se destruye el bitarray?
	//bitarray_destroy(bitarrayBloques);
	return 0;
} */

void atenderPeticionesDeConsola() {
	while (1) {
		printf("SELECT | INSERT | CREATE |\n");
		char* mensaje = malloc(1000);
		do {
			fgets(mensaje, 1000, stdin);
		} while (!strcmp(mensaje, "\n"));
		tomarPeticion(mensaje);
		free(mensaje);
		//verBitArray();
	}
}

void levantarFileSystem() {
	if (!existeCarpeta("Tables")) {
		mkdir(
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
	}
	if (!existeCarpeta("Bloques")) {
		mkdir(
				string_from_format("%sBloques/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
	}
	if (!existeCarpeta("Metadata")) {
		mkdir(
				string_from_format("%sMetadata/",
						structConfiguracionLFS.PUNTO_MONTAJE), 0777);
		//La metadata de bloques le defini algunos valores por defecto
		crearMetadataBloques();
		crearArchivoBitmap();
	}
}

void levantarConfiguracionLFS() {
	char *pathConfiguracion = string_new();
	//¿Cuando dice una ubicacion conocida se refiere a que esta hardcodeada asi?
	string_append(&pathConfiguracion, "configLFS.config");
	configLFS = config_create(pathConfiguracion);
	structConfiguracionLFS.PUERTO_ESCUCHA = config_get_int_value(configLFS,
			"PUERTO_ESCUCHA");
	//Lo que sigue abajo lo hago porque el punto de montaje ya tiene comillas, entonces se las tengo que sacar por
	//que sino queda ""home/carpeta""
	char *puntoMontaje = string_new();
	char *puntoMontajeSinComillas = string_new();
	string_append(&puntoMontaje,
			config_get_string_value(configLFS, "PUNTO_MONTAJE"));
	//saco la doble comilla del principio y la del final
	string_append(&puntoMontajeSinComillas,
			string_substring(puntoMontaje, 1, strlen(puntoMontaje) - 2));

	structConfiguracionLFS.PUNTO_MONTAJE = puntoMontajeSinComillas;
	structConfiguracionLFS.TAMANIO_VALUE = config_get_int_value(configLFS,
			"TAMANIO_VALUE");
	//El dump y el retardo tienen que poder modificarse en tiempo de ejecucion
	//asi que tendria que volver a tomar su valor cuando los vaya a usar
	structConfiguracionLFS.RETARDO = config_get_int_value(configLFS, "RETARDO");
	config_destroy(configLFS);
}

void tomarPeticion(char* mensaje) {
	char* value;
	char* noValue = string_new();
	char *posibleTimestamp = string_new();
	separarPorComillas(mensaje, &value, &noValue, &posibleTimestamp);
	char** mensajeSeparado = malloc(strlen(mensaje) + 1);
	char** mensajeSeparadoConValue = malloc(strlen(mensaje) + 1);
	;
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
	realizarPeticion(mensajeSeparadoConValue);
	free(mensajeSeparado);
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

		if (parametrosValidos(2, parametros, (void*) criterioSelect)) {
			char* tabla = parametros[1];
			char* key = parametros[2];

			char* tablaMayusculas = string_new();
			string_append(&tablaMayusculas, tabla);
			string_to_upper(tablaMayusculas);
			//sem_t *semaforoTabla;
			//close y unlink por si ya estaba abierto el semaforo
			//sem_close(semaforoTabla);
			//sem_unlink(tablaMayusculas);
			//la key del diccionario esta en mayusculas para cada tabla

			//dameSemaforo(tablaMayusculas, &semaforoTabla);
			//sem_wait(semaforoTabla);
				//Seccion critica
				realizarSelect(tabla, key);
			//sem_post(semaforoTabla);
		}

		break;

	case INSERT:
		printf("Seleccionaste Insert\n");
		int criterioInsert(char** parametros, int cantidadDeParametrosUsados) {
			char* key = parametros[2];
			char* value = parametros[3];
			int tamanioValueInsertado = strlen(value) - 2; //2 por las comillas
			if (tamanioValueInsertado > structConfiguracionLFS.TAMANIO_VALUE) {
				printf("El valor a insertar es demasiado grande.\n");
			}
			if (!esUnNumero(key)) {
				printf("La key debe ser un numero.\n");
			}
			if (!estaEntreComillas(value)) {
				printf("El valor a insertar debe estar entre comillas.\n");
			}

			if (cantidadDeParametrosUsados == 4) {
				char* timestamp = parametros[4];
				if (!esUnNumero(timestamp)) {
					printf("El timestamp debe ser un numero.\n");
				}
				return (tamanioValueInsertado
						<= structConfiguracionLFS.TAMANIO_VALUE)
						&& esUnNumero(key) && esUnNumero(timestamp)
						&& estaEntreComillas(value);
			}
			return (tamanioValueInsertado
					<= structConfiguracionLFS.TAMANIO_VALUE) && esUnNumero(key)
					&& estaEntreComillas(value);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(4, parametros, (void *) criterioInsert)) {
			char *tabla = parametros[1];
			char *key = parametros[2];
			//le saco las comillas al valor
			char *valor = string_substring(parametros[3], 1,
					string_length(parametros[3]) - 2);
			char *timestamp = parametros[4];

			char* tablaMayusculas = string_new();
			string_append(&tablaMayusculas, tabla);
			string_to_upper(tablaMayusculas);
			//sem_t *semaforoTabla;
			//la key del diccionario esta en mayusculas para cada tabla

			//dameSemaforo(tablaMayusculas, &semaforoTabla);
			//sem_wait(semaforoTabla);
				//Seccion critica
			insert(tabla, key, valor, timestamp);

			//sem_post(semaforoTabla);


		} else if (parametrosValidos(3, parametros, (void *) criterioInsert)) {
			char *tabla = parametros[1];
			char *key = parametros[2];
			//le saco las comillas al valor
			char *valor = string_substring(parametros[3], 1,
					string_length(parametros[3]) - 2);
			//¿El timestamp nesecita conversion? esto esta en segundos y no hay tipo de dato que banque los milisegundos por el tamanio
			long int timestampActual = (long int) time(NULL);
			char* timestamp = string_itoa(timestampActual);

			char* tablaMayusculas = string_new();
			string_append(&tablaMayusculas, tabla);
			string_to_upper(tablaMayusculas);
			//sem_t *semaforoTabla;

			//la key del diccionario esta en mayusculas para cada tabla

			//dameSemaforo(tablaMayusculas, &semaforoTabla);
			//sem_wait(semaforoTabla);
				//Seccion critica
				insert(tabla, key, valor, timestamp);
			//sem_post(semaforoTabla);
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

			char* tablaMayusculas = string_new();
			string_append(&tablaMayusculas, tabla);
			string_to_upper(tablaMayusculas);
			sem_t *semaforoTabla;
			//close y unlink por si ya estaba abierto el semaforo
			//sem_close(semaforoTabla);
			//sem_unlink(tablaMayusculas);
			//la key del diccionario esta en mayusculas para cada tabla
					//dameSemaforo(tablaMayusculas, &semaforoTabla);
			/*int value;
			 sem_getvalue(semaforoTabla, &value);
			 printf("create: %i\n", value);*/
					//sem_wait(semaforoTabla);
			//seccion critica
			create(tabla, consistencia, cantidadParticiones,
					tiempoCompactacion);
					//sem_post(semaforoTabla);
			/*sem_getvalue(semaforoTabla, &value);
			 printf("create: %i\n", value);*/
			//sem_unlink(tabla);
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

			sem_t *semaforoTabla;
			dameSemaforo(tabla, &semaforoTabla);

			sem_wait(semaforoTabla);
			//seccion critica
			drop(tabla);
			sem_post(semaforoTabla);
			sem_close(semaforoTabla);
			sem_unlink(tabla);
			dictionary_remove(diccionarioDeSemaforos, tabla);

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
			//Los semaforos para esta funcion estan adentro de esta cuando llama a describeUnaTabla
			//el 1 es para que imprima por pantalla
			describeTodasLasTablas(1);
		}
		if (parametrosValidos(1, parametros,
				(void *) criterioDescribeUnaTabla)) {
			char* tabla = parametros[1];
			string_to_upper(tabla);

			sem_t *semaforoTabla;
			//la key del diccionario esta en mayusculas para cada tabla
			dameSemaforo(tabla, &semaforoTabla);
			sem_wait(semaforoTabla);
			//Seccion critica
			//El 1 es para imprimir por pantalla
			describeUnaTabla(tabla, 1);
			sem_post(semaforoTabla);
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
					cantidadDeParametrosNecesarios);
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
				if (!strcmp(peticion, "DROP")) {
					free(peticion);
					return DROP;
				} else {
					if (!strcmp(peticion, "DESCRIBE")) {
						free(peticion);
						return DESCRIBE;
					} else {
						free(peticion);
						return OPERACIONINVALIDA;
					}
				}
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

void actualizarTiempoDeRetardo() {
	char *pathConfiguracion = string_new();
	string_append(&pathConfiguracion, "configLFS.config");
	configLFS = config_create(pathConfiguracion);
	structConfiguracionLFS.RETARDO = config_get_int_value(configLFS, "RETARDO");
	//printf("retardo: %i", structConfiguracionLFS.RETARDO);*/
	//no se destruye el configLFS porque se levanta de todos lados
}

void insert(char* tabla, char* key, char* valor, char* timestamp) {
	//Puedo modificar en tiempo de ejecucion el retardo
	//actualizarTiempoDeRetardo();
	//sleep(structConfiguracionLFS.RETARDO);

	string_to_upper(tabla);
	if (!existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen(" No existe tabla con el nombre : ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, " No existe tabla con el nombre : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create(
				string_from_format("%s/erroresInsert.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		t_registro* p_registro = malloc(12); // 2 int = 2* 4        +       un puntero a char = 4
		p_registro->timestamp = atoi(timestamp);
		p_registro->key = atoi(key);
		p_registro->value = malloc(strlen(valor));
		strcpy(p_registro->value, valor);
		if (!existeUnaListaDeDatosADumpear(tabla)) {
			t_list* listaDeStructs = list_create();
			list_add(listaDeStructs, p_registro);
			dictionary_put(memtable, tabla, listaDeStructs);

			char* mensajeALogear = malloc(100 + strlen(string_itoa(p_registro->key)) +  strlen(string_itoa(p_registro->timestamp)));
			strcpy(mensajeALogear, "  /  SE CREO LA TABLA : ");
			strcat(mensajeALogear, tabla);
			strcat(mensajeALogear, "  /  TIMESTAMP : ");
			strcat(mensajeALogear, string_itoa(p_registro->timestamp));
			strcat(mensajeALogear, "  /  KEY : ");
			strcat(mensajeALogear, string_itoa(p_registro->key));
			strcat(mensajeALogear, "  /  VALUE : ");
			strcat(mensajeALogear, p_registro->value);
			t_log* g_logger;
			g_logger = log_create(string_from_format("%sinsert.log", structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,LOG_LEVEL_INFO);
			log_info(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);

		} else {
			t_list* listaDeStructs = dictionary_get(memtable, tabla);
			list_add(listaDeStructs, p_registro);
			dictionary_remove(memtable, tabla);
			dictionary_put(memtable, tabla, listaDeStructs);

			char* mensajeALogear = malloc(100 + strlen(string_itoa(p_registro->key)) +  strlen(string_itoa(p_registro->timestamp)));
			strcpy(mensajeALogear, "  /  TABLA A DUMPEAR : ");
			strcat(mensajeALogear, tabla);
			strcat(mensajeALogear, "  /  TIMESTAMP : ");
			strcat(mensajeALogear, string_itoa(p_registro->timestamp));
			strcat(mensajeALogear, "  /  KEY : ");
			strcat(mensajeALogear, string_itoa(p_registro->key));
			strcat(mensajeALogear, "  /  VALUE : ");
			strcat(mensajeALogear, p_registro->value);
			t_log* g_logger;
			g_logger = log_create(string_from_format("%sinsert.log", structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,LOG_LEVEL_INFO);
			log_info(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);
		}
	}
}

int existeUnaListaDeDatosADumpear(char* tabla) {
	return dictionary_has_key(memtable, tabla);
}

void dump() {
	while (1) {
		//Levanto el valor de tiempo de dump
		char *pathConfiguracion = string_new();
		string_append(&pathConfiguracion, "configLFS.config");
		configLFS = config_create(pathConfiguracion);
		structConfiguracionLFS.TIEMPO_DUMP = config_get_int_value(configLFS,
				"TIEMPO_DUMP");
		sleep(structConfiguracionLFS.TIEMPO_DUMP);
		//Con sleep tengo que meter un \n al final de un printf porque sino no imprime
		//printf("dump boy!\n");
		dictionary_iterator(memtable, (void *) dumpPorTabla);
		config_destroy(configLFS);
		//compactacion("/home/utnso/lissandra-checkpoint/Tables/TABLA1");
	}
}

void dumpPorTabla(char* tabla) {
	/*char *tablaPath = string_from_format("%sTables/",
	 structConfiguracionLFS.PUNTO_MONTAJE);
	 string_append(&tablaPath, tabla);*/
	if (existeLaTabla(tabla)) {
		//Tomo el tamanio por bloque de mi LFS
		char *metadataPath = string_from_format("%sMetadata/metadata.bin",
				structConfiguracionLFS.PUNTO_MONTAJE);
		t_config *metadata = config_create(metadataPath);
		int tamanioPorBloque = config_get_int_value(metadata, "BLOCK_SIZE");
		config_destroy(metadata);
		//t_registro* vectorStructs[100];
		//vectorStructs[0] = malloc(12);
		t_list *listaDeRegistros = dictionary_get(memtable, tabla);
		int i = 0;
		int cantidadDeBytesADumpear = 0;
		char *registrosADumpear = string_new();
		while (i < list_size(listaDeRegistros)) {
			t_registro *p_registro = list_get(listaDeRegistros, i);
			//memcpy(p_registro, list_get(listaDeRegistros, i), sizeof(p_registro));
			string_append(&registrosADumpear,
					string_itoa(p_registro->timestamp));
			string_append(&registrosADumpear, ";");
			string_append(&registrosADumpear, string_itoa(p_registro->key));
			string_append(&registrosADumpear, ";");
			string_append(&registrosADumpear, p_registro->value);
			string_append(&registrosADumpear, "\n");
			cantidadDeBytesADumpear += (strlen(p_registro->value)
					+ contarLosDigitos(p_registro->key)
					+ contarLosDigitos(p_registro->timestamp) + 3); //2 ; y un \n
			//printf("cantidadDeBytesADumpear: %i\n", cantidadDeBytesADumpear);
			i++;
		}
		//printf("\n\n");
		//Calcular bien la cantidad que necesito si hay un poquito mas que un bloque
		int cantidadDeBloquesCompletosNecesarios = cantidadDeBytesADumpear
				/ tamanioPorBloque;
		int cantidadDeComasNecesarias = cantidadDeBloquesCompletosNecesarios
				- 1;
		char *stringdelArrayDeBloques = string_new();
		//printf("cantidadDeBloquesCompletosNecesarios: %i\n", cantidadDeBloquesCompletosNecesarios);
		//Asigno los bloques y voy creando el array de bloques asignados
		string_append(&stringdelArrayDeBloques, "[");
		int desdeDondeTomarLosRegistros = 0;
		int hayMasDe1Bloque = 0;
		//Primero dump para los que ocupan 1 bloque entero sin fragmentacion interna
		while (cantidadDeBloquesCompletosNecesarios != 0) {
			int bloqueEncontrado = asignarBloque();
			char *stringAuxRegistros = string_new();
			string_append(&stringAuxRegistros,
					string_substring(registrosADumpear,
							desdeDondeTomarLosRegistros, tamanioPorBloque));
			//printf("%s\n", stringAuxRegistros);
			crearArchivoDeBloquesConRegistros(bloqueEncontrado,
					stringAuxRegistros);
			//Agrego al string del array el bloque nuevo
			string_append(&stringdelArrayDeBloques,
					string_itoa(bloqueEncontrado));
			if (cantidadDeComasNecesarias != 0) {
				string_append(&stringdelArrayDeBloques, ",");
			}
			cantidadDeBloquesCompletosNecesarios--;
			cantidadDeComasNecesarias--;
			desdeDondeTomarLosRegistros += tamanioPorBloque;
			//Esta variable es para que no quede una coma de mas en caso de que no haya mas de 1 bloque
			hayMasDe1Bloque = 1;
		}
		//insert tabla1 2 "holapepecomoestassddddaasssssswweeqqwwttppooiikkll" 	64 bytes
		//Ahora lo mismo para el que no completa 1 bloque
		cantidadDeBloquesCompletosNecesarios = cantidadDeBytesADumpear
				/ tamanioPorBloque;
		int remanenteEnBytes = cantidadDeBytesADumpear
				- cantidadDeBloquesCompletosNecesarios * tamanioPorBloque;
		//printf("Remanente: %i\n", remanenteEnBytes);
		if (remanenteEnBytes != 0) {
			int bloqueEncontrado = asignarBloque();
			char *stringAuxRegistros = string_new();
			string_append(&stringAuxRegistros,
					string_substring(registrosADumpear,
							desdeDondeTomarLosRegistros, remanenteEnBytes));
			crearArchivoDeBloquesConRegistros(bloqueEncontrado,
					stringAuxRegistros);
			if (hayMasDe1Bloque) {
				string_append(&stringdelArrayDeBloques, ",");
			}
			string_append(&stringdelArrayDeBloques,
					string_itoa(bloqueEncontrado));
		}
		string_append(&stringdelArrayDeBloques, "]");

		char *tablaPath = string_from_format("%sTables/",
				structConfiguracionLFS.PUNTO_MONTAJE);
		string_append(&tablaPath, tabla);
		//printf("%s\n", tablaPath);
		int numeroDeDumpeoCorrespondiente = cuantosDumpeosHuboEnLaTabla(
				tablaPath) + 1;
		char *directorioTMP = string_new();
		string_append(&directorioTMP, tablaPath);
		string_append(&directorioTMP, "/");
		string_append(&directorioTMP,
				string_itoa(numeroDeDumpeoCorrespondiente));
		string_append(&directorioTMP, ".tmp");
		//printf("%s\n", directorioTMP);
		crearArchivoConBloques(directorioTMP, stringdelArrayDeBloques,
				cantidadDeBytesADumpear);
		//antes de eliminarlo de la memtable lo pongo en el diccionario de tablasQueTienenTMPs porque sino se borra el string tambien
		//dictionary_put(tablasQueTienenTMPs, tabla, tablaPath);
	}
	//Si no existe no hago nada, solo la elimino de la memtable
	dictionary_remove(memtable, tabla);
}

int cuantosDumpeosHuboEnLaTabla(char *tablaPath) {
	int cantidadDeDumpeos = 0;
	DIR *directorio = opendir(tablaPath);
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABLAS existe alguna que tenga el mismo nombre
		if (string_ends_with(directorioALeer->d_name, ".tmp")) {
			/*closedir(directorio);
			 return 1;*/
			//char* nombreArchivoTemporal = string_split(directorioALeer->d_name, ".")[0];
			//El nombre es un numero (por ej. 1.tmp tiene el nombre "1" entonces...
			cantidadDeDumpeos++;
			//printf("%s\n", directorioALeer->d_name);
		}
	}
	closedir(directorio);
	return cantidadDeDumpeos;
}

int contarLosDigitos(int numero) {
	int contador = 0;
	int aux = numero;
	do {
		contador++;
		aux = (aux / 10);
	} while (aux != 0);
	return contador;
}

//crear archivo para tmps y particiones
void crearArchivoConBloques(char* directorioArchivo,
		char* stringdelArrayDeBloques, int tamanioDeLosBloques) {
	FILE *archivo = fopen(directorioArchivo, "w");
	t_config *configArchivo = config_create(directorioArchivo);
	config_set_value(configArchivo, "SIZE", string_itoa(tamanioDeLosBloques));
	//los bloques despues se levantan con config_get_array_value
	config_set_value(configArchivo, "BLOCKS", stringdelArrayDeBloques);
	config_save_in_file(configArchivo, directorioArchivo);
	fclose(archivo);
	config_destroy(configArchivo);
}

void crearArchivoDeBloquesConRegistros(int bloqueEncontrado,
		char* registrosAEscribir) {
	char* pathBloque = string_new();
	string_append(&pathBloque,
			string_from_format("%sBloques/",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&pathBloque, string_itoa(bloqueEncontrado));
	string_append(&pathBloque, ".bin");
	FILE *bloqueCreado = fopen(pathBloque, "w");
	fwrite(registrosAEscribir, sizeof(char), strlen(registrosAEscribir),
			bloqueCreado);
	fclose(bloqueCreado);
}

void levantarHiloCompactacion(char *pathTabla) {
	char *tabla = string_new();
	string_append(&tabla, pathTabla);
	pthread_t hiloCompactacion;
	pthread_create(&hiloCompactacion, NULL, (void*) verificarCompactacion,
			(void *) tabla);
	pthread_detach(hiloCompactacion);
	//Se rompe con el free. ¿detach ya lo hace?
	//free(tabla);
	//printf("-%s\n", pathTabla);
}

void levantarHilosCompactacionParaTodasLasTablas() {
	DIR *directorio = opendir(
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Busco la metadata de todas las tablas (evaluo que no ingrese a los directorios "." y ".."
		if ((directorioALeer->d_type) == DT_DIR
				&& strcmp((directorioALeer->d_name), ".")
				&& strcmp((directorioALeer->d_name), "..")) {
			char *pathTabla = string_new();
			string_append(&pathTabla,
					string_from_format("%sTables/",
							structConfiguracionLFS.PUNTO_MONTAJE));
			string_append(&pathTabla, directorioALeer->d_name);
			levantarHiloCompactacion(pathTabla);
		}
	}
	closedir(directorio);

}

void verificarCompactacion(char *pathTabla) {
	char *tabla = string_new();
	char *pathDeMontajeDeLasTablas = string_new();
	string_append(&pathDeMontajeDeLasTablas,
			structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathDeMontajeDeLasTablas, "Tables/");
	string_append(&tabla,
			string_substring_from(pathTabla, strlen(pathDeMontajeDeLasTablas)));

	while (existeLaTabla(tabla)) {
		//printf("%s\n", pathTabla);
		char *metadataTabla = string_new();
		string_append(&metadataTabla, pathTabla);
		string_append(&metadataTabla, "/metadata");
		//printf("%s\n", pathTabla);
		//sleep(10);
		//Levanto el valor de tiempo de compactacion de la tabla
		t_config *configTabla = config_create(metadataTabla);
		int tiempoCompactacion = config_get_int_value(configTabla,
				"COMPACTION_TIME");
		//printf("%i\n", tiempoCompactacion);
		sleep(tiempoCompactacion);
		//Con sleep tengo que meter un \n al final de un printf porque sino no imprime
		compactacion(pathTabla);
		free(metadataTabla);
		config_destroy(configTabla);
	}
}

void compactacion(char* pathTabla) {
	char *tabla = string_new();
	char *pathDeMontajeDeLasTablas = string_new();
	string_append(&pathDeMontajeDeLasTablas,
			structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathDeMontajeDeLasTablas, "Tables/");
	string_append(&tabla,
			string_substring_from(pathTabla, strlen(pathDeMontajeDeLasTablas)));

	//Me vuelvo a fijar que exista por si se borro mientras se accedia aca
	if (existeLaTabla(tabla)) {
		renombrarTodosLosTMPATMPC(pathTabla);
		actualizarRegistros(pathTabla);
	}
	//dictionary_iterator(tablasQueTienenTMPs, (void*) actualizarRegistros);
	//dictionary_clean(tablasQueTienenTMPs);
}

void actualizarRegistros(char *tablaPath) {
	t_queue *tmpcs = queue_create();
	tomarLosTmpc(tablaPath, tmpcs);
	//printf("hola");
	int i = 0;
	int cantidadDeElementosEnLaCola = queue_size(tmpcs);
	while (i < cantidadDeElementosEnLaCola) {
		actualizarRegistrosCon1TMPC(queue_pop(tmpcs), tablaPath);
		//printf("%s", queue_pop(tmpcs));
		//printf("%i - %i\n\n", queue_size(tmpcs), i);
		i++;
	}
}

void actualizarRegistrosCon1TMPC(char *tmpc, char *tablaPath) {
	//primero levanto los registros que tengo a nivel logico para tenerlos juntos
	char *registros = string_new();
	string_append(&registros, levantarRegistros(tmpc));
	char **registrosSeparados = string_split(registros, "\n");
	//cuando tengo todos los registros los separo y voy evaluando de a 1
	int i = 0;
	t_list *binariosAfectados = list_create();
	//printf("%s\n", registros);
	while (registrosSeparados[i] != NULL) {
		evaluarRegistro(registrosSeparados[i], tablaPath, &binariosAfectados);
		//printf("%i\n", i);
		i++;
	}

	sem_t *semaforoTabla;
	char *tabla = string_new();
	char *pathDeMontajeDeLasTablas = string_new();
	string_append(&pathDeMontajeDeLasTablas,
			structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathDeMontajeDeLasTablas, "Tables/");
	string_append(&tabla,
			string_substring_from(tablaPath, strlen(pathDeMontajeDeLasTablas)));
	dameSemaforo(tabla, &semaforoTabla);
	sem_wait(semaforoTabla);
	//Temporizador para ver cuanto tiempo estuvo bloqueada la tabla para compactacion
	time_t inicio;
	time_t fin;
	time_t delta;
	inicio = time(NULL);

	//Seccion critica aca
	liberarBloques(tmpc);
	remove(tmpc);
	list_iterate(binariosAfectados, (void*) actualizarBin);
	list_clean(binariosAfectados);
	sem_post(semaforoTabla);
	fin = time(NULL);
	delta = fin - inicio;
	//printf("delta: %i\n", (int)delta);
	char* mensajeALogear =
			malloc(
					strlen(
							"Compactacion realizada con exito se bloqueo la tabla: , un total de  segundos")
							+ contarLosDigitos((long int) delta) * sizeof(int)
							+ strlen(tabla));
	strcpy(mensajeALogear,
			"Compactacion realizada con exito se bloqueo la tabla: ");
	strcat(mensajeALogear, tabla);
	strcat(mensajeALogear, ", un total de ");
	strcat(mensajeALogear, string_itoa((long int) delta));
	strcat(mensajeALogear, " segundos");
	t_log* g_logger;
	g_logger = log_create(
			string_from_format("%sbloqueoEntreCompactacion.log",
					structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,
			LOG_LEVEL_INFO);
	log_info(g_logger, mensajeALogear);
	log_destroy(g_logger);
	free(mensajeALogear);

	/*int value;
	 sem_getvalue(semaforoTabla, &value);
	 //printf("compactacion: %i\n", value);*/

	/**/
	//Desbloquear la tabla y dejar registro de cuanto tiempo estuvo bloqueada la tabla
}

/*void imprimir(char *key){
 printf("key:%s\n", key);
 }*/

void dameSemaforo(char *tabla, sem_t **semaforoTabla) {
	//Si se queda trabado en una tabla en particular descomentar lo de abajo, y ejecutar algun comando con la tabla
	//para limpiar el semaforo
	//sem_close(*semaforoTabla);
	//sem_unlink(tabla);
	if (!dictionary_has_key(diccionarioDeSemaforos, tabla)) {
		*semaforoTabla = sem_open(tabla, O_CREAT, 0777, 1);
		if (*semaforoTabla == SEM_FAILED) {
			sem_close(*semaforoTabla);
			sem_unlink(tabla);
			printf("%s\n", tabla);
			perror("Fallo al levantar el semaforo de la tabla");
			exit(-1);
		}
		dictionary_put(diccionarioDeSemaforos, tabla, *semaforoTabla);
		//printf("no existe\n");
	}
	*semaforoTabla = dictionary_get(diccionarioDeSemaforos, tabla);
	//return semaforoTabla;
}

void actualizarBin(char *pathBin) {
	//Libero los bloques del binario
	binarioCompactacion *unBinario = dictionary_get(binariosParaCompactar,
			pathBin);
	liberarBloques(pathBin);
	char * registrosNuevos = malloc(strlen(unBinario->registros) + 1);
	strcpy(registrosNuevos, unBinario->registros);
	//printf("%s\n", unBinario->registros);
	//char *bloquesAsignados = string_new();
	//printf("%s\n", registrosNuevos);

	//Tomo el tamanio por bloque de mi LFS
	char *metadataPath = string_from_format("%sMetadata/metadata.bin",
			structConfiguracionLFS.PUNTO_MONTAJE);
	t_config *metadata = config_create(metadataPath);
	int tamanioPorBloque = config_get_int_value(metadata, "BLOCK_SIZE");
	config_destroy(metadata);
	int cantidadDeBytesAEscribir = 0;
	cantidadDeBytesAEscribir += strlen(registrosNuevos);
	int totalDeBytesDelBin = cantidadDeBytesAEscribir;

	//Cantidad de bloques enteros
	int cantidadDeBloquesCompletosNecesarios = cantidadDeBytesAEscribir
			/ tamanioPorBloque;
	int cantidadDeComasNecesarias = cantidadDeBloquesCompletosNecesarios - 1;
	char *stringdelArrayDeBloques = string_new();
	//printf("cantidadDeBloquesCompletosNecesarios: %i\n", cantidadDeBloquesCompletosNecesarios);
	//Asigno los bloques y voy creando el array de bloques asignados
	string_append(&stringdelArrayDeBloques, "[");
	int desdeDondeTomarLosRegistros = 0;
	int hayMasDe1Bloque = 0;
	//Primero  para los que ocupan 1 bloque entero sin fragmentacion interna
	while (cantidadDeBloquesCompletosNecesarios != 0) {
		int bloqueEncontrado = asignarBloque();
		char *stringAuxRegistros = string_new();
		string_append(&stringAuxRegistros,
				string_substring(registrosNuevos, desdeDondeTomarLosRegistros,
						tamanioPorBloque));
		//printf("%s\n", stringAuxRegistros);
		crearArchivoDeBloquesConRegistros(bloqueEncontrado, stringAuxRegistros);
		//Agrego al string del array el bloque nuevo
		string_append(&stringdelArrayDeBloques, string_itoa(bloqueEncontrado));
		if (cantidadDeComasNecesarias != 0) {
			string_append(&stringdelArrayDeBloques, ",");
		}
		cantidadDeBloquesCompletosNecesarios--;
		cantidadDeComasNecesarias--;
		desdeDondeTomarLosRegistros += tamanioPorBloque;
		//Esta variable es para que no quede una coma de mas en caso de que no haya mas de 1 bloque
		hayMasDe1Bloque = 1;
	}
	//insert tabla1 2 "holapepecomoestassddddaasssssswweeqqwwttppooiikkll" 	64 bytes
	//Ahora lo mismo para el que no completa 1 bloque
	cantidadDeBloquesCompletosNecesarios = cantidadDeBytesAEscribir
			/ tamanioPorBloque;
	int remanenteEnBytes = cantidadDeBytesAEscribir
			- cantidadDeBloquesCompletosNecesarios * tamanioPorBloque;
	//printf("Remanente: %i\n", remanenteEnBytes);
	if (remanenteEnBytes != 0) {
		int bloqueEncontrado = asignarBloque();
		char *stringAuxRegistros = string_new();
		string_append(&stringAuxRegistros,
				string_substring(registrosNuevos, desdeDondeTomarLosRegistros,
						remanenteEnBytes));
		crearArchivoDeBloquesConRegistros(bloqueEncontrado, stringAuxRegistros);
		if (hayMasDe1Bloque) {
			string_append(&stringdelArrayDeBloques, ",");
		}
		string_append(&stringdelArrayDeBloques, string_itoa(bloqueEncontrado));
	}
	string_append(&stringdelArrayDeBloques, "]");

	//Creo el binario con el stringdelArrayDeBloques y totalDeBytesDelBin
	crearArchivoConBloques(pathBin, stringdelArrayDeBloques,
			totalDeBytesDelBin);

}

void liberarBloques(char *pathArchivo) {
	t_config *configArchivo = config_create(pathArchivo);
	char **arrayDeBloques = config_get_array_value(configArchivo, "BLOCKS");
	int i = 0;
	while (arrayDeBloques[i] != NULL) {
		desasignarBloqueDelBitarray(atoi(arrayDeBloques[i]));
		i++;
	}
}

void desasignarBloqueDelBitarray(int bloque) {
	if (bitarray_test_bit(bitarrayBloques, bloque) == 1) {
		//verBitArray();
		bitarray_clean_bit(bitarrayBloques, bloque);
		//printf("Bloque %i -------\n", bloque);
		//printf(">>>%i\n", bitarray_test_bit(bitarrayBloques, bloque));
		//verBitArray();
	} else {
		printf("Se esta tratando de desasignar un bloque libre\n");
	}
	//verBitArray();
}

//En un diccionario voy a guardar los registros actualizados de determinada tabla
//y a que tabla pertenecen para cuando tenga que bloquear la misma
void evaluarRegistro(char *registro, char *tablaPath,
		t_list **binariosAfectados) {
	char* metadataPath = string_new();
	string_append(&metadataPath, tablaPath);
	string_append(&metadataPath, "/metadata");
	t_config *metadata = config_create(metadataPath);
	//printf("%s\n", tablaPath);
	int cantidadDeParticiones = config_get_int_value(metadata, "PARTITIONS");
	//Separo el registro en timestamp, key y value
	char **infoSeparada = string_split(registro, ";");
	int timestamp = atoi(infoSeparada[0]);
	int key = atoi(infoSeparada[1]);
	char *value = string_new();
	string_append(&value, infoSeparada[2]);
	int particionQueContieneLaKey = key % cantidadDeParticiones;

	char *pathBinario = string_new();
	string_append(&pathBinario, tablaPath);
	string_append(&pathBinario, "/");
	string_append(&pathBinario, string_itoa(particionQueContieneLaKey));
	string_append(&pathBinario, ".bin");
	//binariosAfectados es una lista con los binarios a los que voy a tener que liberar los bloques y actualizar los registros
	list_add(*binariosAfectados, pathBinario);

	if (!dictionary_has_key(binariosParaCompactar, pathBinario)) {
		binarioCompactacion *unBinario = (binarioCompactacion*) malloc(
				sizeof(binarioCompactacion));
		unBinario->tablaALaQuePertenece = malloc(strlen(tablaPath) + 1);
		strcpy(unBinario->tablaALaQuePertenece, tablaPath);
		unBinario->registros = malloc(
				strlen(levantarRegistros(pathBinario)) + 1);
		strcpy(unBinario->registros, levantarRegistros(pathBinario));
		//printf("pathBinario--- %s\n", unBinario->registros);
		//uso el path del binario como clave para insertar en el diccionario
		dictionary_put(binariosParaCompactar, pathBinario, unBinario);
		//printf("%s\n", pathBinario);
	}
	binarioCompactacion *unBinario = dictionary_get(binariosParaCompactar,
			pathBinario);
	//printf("%s\n", unBinario->registros);
	//printf("%s\n", unBinario->tablaALaQuePertenece);
	//printf("%s\n", otro->registros);unBinario
	compararRegistros(timestamp, key, value, unBinario, pathBinario);
	//printf("%s\n", value);

	/*char *registrosBinario = string_new();

	 string_append(&registrosBinario, levantarRegistros(pathBinario));*/
}

void compararRegistros(int timestamp, int key, char *value,
		binarioCompactacion *unBinario, char* pathBinario) {
	char *registrosBinario = string_new();
	string_append(&registrosBinario, unBinario->registros);
	char **registrosSeparados = string_split(registrosBinario, "\n");
	free(registrosBinario);
	char *registrosActualizados = string_new();
	int i = 0;
	int existeLaKeyEnElBinario = 0;
	while (registrosSeparados[i] != NULL) {
		//printf("%s\n", registrosSeparados[i]);
		int elRegistroContieneLaKey = 0;
		comparar1RegistroBinarioCon1NuevoRegistro(timestamp, key, value,
				&(registrosSeparados[i]), &elRegistroContieneLaKey);
		if (elRegistroContieneLaKey) {
			existeLaKeyEnElBinario = 1;
		}
		string_append(&registrosActualizados, registrosSeparados[i]);
		string_append(&registrosActualizados, "\n");
		i++;
	}
	//printf("%i\n", existeLaKeyEnElBinario);
	//Si i = 0 quiere decir que el binario estaba vacio por lo que tengo que agregar solo este nuevo registro
	if (!existeLaKeyEnElBinario || i == 0) {
		char *nuevoRegistro = string_new();
		string_append(&nuevoRegistro, string_itoa(timestamp));
		string_append(&nuevoRegistro, ";");
		string_append(&nuevoRegistro, string_itoa(key));
		string_append(&nuevoRegistro, ";");
		string_append(&nuevoRegistro, value);
		string_append(&nuevoRegistro, "\n");
		string_append(&registrosActualizados, nuevoRegistro);
		free(nuevoRegistro);
	}
	//Saco los que tenia antes y asigno los nuevos registros
	free(unBinario->registros);
	unBinario->registros = malloc(strlen(registrosActualizados) + 1);
	strcpy(unBinario->registros, registrosActualizados);
	//printf("%s\n", unBinario->registros);
	dictionary_remove(binariosParaCompactar, pathBinario);
	dictionary_put(binariosParaCompactar, pathBinario, unBinario);

	/*binarioCompactacion *otro = dictionary_get(binariosParaCompactar, pathBinario);
	 printf("%s", otro->registros);*/
}

void comparar1RegistroBinarioCon1NuevoRegistro(int timestampRegistro,
		int keyRegistro, char *valueRegistro, char **registroBinario,
		int *elRegistroContieneLaKey) {
	char **infoSeparada = string_split(*registroBinario, ";");
	int timestampBinario = atoi(infoSeparada[0]);
	int keyBinario = atoi(infoSeparada[1]);
	char *valueBinario = string_new();
	string_append(&valueBinario, infoSeparada[2]);
	//printf("binario: %i - registro:%i\n",timestampBinario, timestampRegistro);
	//printf("%s\n", valueRegistro);
	if ((keyBinario == keyRegistro)) {
		*elRegistroContieneLaKey = 1;
		if (timestampBinario < timestampRegistro) {
			char *nuevoRegistro = string_new();
			string_append(&nuevoRegistro, string_itoa(timestampRegistro));
			string_append(&nuevoRegistro, ";");
			string_append(&nuevoRegistro, string_itoa(keyRegistro));
			string_append(&nuevoRegistro, ";");
			string_append(&nuevoRegistro, valueRegistro);
			//printf("binario < registro\n");
			//aca borro lo que tiene registroBinario y lo cambio a nuevoRegistro
			free(*registroBinario);
			*registroBinario = malloc(strlen(nuevoRegistro) + 1);
			strcpy(*registroBinario, nuevoRegistro);
		}
	}
}

//Dice tmpc pero tambien es para bin... cambiarlo
char *levantarRegistros(char *tmpc) {
	//FILE *archivoTmpc = fopen(tmpc, "r");
	t_config *configTmpc = config_create(tmpc);
	char **arrayDeBloques = config_get_array_value(configTmpc, "BLOCKS");
	int i = 0;
	char *registros = string_new();
	while (arrayDeBloques[i] != NULL) {
		char *bloque = string_new();
		char *stringAux = string_new();
		string_append(&bloque, structConfiguracionLFS.PUNTO_MONTAJE);
		string_append(&bloque, "Bloques/");
		string_append(&bloque, arrayDeBloques[i]);
		string_append(&bloque, ".bin");
		//printf("%s\n", bloque);
		levantarRegistroDe1Bloque(bloque, &stringAux);
		//printf("%s\n", stringAux); //\n

		string_append(&registros, stringAux);
		i++;
	}
	//printf("tmpc: %s, registros :%s\n",tmpc, registros);
	config_destroy(configTmpc);
	//Aca ya tendria los registros de los distintos bloques del tmp unidos
	return registros;
}

void levantarRegistroDe1Bloque(char *bloque, char **stringAux) {
	FILE * archivoBloque;
	char * buffer;
	//char *metadataPath = string_new();
	//string_append(&metadataPath, "/home/utnso/lissandra-checkpoint/Metadata/metadata.bin");
	char *metadataPath = string_from_format("%sMetadata/metadata.bin",
			structConfiguracionLFS.PUNTO_MONTAJE);
	t_config *metadata = config_create(metadataPath);
	int tamanioPorBloque = config_get_int_value(metadata, "BLOCK_SIZE");
	//printf("%i", tamanioPorBloque);
	config_destroy(metadata);
	free(metadataPath);
	archivoBloque = fopen(bloque, "rb");
	;
	//buffer = string_new();
	buffer = (char*) malloc(tamanioPorBloque);
	//Leo todos los registros del bloque que pueden ser como maximo el tamanio del bloque
	//fgets(buffer, tamanioPorBloque, archivoBloque);
	/*while(fgets(buffer, 128, archivoBloque)) {
	 string_append(stringAux, buffer);
	 }*/
	while (fgets(buffer, tamanioPorBloque, archivoBloque)) {
		string_append(stringAux, buffer);
	}
	//char *hola = (char*) malloc((sizeof(char) * longitudArchivo)+2);
	//printf("%s\n", *stringAux);
	//strcat(hola, "\0");
	//printf("%s", hola);
	//string_append(&buffer, "\0");
	//printf("bloque: %s---%s\n", bloque, buffer);
	//free(hola);
	//char *hola = (char *)malloc(strlen(buffer)+1);
	//strcpy(hola, buffer);
	//printf("%s", hola);
	//printf("%i", strlen(buffer));
	//free(hola);*/
	fclose(archivoBloque);
	free(buffer);
}

void tomarLosTmpc(char *tablaPath, t_queue *tmpcs) {
	DIR *directorio = opendir(tablaPath);
	struct dirent *archivoALeer;
	while ((archivoALeer = readdir(directorio)) != NULL) {
		if (string_ends_with(archivoALeer->d_name, ".tmpc")) {
			char *tmpc = string_new();
			string_append(&tmpc, tablaPath);
			string_append(&tmpc, "/");
			string_append(&tmpc, archivoALeer->d_name);
			queue_push(tmpcs, tmpc);
			//printf("--%s\n", tmpc);
			//("%s\n", queue_pop(tmpcs));
		}
	}
	closedir(directorio);
}

void renombrarTodosLosTMPATMPC(char* tablaPath) {
	DIR *directorio = opendir(tablaPath);
	struct dirent *archivoALeer;
	while ((archivoALeer = readdir(directorio)) != NULL) {
		if (string_ends_with(archivoALeer->d_name, ".tmp")) {
			char *viejoNombre = string_new();
			char *nuevoNombre = string_new();
			string_append(&viejoNombre, tablaPath);
			string_append(&viejoNombre, "/");
			string_append(&viejoNombre, archivoALeer->d_name);
			string_append(&nuevoNombre, viejoNombre);
			string_append(&nuevoNombre, "c");
			rename(viejoNombre, nuevoNombre);
			//printf("%s\n", nuevoNombre);
		}
	}
	closedir(directorio);
}

void create(char* tabla, char* consistencia, char* cantidadDeParticiones,
		char* tiempoDeCompactacion) {
	actualizarTiempoDeRetardo();
	sleep(structConfiguracionLFS.RETARDO);
	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* mensajeALogear = malloc(
				strlen("Error: ya existe una tabla con el nombre ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, "Error: ya existe una tabla con el nombre ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		//Si uso LOG_LEVEL_ERROR no lo imprime ni lo escribe
		g_logger = log_create(
				string_from_format("%serroresCreate.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_INFO);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	} else {
		char* path = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		char* metadataPath = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/metadata") + 1);
		strcpy(path,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
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
		levantarHiloCompactacion(path);
		free(path);
	}
}

void crearBinarios(char* path, int cantidadDeParticiones) {
	for (int i = 0; i < cantidadDeParticiones; i++) {
		//10 para dejar cierto margen a la cantidad de digitos de las particiones
		char* directorioBinario = malloc(strlen(path) + 10);
		char* numeroDeParticion = string_itoa(i);
		strcpy(directorioBinario, path);
		strcat(directorioBinario, "/");
		strcat(directorioBinario, numeroDeParticion);
		strcat(directorioBinario, ".bin");
		int numeroDeBloque = asignarBloque();
		crearArchivoDeBloquesVacio(directorioBinario, numeroDeBloque);
		free(directorioBinario);
		free(numeroDeParticion);
	}
}

int asignarBloque() {

	int encontroUnBloque = 0;
	int bloqueEncontrado = 0;
	//Reemplazar para que vaya hasta la cantidad de bloques del archivo de config
	for (int i = 0; i < bitarray_get_max_bit(bitarrayBloques); i++) {
		if (bitarray_test_bit(bitarrayBloques, i) == 0) {
			bitarray_set_bit(bitarrayBloques, i);
			encontroUnBloque = 1;
			bloqueEncontrado = i;
			break;
		}
	}

	if (encontroUnBloque) {
		return bloqueEncontrado;
	}

	printf("No se encontro bloque disponible\n");
	exit(-1);
}

void crearArchivoDeBloquesVacio(char* directorioBinario, int bloqueEncontrado) {
	FILE *archivoBinario = fopen(directorioBinario, "w");
	t_config *binario = config_create(directorioBinario);
	char *stringdelArrayDeBloques = string_new();
	string_append(&stringdelArrayDeBloques, "[");
	string_append(&stringdelArrayDeBloques, string_itoa(bloqueEncontrado));
	string_append(&stringdelArrayDeBloques, "]");
	//64 lo tengo que reemplazar por el tamanio de un bloque supongo
	config_set_value(binario, "SIZE", "0");
	//los bloques despues se levantan con config_get_array_value
	config_set_value(binario, "BLOCKS", stringdelArrayDeBloques);
	config_save_in_file(binario, directorioBinario);
	//actualizarBitArray();
	//creo el archivo .bin del bloque
	char* pathBloque = string_new();
	string_append(&pathBloque,
			string_from_format("%sBloques/",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&pathBloque, string_itoa(bloqueEncontrado));
	string_append(&pathBloque, ".bin");
	FILE *bloqueCreado = fopen(pathBloque, "w");
	/*if(registrosAEscribir!=NULL){
	 fwrite(registrosAEscribir, sizeof(char), strlen(registrosAEscribir), pathBloque);
	 }*/
	fclose(bloqueCreado);
	fclose(archivoBinario);
	config_destroy(binario);
}

void verBitArray() {
	printf("%i -- ", bitarray_get_max_bit(bitarrayBloques));
	for (int j = 0; j < bitarray_get_max_bit(bitarrayBloques); j++) {

		bool bit = bitarray_test_bit(bitarrayBloques, j);
		printf("%i", bit);
	}
	printf("\n");
}

//la cantidad de bloques dividido por 8 bits = cantidad de bytes necesarios
//¿Cuantos bloques genero con 649 bytes? como cada bloque es un bit, 649 bytes * 8 bits = 5192 bloques
int tamanioEnBytesDelBitarray() {
	char *metadataPath = string_from_format("%sMetadata/metadata.bin",
			structConfiguracionLFS.PUNTO_MONTAJE);
	t_config *metadata = config_create(metadataPath);
	//int tamanioPorBloque = config_get_int_value(metadata, "BLOCK_SIZE");
	int cantidadDeBloques = config_get_int_value(metadata, "BLOCKS");
	config_destroy(metadata);
	//¿Que pasa si la cantidad de bloques no es divisible por 8?
	return cantidadDeBloques / 8;
}

//Esto es para testear
void crearMetadataBloques() {
	char *metadataPath = string_new();
	string_append(&metadataPath,
			string_from_format("%sMetadata/metadata.bin",
					structConfiguracionLFS.PUNTO_MONTAJE));
	FILE *archivoMetadata = fopen(metadataPath, "w");
	t_config *metadata = config_create(metadataPath);
	//Estos datos harcodeados despues tienen que modificarse. ¿Tienen que tener algun valor especial por defecto?
	config_set_value(metadata, "BLOCK_SIZE", "128");
	//4096
	config_set_value(metadata, "BLOCKS", "512");
	config_set_value(metadata, "MAGIC_NUMBER", "LISSANDRA");
	config_save_in_file(metadata, metadataPath);
	fclose(archivoMetadata);
	config_destroy(metadata);
}

void crearArchivoBitmap() {
	char *pathBitmap = string_new();
	string_append(&pathBitmap, structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathBitmap, "Metadata/bitmap.bin");
	FILE *f = fopen(pathBitmap, "w");

	//5192 sale de mystat.st_size (tamanio del archivo de bitmap). tengo que escribir en el archivo para mapear la memoria.
	for (int i = 0; i < 5192; i++) {
		fputc(0, f);
	}

	fclose(f);
}

void iniciarMmap() {
	char *pathBitmap = string_new();
	string_append(&pathBitmap, structConfiguracionLFS.PUNTO_MONTAJE);
	string_append(&pathBitmap, "Metadata/bitmap.bin");
	//Open es igual a fopen pero el FD me lo devuelve como un int (que es lo que necesito para fstat)
	int bitmap = open(pathBitmap, O_RDWR);
	struct stat mystat;

	//fstat rellena un struct con informacion del FD dado
	if (fstat(bitmap, &mystat) < 0) {
		close(bitmap);
	}

	/*	mmap mapea un archivo en memoria y devuelve la direccion de memoria donde esta ese mapeo
	 *  MAP_SHARED Comparte este área con todos los otros  objetos  que  señalan  a  este  objeto.
	 Almacenar  en  la  región  es equivalente a escribir en el fichero.
	 */
	mmapDeBitmap = mmap(NULL, mystat.st_size, PROT_WRITE | PROT_READ,
	MAP_SHARED, bitmap, 0);
	close(bitmap);

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

//Las funciones de abajo repiten logica, si hay tiempo hacer una funcion sola
int existeLaTabla(char* nombreDeTabla) {
	DIR *directorio = opendir(
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABLAS existe alguna que tenga el mismo nombre
		if ((directorioALeer->d_type) == DT_DIR
				&& !strcmp((directorioALeer->d_name), nombreDeTabla)) {
			closedir(directorio);
			return 1;
		}
	}
	closedir(directorio);
	return 0;
}

void drop(char* tabla) {
	actualizarTiempoDeRetardo();
	sleep(structConfiguracionLFS.RETARDO);
	char *path = string_new();
	string_append(&path,
			string_from_format("%sTables/",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&path, tabla);
	DIR *directorio = opendir(path);
	//printf("%s\n\n", path);
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//printf("- %s\n", directorioALeer->d_name);
		//Elimino todito lo que tiene adentro la tabla
		if (!((directorioALeer->d_type) == DT_DIR)) {
			char *archivoABorrar = string_new();
			string_append(&archivoABorrar, path);
			string_append(&archivoABorrar, "/");
			string_append(&archivoABorrar, directorioALeer->d_name);
			//printf("!!%s\n", archivoABorrar);
			remove(archivoABorrar);
		}
	}
	rmdir(path);
	closedir(directorio);
}

int existeCarpeta(char *nombreCarpeta) {
	DIR *directorio = opendir(structConfiguracionLFS.PUNTO_MONTAJE);
	if (directorio == NULL) {
		mkdir(structConfiguracionLFS.PUNTO_MONTAJE, 0777);
		return 0;
	}
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Evaluo si de todas las carpetas dentro de TABAS existe alguna que tenga el mismo nombre
		if ((directorioALeer->d_type) == DT_DIR
				&& !strcmp((directorioALeer->d_name), nombreCarpeta)) {
			closedir(directorio);
			return 1;
		}
	}
	closedir(directorio);
	return 0;
}

//No le pongo "select" porque ya esta la funcion de socket y rompe
char* realizarSelect(char* tabla, char* key) {
	//actualizarTiempoDeRetardo();
	//sleep(structConfiguracionLFS.RETARDO);

	string_to_upper(tabla);
	if (existeLaTabla(tabla)) {
		char* pathMetadata = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/metadata") + 1);
		strcpy(pathMetadata,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathMetadata, tabla);
		strcat(pathMetadata, "/metadata");
		t_config *metadata = config_create(pathMetadata);
		int cantidadDeParticiones = config_get_int_value(metadata,
				"PARTITIONS");

		int particionQueContieneLaKey = (atoi(key)) % cantidadDeParticiones;
		printf("La key esta en la particion %i\n", particionQueContieneLaKey);
		char* stringParticion = malloc(4);
		stringParticion = string_itoa(particionQueContieneLaKey);

		char* pathParticionQueContieneKey = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + strlen("/") + strlen(stringParticion)
						+ strlen(".bin") + 1);
		strcpy(pathParticionQueContieneKey,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathParticionQueContieneKey, tabla);
		strcat(pathParticionQueContieneKey, "/");
		strcat(pathParticionQueContieneKey, stringParticion);
		strcat(pathParticionQueContieneKey, ".bin");
		t_config *tamanioYBloques = config_create(pathParticionQueContieneKey);
		char** vectorBloques = config_get_array_value(tamanioYBloques, "BLOCKS"); //devuelve vector de STRINGS

		int m = 0;
		while (vectorBloques[m] != NULL) {
			m++;
		}

		int timestampActualMayorBloques = -1;
		char* valueDeTimestampActualMayorBloques = string_new();

		// POR CADA BLOQUE, TENGO QUE ENTRAR A ESTE BLOQUE
		for (int i = 0; i < m; i++) {
			char* pathBloque = malloc(
					strlen(
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE))
							+ strlen((vectorBloques[i])) + strlen(".bin") + 1);
			strcpy(pathBloque,
					string_from_format("%sBloques/",
							structConfiguracionLFS.PUNTO_MONTAJE));
			strcat(pathBloque, vectorBloques[i]);
			strcat(pathBloque, ".bin");
			FILE *archivoBloque = fopen(pathBloque, "r");
			if (archivoBloque == NULL) {
				printf("no se pudo abrir archivo de bloques\n");
				exit(1);
			}

			int cantidadIgualDeKeysEnBloque = 0;

			char* bloqueAnterior;
			if (i == 0) {
				bloqueAnterior = NULL;
			} else {
				bloqueAnterior = vectorBloques[i - 1];
			}

			char* bloqueSiguiente;
			// si es el ultimo del vector de bloques, el bloqueSiguiente es NULL
			if ((i + 1) == m) {
				bloqueSiguiente = NULL;
			} else {
				bloqueSiguiente = vectorBloques[i + 1];
			}

			t_registro* vectorStructs[100];
			obtenerDatosParaKeyDeseada(archivoBloque, (atoi(key)),
					vectorStructs, &cantidadIgualDeKeysEnBloque,
					bloqueSiguiente, bloqueAnterior);

			//printf("%i", vectorStructs[0]->timestamp);
			//printf("%i", vectorStructs[1]->timestamp);

			//cual de estos tiene el timestamp mas grande? guardar timestamp y value
			int temp = 0;
			char* valor;
			for (int k = 1; k < cantidadIgualDeKeysEnBloque; k++) {
				for (int j = 0; j < (cantidadIgualDeKeysEnBloque - k); j++) {
					if (vectorStructs[j]->timestamp
							< vectorStructs[j + 1]->timestamp) {
						temp = vectorStructs[j + 1]->timestamp;
						valor = malloc(strlen(vectorStructs[j + 1]->value));
						strcpy(valor, vectorStructs[j + 1]->value);

						vectorStructs[j + 1]->timestamp =
								vectorStructs[j]->timestamp;
						vectorStructs[j + 1]->value = vectorStructs[j]->value;

						vectorStructs[j]->timestamp = temp;
						vectorStructs[j]->value = valor;
					}
				}
			} // aca quedaria el vector ordenado por timestamp mayor
			if (vectorStructs[0]->timestamp > timestampActualMayorBloques) {
					timestampActualMayorBloques = vectorStructs[0]->timestamp;
					strcpy(valueDeTimestampActualMayorBloques, "");
					string_append(&valueDeTimestampActualMayorBloques,
							vectorStructs[0]->value);
			}
			fclose(archivoBloque);
			free(pathBloque);
			//free(vectorStructs);
		} 	//cierra el for

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorBloques
		// y guarda el valor en valueDeTimestampActualMayorBloques
		// si no hay ninguno en vectorBloques (porque por ej, esta en los temporales)
		// entonces timestampActualMayorBloques = -1 y
		// valueDeTimestampActualMayorBloques = NULL

		//-------------------------------------------------

		// AHORA ABRO ARCHIVOS TEMPORALES. EL PROCEDIMIENTO ES MUY PARECIDO AL ANTERIOR
		char* pathTemporales = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		strcpy(pathTemporales,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathTemporales, tabla);

		DIR *directorioTemporal = opendir(pathTemporales);
		struct dirent *archivoALeer;

		int timestampActualMayorTemporales = -1;
		char* valueDeTimestampActualMayorTemporales = string_new();

		while ((archivoALeer = readdir(directorioTemporal)) != NULL) { //PARA CADA ARCHIVO DE LA TABLA ESPECIFICA
			if (string_ends_with(archivoALeer->d_name, ".tmp")) {

				//obtengo el nombre de ese archivo .tmp . Ejemplo obtengo A1.tmp siendo A1 el nombre (tipo char*)
				char* nombreArchivoTemporal = string_split(archivoALeer->d_name,
						".")[0];
				// ahora ya tengo el nombre del archivo .tmp

				char* pathTemporal = malloc(
						strlen(
								string_from_format("%sTables/",
										structConfiguracionLFS.PUNTO_MONTAJE))
								+ strlen(tabla) + strlen("/")
								+ strlen(nombreArchivoTemporal) + strlen(".tmp")
								+ 1);
				strcpy(pathTemporal,
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE));
				strcat(pathTemporal, tabla);
				strcat(pathTemporal, "/");
				strcat(pathTemporal, nombreArchivoTemporal);
				strcat(pathTemporal, ".tmp");
				FILE *fileTemporal = fopen(pathTemporal, "r");
				if (fileTemporal == NULL) {
					printf("no se pudo abrir archivo de temporales\n");
					exit(1);
				}

				t_config *tamanioYBloquesTmp = config_create(pathTemporal);
				char** vectorBloquesTmp = config_get_array_value(
						tamanioYBloquesTmp, "BLOCKS"); //devuelve vector de STRINGS

				int n = 0;
				while (vectorBloquesTmp[n] != NULL) {
					n++;
				}

				//POR CADA BLOQUE, TENGO QUE ENTRAR A ESE BLOQUE
				for (int q = 0; q < n; q++) {
					char* pathBloqueTmp =
							malloc(
									strlen(
											string_from_format("%sBloques/",
													structConfiguracionLFS.PUNTO_MONTAJE))
											+ strlen((vectorBloquesTmp[q]))
											+ strlen(".bin") + 1);
					strcpy(pathBloqueTmp,
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE));
					strcat(pathBloqueTmp, vectorBloquesTmp[q]);
					strcat(pathBloqueTmp, ".bin");
					FILE *archivoBloqueTmp = fopen(pathBloqueTmp, "r");
					if (archivoBloqueTmp == NULL) {
						printf("no se pudo abrir archivo de bloques\n");
					}

					int cantidadIgualDeKeysEnTemporal = 0;
					t_registro* vectorStructsTemporal[100];

					char * bloqueAnterior;
					if (q == 0) {
						bloqueAnterior = NULL;
					} else {
						bloqueAnterior = vectorBloquesTmp[q - 1];
					}

					char* bloqueSiguiente;
					// si es el ultimo del vector de bloques, el bloqueSiguiente es NULL
					if ((q + 1) == n) {
						bloqueSiguiente = NULL;
					} else {
						bloqueSiguiente = vectorBloquesTmp[q + 1];
					}

					obtenerDatosParaKeyDeseada(archivoBloqueTmp, (atoi(key)),
							vectorStructsTemporal,
							&cantidadIgualDeKeysEnTemporal, bloqueSiguiente,
							bloqueAnterior);

					//cual de estos tiene el timestamp mas grande? guardar timestamp y value
					int tempo = 0;
					char* valorTemp;
					for (int k = 1; k < cantidadIgualDeKeysEnTemporal; k++) {
						for (int j = 0; j < (cantidadIgualDeKeysEnTemporal - k);
								j++) {
							if (vectorStructsTemporal[j]->timestamp
									< vectorStructsTemporal[j + 1]->timestamp) {
								tempo = vectorStructsTemporal[j + 1]->timestamp;
								valorTemp =
										malloc(
												strlen(
														vectorStructsTemporal[j
																+ 1]->value));
								strcpy(valorTemp,
										vectorStructsTemporal[j + 1]->value);

								vectorStructsTemporal[j + 1]->timestamp =
										vectorStructsTemporal[j]->timestamp;
								vectorStructsTemporal[j + 1]->value =
										vectorStructsTemporal[j]->value;

								vectorStructsTemporal[j]->timestamp = tempo;
								vectorStructsTemporal[j]->value = valorTemp;
							}
						}
					}

					if (vectorStructsTemporal[0]->timestamp
							> timestampActualMayorTemporales) {
						timestampActualMayorTemporales =
								vectorStructsTemporal[0]->timestamp;
						strcpy(valueDeTimestampActualMayorTemporales, "");
						string_append(&valueDeTimestampActualMayorTemporales,
								vectorStructsTemporal[0]->value);
					}
					fclose(archivoBloqueTmp);
					free(pathBloqueTmp);
					//free(vectorStructsTemporal);
				} // cierra el for
			} // cierra el if
		} //cierra el while

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorTemporales
		// y guarda el valor en valueDeTimestampActualMayorTemporales
		// si no hay ninguno en vectorStructsTemporal
		// entonces timestampActualMayorTemporales = -1 y
		// valueDeTimestampActualMayorTemporales = NULL

		closedir(directorioTemporal);

		//-------------------------------------------------

		// AHORA ABRO ARCHIVOS TEMPC DE COMPACTACION. EL PROCEDIMIENTO ES MUY PARECIDO AL ANTERIOR
		char* pathTemporalesC = malloc(
				strlen(
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(tabla) + 1);
		strcpy(pathTemporalesC,
				string_from_format("%sTables/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathTemporalesC, tabla);

		DIR *directorioTemporalC = opendir(pathTemporalesC);
		struct dirent *archivoCALeer;

		int timestampActualMayorTemporalesC = -1;
		char* valueDeTimestampActualMayorTemporalesC = string_new();

		while ((archivoCALeer = readdir(directorioTemporalC)) != NULL) { //PARA CADA ARCHIVO DE LA TABLA ESPECIFICA
			if (string_ends_with(archivoCALeer->d_name, ".tmpc")) {

				//obtengo el nombre de ese archivo .tmpc . Ejemplo obtengo A1.tmp siendo A1 el nombre (tipo char*)
				char* nombreArchivoTemporalC = string_split(
						archivoCALeer->d_name, ".")[0];
				// ahora ya tengo el nombre del archivo .tmpc

				char* pathTemporalC = malloc(
						strlen(
								string_from_format("%sTables/",
										structConfiguracionLFS.PUNTO_MONTAJE))
								+ strlen(tabla) + strlen("/")
								+ strlen(nombreArchivoTemporalC)
								+ strlen(".tmpc") + 1);
				strcpy(pathTemporalC,
						string_from_format("%sTables/",
								structConfiguracionLFS.PUNTO_MONTAJE));
				strcat(pathTemporalC, tabla);
				strcat(pathTemporalC, "/");
				strcat(pathTemporalC, nombreArchivoTemporalC);
				strcat(pathTemporalC, ".tmpc");
				FILE *fileTemporalC = fopen(pathTemporalC, "r");
				if (fileTemporalC == NULL) {
					printf("no se pudo abrir archivo de temporales\n");
					exit(1);
				}

				t_config *tamanioYBloquesTmpC = config_create(pathTemporalC);
				char** vectorBloquesTmpC = config_get_array_value(
						tamanioYBloquesTmpC, "BLOCKS"); //devuelve vector de STRINGS

				int n = 0;
				while (vectorBloquesTmpC[n] != NULL) {
					n++;
				}

				//POR CADA BLOQUE, TENGO QUE ENTRAR A ESE BLOQUE
				for (int q = 0; q < n; q++) {
					char* pathBloqueTmpC =
							malloc(
									strlen(
											string_from_format("%sBloques/",
													structConfiguracionLFS.PUNTO_MONTAJE))
											+ strlen((vectorBloquesTmpC[q]))
											+ strlen(".bin") + 1);
					strcpy(pathBloqueTmpC,
							string_from_format("%sBloques/",
									structConfiguracionLFS.PUNTO_MONTAJE));
					strcat(pathBloqueTmpC, vectorBloquesTmpC[q]);
					strcat(pathBloqueTmpC, ".bin");
					FILE *archivoBloqueTmpC = fopen(pathBloqueTmpC, "r");
					if (archivoBloqueTmpC == NULL) {
						printf("no se pudo abrir archivo de bloques\n");
					}

					int cantidadIgualDeKeysEnTemporal = 0;

					char * bloqueAnterior;
					if (q == 0) {
						bloqueAnterior = NULL;
					} else {
						bloqueAnterior = vectorBloquesTmpC[q - 1];
					}

					char* bloqueSiguiente;
					// si es el ultimo del vector de bloques, el bloqueSiguiente es NULL
					if ((q + 1) == n) {
						bloqueSiguiente = NULL;
					} else {
						bloqueSiguiente = vectorBloquesTmpC[q + 1];
					}

					t_registro* vectorStructsTemporalC[100];
					obtenerDatosParaKeyDeseada(archivoBloqueTmpC, (atoi(key)),
							vectorStructsTemporalC,
							&cantidadIgualDeKeysEnTemporal, bloqueSiguiente,
							bloqueAnterior);

					//cual de estos tiene el timestamp mas grande? guardar timestamp y value
					int tempo = 0;
					char* valorTempC;
					for (int k = 1; k < cantidadIgualDeKeysEnTemporal; k++) {
						for (int j = 0; j < (cantidadIgualDeKeysEnTemporal - k);
								j++) {
							if (vectorStructsTemporalC[j]->timestamp
									< vectorStructsTemporalC[j + 1]->timestamp) {
								tempo =
										vectorStructsTemporalC[j + 1]->timestamp;
								valorTempC =
										malloc(
												strlen(
														vectorStructsTemporalC[j
																+ 1]->value));
								strcpy(valorTempC,
										vectorStructsTemporalC[j + 1]->value);

								vectorStructsTemporalC[j + 1]->timestamp =
										vectorStructsTemporalC[j]->timestamp;
								vectorStructsTemporalC[j + 1]->value =
										vectorStructsTemporalC[j]->value;

								vectorStructsTemporalC[j]->timestamp = tempo;
								vectorStructsTemporalC[j]->value = valorTempC;
							}
						}
					}

					if (vectorStructsTemporalC[0]->timestamp
							> timestampActualMayorTemporalesC) {
						timestampActualMayorTemporalesC =
								vectorStructsTemporalC[0]->timestamp;
						strcpy(valueDeTimestampActualMayorTemporalesC, "");
						string_append(&valueDeTimestampActualMayorTemporalesC,
								vectorStructsTemporalC[0]->value);
					}
					fclose(archivoBloqueTmpC);
					free(pathBloqueTmpC);
					//free(vectorStructsTemporalC);
				} // cierra el for
			} // cierra el if
		} //cierra el while

		// si encontro alguno, me guarda el timestamp mayor en timestampActualMayorTemporalesC
		// y guarda el valor en valueDeTimestampActualMayorTemporalesC
		// si no hay ninguno en vectorStructsTemporalC
		// entonces timestampActualMayorTemporalesC = -1 y
		// valueDeTimestampActualMayorTemporalesC = NULL

		closedir(directorioTemporal);

		// ----------------------------------------------------

		// LEO MEMTABLE

		t_list* listaRegistros = dictionary_get(memtable, tabla);

		/*
		char* mensajeALogear = malloc( 50 +  strlen(entradaTabla[0]->timestamp));
		strcpy(mensajeALogear, "MEMTABLE : ");
		strcat(mensajeALogear, &entradaTabla[0]->timestamp);
		t_log* g_logger;
		g_logger = log_create(
				string_from_format("%sbloqueoEntreCompactacion.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,
				LOG_LEVEL_INFO);
		log_info(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
		*/

		int cantIgualDeKeyEnMemtable = 0;
		//creo nuevo array que va a tener solo los structs de la key que me pasaron por parametro
		t_registro* arrayPorKeyDeseadaMemtable[100];

		crearArrayPorKeyMemtable(arrayPorKeyDeseadaMemtable, listaRegistros, atoi(key), &cantIgualDeKeyEnMemtable, tabla);

		int t = 0;
		char* unValor;
		for (int k = 1; k < cantIgualDeKeyEnMemtable; k++) {
			for (int j = 0; j < (cantIgualDeKeyEnMemtable - k); j++) {
				if (arrayPorKeyDeseadaMemtable[j]->timestamp
						< arrayPorKeyDeseadaMemtable[j + 1]->timestamp) {
					t = arrayPorKeyDeseadaMemtable[j + 1]->timestamp;
					unValor = malloc(
							strlen(arrayPorKeyDeseadaMemtable[j + 1]->value));
					strcpy(unValor, arrayPorKeyDeseadaMemtable[j + 1]->value);

					arrayPorKeyDeseadaMemtable[j + 1]->timestamp =
							arrayPorKeyDeseadaMemtable[j]->timestamp;
					arrayPorKeyDeseadaMemtable[j + 1]->value =
							arrayPorKeyDeseadaMemtable[j]->value;

					arrayPorKeyDeseadaMemtable[j]->timestamp = t;
					arrayPorKeyDeseadaMemtable[j]->value = unValor;
				}
			}
		} // aca quedaria el array arrayPorKeyDeseadaMemtable ordenado por timestamp mayor

		int timestampMayorMemtable;
		if (cantIgualDeKeyEnMemtable == 0) {
			timestampMayorMemtable = -1;
		} else {
			timestampMayorMemtable = arrayPorKeyDeseadaMemtable[0]->timestamp;
		}

		//-----------------------------------------------------
		char *valueFinal = string_new();

		// si no existe la key, error
		if ((timestampActualMayorBloques == -1)
				&& (timestampActualMayorTemporales == -1)
				&& (timestampMayorMemtable == -1)
				&& (timestampActualMayorTemporalesC == -1)) {
			char* mensajeALogear = malloc(
					strlen(" No existe la key numero : ") + strlen(key)
							+ 1);
			strcpy(mensajeALogear, " No existe la key numero : ");
			strcat(mensajeALogear, key);
			t_log* g_logger;
			g_logger = log_create(
					string_from_format("%serroresSelect.log",
							structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
					LOG_LEVEL_ERROR);
			log_error(g_logger, mensajeALogear);
			log_destroy(g_logger);
			free(mensajeALogear);

			return NULL;
		} else { // o sea, si existe la key en algun lugar

			// si bloques tiene mayor timestamp que todos
			if ((timestampActualMayorBloques >= timestampActualMayorTemporales)
					&& (timestampActualMayorBloques
							>= timestampActualMayorTemporalesC)
					&& (timestampActualMayorBloques >= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorBloques);
				string_append(&valueFinal, valueDeTimestampActualMayorBloques);

				char* mensajeALogear = malloc( 100 + strlen(tabla) + strlen(string_itoa(timestampActualMayorBloques)) + strlen(valueDeTimestampActualMayorBloques) + 1);
				strcpy(mensajeALogear, " Se selecciono tabla : ");
				strcat(mensajeALogear, tabla);
				strcat(mensajeALogear, " / Mayor timestamp en BLOQUE : ");
				strcat(mensajeALogear, string_itoa(timestampActualMayorBloques));
				strcat(mensajeALogear, " / Value : ");
				strcat(mensajeALogear, valueDeTimestampActualMayorBloques);
				t_log* g_logger;
				g_logger = log_create( string_from_format("%sselect.log",structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0, LOG_LEVEL_INFO);
				log_info(g_logger, mensajeALogear);
				log_destroy(g_logger);
				free(mensajeALogear);
			}

			// si tmp tiene mayor timestamp que todos
			if ((timestampActualMayorTemporales >= timestampActualMayorBloques)
					&& (timestampActualMayorTemporales
							>= timestampActualMayorTemporalesC)
					&& (timestampActualMayorTemporales >= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorTemporales);
				string_append(&valueFinal,
						valueDeTimestampActualMayorTemporales);


				char* mensajeALogear = malloc( 120 + strlen(tabla) + strlen(string_itoa(timestampActualMayorTemporales)) + strlen(valueDeTimestampActualMayorTemporales) + 1);
				strcpy(mensajeALogear, " Se selecciono tabla : ");
				strcat(mensajeALogear, tabla);
				strcat(mensajeALogear, " / Mayor timestamp en TMP : ");
				strcat(mensajeALogear,
						string_itoa(timestampActualMayorTemporales));
				strcat(mensajeALogear, " / Value : ");
				strcat(mensajeALogear, valueDeTimestampActualMayorTemporales);
				t_log* g_logger;
				g_logger = log_create(
						string_from_format("%sselect.log",
								structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,
						LOG_LEVEL_INFO);
				log_info(g_logger, mensajeALogear);
				log_destroy(g_logger);
				free(mensajeALogear);
			}

			// si tmpc tiene mayor timestamp que todos
			if ((timestampActualMayorTemporalesC
					>= timestampActualMayorTemporales)
					&& (timestampActualMayorTemporalesC
							>= timestampActualMayorBloques)
					&& (timestampActualMayorTemporalesC
							>= timestampMayorMemtable)) {
				printf("%s\n", valueDeTimestampActualMayorTemporalesC);
				string_append(&valueFinal,
						valueDeTimestampActualMayorTemporalesC);

				char* mensajeALogear = malloc( 120 + strlen(tabla) + strlen(string_itoa(timestampActualMayorTemporalesC)) + strlen(valueDeTimestampActualMayorTemporalesC) + 1);
				strcpy(mensajeALogear, " Se selecciono tabla : ");
				strcat(mensajeALogear, tabla);
				strcat(mensajeALogear, " / Mayor timestamp en TMPC : ");
				strcat(mensajeALogear,
						string_itoa(timestampActualMayorTemporalesC));
				strcat(mensajeALogear, " / Value : ");
				strcat(mensajeALogear, valueDeTimestampActualMayorTemporalesC);
				t_log* g_logger;
				g_logger = log_create(
						string_from_format("%sselect.log",
								structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,
						LOG_LEVEL_INFO);
				log_info(g_logger, mensajeALogear);
				log_destroy(g_logger);
				free(mensajeALogear);
			}

			// si memtable tiene mayor timestamp que todos
			if ((timestampMayorMemtable >= timestampActualMayorBloques)
					&& (timestampMayorMemtable >= timestampActualMayorTemporales)
					&& (timestampMayorMemtable
							>= timestampActualMayorTemporalesC)) {
				printf("%s\n", arrayPorKeyDeseadaMemtable[0]->value);
				string_append(&valueFinal,
						arrayPorKeyDeseadaMemtable[0]->value);

				char* mensajeALogear = malloc( 120 + strlen(tabla) + strlen(string_itoa(timestampMayorMemtable)) + strlen(arrayPorKeyDeseadaMemtable[0]->value) + 1);
				strcpy(mensajeALogear, " Se selecciono tabla : ");
				strcat(mensajeALogear, tabla);
				strcat(mensajeALogear, " / Mayor timestamp en MEMTABLE : ");
				strcat(mensajeALogear,
						string_itoa(timestampMayorMemtable));
				strcat(mensajeALogear, " / Value : ");
				strcat(mensajeALogear, arrayPorKeyDeseadaMemtable[0]->value);
				t_log* g_logger;
				g_logger = log_create(
						string_from_format("%sselect.log",
								structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 0,
						LOG_LEVEL_INFO);
				log_info(g_logger, mensajeALogear);
				log_destroy(g_logger);
				free(mensajeALogear);
			}

			return valueFinal;
			string_append(&valueFinal, "");
		}

		free(pathMetadata);
		free(pathParticionQueContieneKey);
		free(pathTemporales);
		free(pathTemporalesC);
		//free(pathTemporal);
		config_destroy(tamanioYBloques);
		config_destroy(metadata);

		// SI NO ENCUENTRA LA TABLA (lo de abajo)
	} else {
		char* mensajeALogear = malloc(
				strlen(" No existe una tabla con el nombre : ")
						+ strlen(tabla) + 1);
		strcpy(mensajeALogear, " No existe una tabla con el nombre : ");
		strcat(mensajeALogear, tabla);
		t_log* g_logger;
		g_logger = log_create(
				string_from_format("%serroresSelect.log",
						structConfiguracionLFS.PUNTO_MONTAJE), "LFS", 1,
				LOG_LEVEL_ERROR);
		log_error(g_logger, mensajeALogear);
		log_destroy(g_logger);
		free(mensajeALogear);
	}
	return NULL;
}

void obtenerDatosParaKeyDeseada(FILE *fp, int key, t_registro** vectorStructs,
		int *cant, char* charProximoBloque, char* charAnteriorBloque) {
	int i = 0;
	char * line = NULL;
	char * line2 = NULL;
	size_t len = 0;
	ssize_t read;
	FILE *anteriorBloque = NULL;
	FILE *proximoBloque = NULL;

	if (charAnteriorBloque == NULL) {
			//printf("no existe el bloque anterior \n");
	}
	else{
		char* pathBloque = malloc(
				strlen(
						string_from_format("%sBloques/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(charAnteriorBloque) + strlen(".bin") + 1);
		strcpy(pathBloque,
				string_from_format("%sBloques/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathBloque, charAnteriorBloque);
		strcat(pathBloque, ".bin");
		anteriorBloque = fopen(pathBloque, "r");
		free(pathBloque);
	}

	if (charProximoBloque) {
		char* pathBloque2 = malloc(
				strlen(
						string_from_format("%sBloques/",
								structConfiguracionLFS.PUNTO_MONTAJE))
						+ strlen(charProximoBloque) + strlen(".bin") + 1);
		strcpy(pathBloque2,
				string_from_format("%sBloques/",
						structConfiguracionLFS.PUNTO_MONTAJE));
		strcat(pathBloque2, charProximoBloque);
		strcat(pathBloque2, ".bin");
		proximoBloque = fopen(pathBloque2, "r");

		free(pathBloque2);
	}

	if (proximoBloque == NULL) {
		//printf("no existe el prox bloque \n");
	}

	// si NO es el primer bloque del array de BLOCK
	if (anteriorBloque != NULL) {
		// si el anterior bloque no termina con \n => anterior tiene renglon incompleto => yo tengo el primer renglon al pedo
		char* ultimoCaracter = malloc(1);
		fseek(anteriorBloque, -1, SEEK_END);
		fread(ultimoCaracter, 1, 1, anteriorBloque);
		if (strncmp(ultimoCaracter, "\n", 1)) {
			// descarto primer renglon y sigo con el sgte
			getline(&line, &len, fp);
		}
	}
	while ((read = getline(&line, &len, fp)) != -1) {
		FILE* fpCopia = fdopen(dup(fileno(fp)), "r");
		if (proximoBloque != NULL) {
			// si( esta linea es la ultima y no termina con \n (es decir que ademas esta incompleta) )
			size_t len2 = len;
			if ((getline(&line2, &len2, fpCopia) == -1)
					&& (line[strlen(line) - 1] != '\n')) {
				char * lineProxBloque = NULL;
				size_t lenProxBloque = 0;
				getline(&lineProxBloque, &lenProxBloque, proximoBloque);
				//concateno la linea que tengo con la primera linea del proximo bloque
				strcat(line, lineProxBloque);
			}
		}
		int keyLeida = atoi(string_split(line, ";")[1]);
		if (keyLeida == key) {
			t_registro* p_registro = malloc(12); // 2 int = 2* 4        +       un puntero a char = 4
			t_registro p_registro2;
			p_registro = &p_registro2;
			char** arrayLinea = malloc(strlen(line) + 1);
			arrayLinea = string_split(line, ";");
			int timestamp = atoi(arrayLinea[0]);
			int key = atoi(arrayLinea[1]);
			p_registro->timestamp = timestamp;
			p_registro->key = key;
			p_registro->value = malloc(strlen(arrayLinea[2]));
			strcpy(p_registro->value, arrayLinea[2]);
			vectorStructs[i] = malloc(12);
			memcpy(&vectorStructs[i]->key, &p_registro->key,
					sizeof(p_registro->key));
			memcpy(&vectorStructs[i]->timestamp, &p_registro->timestamp,
					sizeof(p_registro->timestamp));
			vectorStructs[i]->value = malloc(strlen(p_registro->value));
			memcpy(vectorStructs[i]->value, p_registro->value,
					strlen(p_registro->value));
			i++;
			(*cant)++;
		}
	} // cierra el while
	if(i==0){
		t_registro* p_registro = malloc(12);
		p_registro->timestamp = -1;
		memcpy(&vectorStructs[i]->timestamp, &p_registro->timestamp, sizeof(p_registro->timestamp));
	}

	if (anteriorBloque != NULL) {
		fclose(anteriorBloque);
	}
	if (proximoBloque != NULL) {
		fclose(proximoBloque);
	}
}

void crearArrayPorKeyMemtable(t_registro** arrayPorKeyDeseadaMemtable,
		t_list* entradaTabla, int laKey, int *cant, char* tabla) {
	if(entradaTabla){
		while ((*cant) < list_size(entradaTabla)) {
			t_registro* p_registro = list_get(entradaTabla, (*cant));
			if (p_registro->key == laKey) {
				arrayPorKeyDeseadaMemtable[*cant] = malloc(12);
				memcpy(&arrayPorKeyDeseadaMemtable[*cant]->key,
						&p_registro->key, sizeof(p_registro->key));
				memcpy(&arrayPorKeyDeseadaMemtable[*cant]->timestamp,
						&p_registro->timestamp, sizeof(p_registro->timestamp));

				arrayPorKeyDeseadaMemtable[*cant]->value = malloc(
						strlen(p_registro->value));
				memcpy(arrayPorKeyDeseadaMemtable[*cant]->value,
						p_registro->value, strlen(p_registro->value));

				(*cant)++;
			}
		}
	}
}

int estaEntreComillas(char* valor) {
	return string_starts_with(valor, "\"") && string_ends_with(valor, "\"");
}

metadataTabla describeUnaTabla(char *tabla, int seImprimePorPantalla) {
	actualizarTiempoDeRetardo();
	sleep(structConfiguracionLFS.RETARDO);
	char* pathTabla = string_new();
	string_append(&pathTabla,
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	string_append(&pathTabla, "/");
	string_append(&pathTabla, tabla);
	DIR *directorio = opendir(pathTabla);
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Busco la metadata en la tabla
		if (!((directorioALeer->d_type) == DT_DIR)
				&& !strcmp((directorioALeer->d_name), "metadata")) {
			char *pathMetadata = string_new();
			string_append(&pathMetadata, pathTabla);
			string_append(&pathMetadata, "/");
			string_append(&pathMetadata, directorioALeer->d_name);
			//Lleno el struct con los valores de la metadata
			metadataTabla metadataTabla;
			metadataTabla.CONSISTENCY = string_new();
			t_config *metadata = config_create(pathMetadata);
			string_append(&(metadataTabla.CONSISTENCY),
					config_get_string_value(metadata, "CONSISTENCY"));
			metadataTabla.PARTITIONS = config_get_int_value(metadata,
					"PARTITIONS");
			metadataTabla.COMPACTION_TIME = config_get_int_value(metadata,
					"COMPACTION_TIME");

			closedir(directorio);
			config_destroy(metadata);

			if (seImprimePorPantalla) {
				printf("%s: \n", tabla);
				printf("Particiones: %i\n", metadataTabla.PARTITIONS);
				printf("Consistencia: %s\n", metadataTabla.CONSISTENCY);
				printf("Tiempo de compactacion: %i\n\n",
						metadataTabla.COMPACTION_TIME);
			}
			return metadataTabla;
		}
	}
	printf("¡Error! no se encontro la metadata\n");
	closedir(directorio);
	exit(-1);
}

t_dictionary *describeTodasLasTablas(int seImprimePorPantalla) {
	//Podria conservar la estructura en vez de borrar lo que tengo adentro pero tendria que ir fijandome que los valores que
	//tenga adentro el diccionario todavia sean validos, entonces es mas facil borrar tod y hacer describe desde 0
	dictionary_clean(diccionarioDescribe);

	DIR *directorio = opendir(
			string_from_format("%sTables",
					structConfiguracionLFS.PUNTO_MONTAJE));
	struct dirent *directorioALeer;
	while ((directorioALeer = readdir(directorio)) != NULL) {
		//Busco la metadata de todas las tablas (evaluo que no ingrese a los directorios "." y ".."
		if ((directorioALeer->d_type) == DT_DIR
				&& strcmp((directorioALeer->d_name), ".")
				&& strcmp((directorioALeer->d_name), "..")) {
			metadataTabla structMetadata;

			sem_t *semaforoTabla;
			char *tabla = string_new();
			string_append(&tabla, directorioALeer->d_name);
			//la key del diccionario esta en mayusculas para cada tabla
			dameSemaforo(tabla, &semaforoTabla);
			sem_wait(semaforoTabla);

			structMetadata = describeUnaTabla(directorioALeer->d_name,
					seImprimePorPantalla);
			dictionary_put(diccionarioDescribe, directorioALeer->d_name,
					&structMetadata);

			sem_post(semaforoTabla);

			/* Para probar que funciona esta wea
			 *
			 * metadataTabla *metadata;
			 * metadata = (metadataTabla *)dictionary_get(diccionarioDescribe, directorioALeer->d_name);
			 * printf("%i", metadata->COMPACTION_TIME);
			 *
			 */
		}
	}
	closedir(directorio);
	return diccionarioDescribe;

}

int32_t iniciarConexion() {
	int opt = 1;
	int master_socket, addrlen, new_socket, client_socket[30], max_clients = 30,
			activity, i, valread, sd;
	int max_sd;
	struct sockaddr_in address;

	char buffer[1025]; //data buffer of 1K

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

	//type of socket created
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons(structConfiguracionLFS.PUERTO_ESCUCHA);

	//bind the socket to localhost port 8888
	if (bind(master_socket, (struct sockaddr *) &address, sizeof(address))
			< 0) {
		perror("Bind fallo en el FS");
		return 1;
	}
	printf("Escuchando en el puerto: %d \n",
			structConfiguracionLFS.PUERTO_ESCUCHA);

	listen(master_socket, 100);

	//accept the incoming connection
	addrlen = sizeof(address);
	puts("Esperando conexiones ...");

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

			char* tamanioValue = string_itoa(structConfiguracionLFS.TAMANIO_VALUE);
			//send new connection greeting message
			if ( send(new_socket, (void*)tamanioValue, strlen(tamanioValue), 0)    !=      strlen(tamanioValue)) {
				perror("send");
			}

			//puts("Welcome message sent successfully");

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
				char *operacion = malloc(sizeof(int));
				valread = read(sd, operacion, sizeof(int));

				switch (atoi(operacion)) {
				case 1:
					//Select
					tomarPeticionSelect(sd);
					break;

				case 3:
					//Create
					tomarPeticionCreate(sd);
					break;

				case 6:
					//Insert
					tomarPeticionInsert(sd);
					break;

				default:
					break;

				}

				if (valread == 0) {
					//Somebody disconnected , get his details and print
					getpeername(sd, (struct sockaddr*) &address,
							(socklen_t*) &addrlen);
					printf("Host disconnected , ip %s , port %d \n",
							inet_ntoa(address.sin_addr),
							ntohs(address.sin_port));

					//Close the socket and mark as 0 in list for reuse
					close(sd);
					client_socket[i] = 0;
				}

				//Echo back the message that came in
				else {
					//set the string terminating NULL byte on the end
					//of the data read
					char mensaje[] = "Le llego tu mensaje al File System";
					buffer[valread] = '\0';
					printf("Memoria %d: %s\n", sd, buffer);
					send(sd, mensaje, strlen(mensaje), 0);

				}
			}
		}
	}

}


 void tomarPeticionSelect(int sd) {
	 // deserializo peticion de la memoria
	 void *tamanioTabla = malloc(sizeof(int));
	 read(sd, tamanioTabla, sizeof(int));

	 void *tabla = malloc(atoi(tamanioTabla));
	 read(sd, tabla, (int)tamanioTabla);

	 char *tamanioKey = malloc(sizeof(int));
	 read(sd, tamanioKey, sizeof(int));

	 char *key = malloc(atoi(tamanioKey));
	 read(sd, key, (int)tamanioKey);

	 printf("Haciendo Select");
	 char *value = realizarSelect(tabla, key);

	 // serializo paquete
	 void *buffer = malloc(strlen(value) + sizeof(int));
	 int tamanio = strlen(value);
	 memcpy(&buffer, &tamanio, sizeof(int));
	 memcpy(&buffer + sizeof(int), &value, strlen(value));

	 send(sd, buffer, strlen(value)+sizeof(int), 0);
 }

 void tomarPeticionCreate(int sd) {
 void *tamanioTabla = malloc(sizeof(int));
 read(sd, tamanioTabla, sizeof(int));
 void *tabla = malloc(atoi(tamanioTabla));
 read(sd, tabla, (int)tamanioTabla);

 void *tamanioConsistencia = malloc(sizeof(int));
 read(sd, tamanioConsistencia, sizeof(int));
 void *tipoConsistencia = malloc(atoi(tamanioConsistencia));
 read(sd, tipoConsistencia, (int)tamanioConsistencia);

 char *tamanioNumeroParticiones = malloc(sizeof(int));
 read(sd, tamanioNumeroParticiones, sizeof(int));
 void *numeroParticiones = malloc((int)tamanioNumeroParticiones);
 read(sd, numeroParticiones, (int)tamanioNumeroParticiones);

 void *tamanioTiempoCompactacion = malloc(sizeof(int));
 read(sd, tamanioTiempoCompactacion, sizeof(int));
 void *tiempoCompactacion = malloc((int)tamanioTiempoCompactacion);
 read(sd, tiempoCompactacion, (int)tamanioTiempoCompactacion);

 create(tabla, tipoConsistencia, numeroParticiones, tiempoCompactacion);
 void* buffer = malloc(sizeof(int));
 int ok = 1;
 memcpy(&buffer, &ok, sizeof(int));
 send(sd, buffer, sizeof(int), 0);
 }

 void tomarPeticionInsert(int sd) {
 char *tamanioTabla = malloc(sizeof(int));
 read(sd, tamanioTabla, sizeof(int));
 char *tabla = malloc(atoi(tamanioTabla));
 read(sd, tabla, atoi(tamanioTabla));

 char *tamanioKey = malloc(sizeof(int));
 read(sd, tamanioKey, sizeof(int));
 char *key = malloc(atoi(tamanioKey));
 read(sd, key, atoi(tamanioKey));

 char *tamanioValue = malloc(sizeof(int));
 read(sd, tamanioValue, sizeof(int));
 char *value = malloc(atoi(tamanioValue));
 read(sd, value, atoi(tamanioValue));

 void* existeTimestamp = malloc(sizeof(int));
 read(sd, existeTimestamp, sizeof(int));

 char* timestamp = NULL;
 if(existeTimestamp){
	 char *tamanioTime = malloc(sizeof(int));
	 read(sd, tamanioTime, sizeof(int));
	 timestamp = malloc(atoi(tamanioTime));
	 read(sd, timestamp, atoi(tamanioTime));
 }

 insert(tabla, key, value, timestamp);
 void* buffer = malloc(sizeof(int));
 int ok = 1;
 memcpy(&buffer, &ok, sizeof(int));
 send(sd, buffer, sizeof(int), 0);
 }


 void tomarPeticionDescribe(int sd){
	 char *tamanioTabla = malloc(sizeof(int));
	 read(sd, tamanioTabla, sizeof(int));
	 char *tabla = malloc(atoi(tamanioTabla));
	 read(sd, tabla, atoi(tamanioTabla));
 }
