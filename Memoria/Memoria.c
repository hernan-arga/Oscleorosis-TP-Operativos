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

typedef struct
{
	char* nombreTabla;
	t_list* paginas;
}segmento;

typedef struct
{
	int numeroPag;
	bool modificado;
	int numeroFrame;
	long int timeStamp;
}pagina;

typedef struct
{
	int particiones;
	char* consistencia;
	int tiempoCompactacion;
}metadataTabla;

typedef struct{
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
}archivoConfiguracion;

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

//Hilos
pthread_t threadKernel;
pthread_t threadFS;

void analizarInstruccion(char* instruccion);
void realizarComando(char** comando);
OPERACION tipoDePeticion(char* peticion);
int realizarSelect(char* tabla, char* key);
int realizarInsert(char* tabla, char* key, char* value);
int frameLibre();
char* pedirValue(char* tabla, char* key);
int ejecutarLRU();
void ejecutarJournaling();
void realizarCreate(char* tabla, char* tipoConsistencia, char* numeroParticiones, char* tiempoCompactacion);
void realizarDrop(char* tabla);
void realizarDescribeGolbal();
void realizarDescribe(char* tabla);
void consola();
void serServidor();
void conectarseAFS();
void conectarseAKernel();
void gosiping(int cliente);
void tratarCliente(int cliente);

int main()
{
	sem_init(&sem, 1, 0);

	pthread_t threadSerServidor;
	int32_t idThreadSerServidor = pthread_create(&threadSerServidor, NULL, serServidor, NULL);

	tablaSegmentos = dictionary_create();

	memoriaPrincipal = malloc(1000);

	sem_wait(&sem);

	//tamanoValue = 100;

	tamanoFrame = sizeof(int)+sizeof(long int)+tamanoValue;
	//Key , TimeStamp, Value

	int tablaFrames[1000/tamanoFrame];
	frames = tablaFrames;

	for(int i = 0; i < 1000/tamanoFrame; i++)
	{
		*(frames+i) = 0;
	}

	pthread_t threadConsola;
	int32_t idthreadConsola = pthread_create(&threadConsola, NULL, consola, NULL);

	pthread_join(threadConsola, NULL);
	//pthread_join(threadSerServidor, NULL);
}

void analizarInstruccion(char* instruccion)
{
	char** comando = malloc(strlen(instruccion) + 1 );
	comando = string_split(instruccion, " \n");
	realizarComando(comando);
	free(comando);
}

void realizarComando(char** comando)
{
	char *peticion = comando[0];
	OPERACION accion = tipoDePeticion(peticion);
	char* tabla;
	char* key;
	char* value;
	switch(accion)
	{
		case SELECT:
			printf( "SELECT");
			tabla = comando[1];
			key = comando[2];
			realizarSelect(tabla, key);
			break;

		case INSERT:
			printf("INSERT");
			tabla = comando[1];
			key = comando[2];
			value = comando[3];
			realizarInsert(tabla, key, value);
			break;

		case CREATE:
			printf("CREATE");
			tabla = comando[1];
			char* tipoConsistencia = comando[2];
			char* numeroParticiones = comando[3];
			char* tiempoCompactacion = comando[4];
			realizarCreate(tabla, tipoConsistencia, numeroParticiones, tiempoCompactacion);
			break;

		//Describe recibe un diccionario con (nombreTabla - struct(con la info de la metadata)

		case DESCRIBE:
			printf("\nDESCRIBE");

			if(comando[1] == NULL)
			{
				printf("GLOBAL");
				realizarDescribeGolbal();
			}
			else
			{
				printf("Normal");
				tabla = comando[1];
				realizarDescribe(tabla);
			}
			break;

		case DROP:
			printf("\nDROP");
			tabla = comando[1];
			realizarDrop(tabla);
			break;

		case JOURNAL:
			printf("\nJOURNAL");
			ejecutarJournaling();
			break;

		case OPERACIONINVALIDA:
			printf("OPERACION INVALIDA");
			break;
	}
}

OPERACION tipoDePeticion(char* peticion)
{
	if (!strcmp(peticion, "SELECT"))
	{
		free(peticion);
		return SELECT;
	} else if (!strcmp(peticion, "INSERT")) {
			free(peticion);
			return INSERT;
		} else if (!strcmp(peticion, "CREATE")) {
				free(peticion);
				return CREATE;
			} else if(!strcmp(peticion, "DESCRIBE"))
			{
				free(peticion);
				return DESCRIBE;
			}else if(!strcmp(peticion, "DROP"))
			{
				free(peticion);
				return DROP;
			}else if(!strcmp(peticion, "JOURNAL"))
			{
				free(peticion);
				return JOURNAL;
			}else {
				free(peticion);
				return OPERACIONINVALIDA;
			}
		}

int realizarSelect(char* tabla, char* key)
{
	if(dictionary_has_key(tablaSegmentos, tabla))
	{
		t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

		for(int i = 0; i < list_size(tablaPag); i++)
		{
			pagina* pag = list_get(tablaPag, i);

			char* laKey = malloc(sizeof(int));

			memcpy(laKey, (memoriaPrincipal+pag->numeroFrame*tamanoFrame), sizeof(int));

			if(!strcmp(laKey, key))
			{
				char* value = malloc(tamanoValue);

				memcpy(value, (memoriaPrincipal+pag->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), *(frames+pag->numeroFrame));

				char* timeStamp = malloc(sizeof(long int));
				long int timeS = (long int) time(NULL);
				sprintf(timeStamp, "%d", timeS);

				memcpy((memoriaPrincipal+pag->numeroFrame*tamanoFrame+sizeof(int)), timeStamp, sizeof(long int));

				free(timeStamp);

				printf("%s", value);

				pag->timeStamp = timeS;

				free(value);
				free(laKey);

				return 0;
			}
			free(laKey);
		}

		int frameNum = frameLibre();

		pagina* pagp = malloc(sizeof(pagina));
		pagp->modificado = false;
		pagp->numeroFrame = frameNum;
		pagp->numeroPag = list_size(tablaPag);
		pagp->timeStamp = (long int) time(NULL);

		list_add(tablaPag, pagp);

		char* value = malloc(tamanoValue);
		value = pedirValue(tabla, key);

		*(frames+frameNum) = strlen(value);

		printf("%s", value);

		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame), key, sizeof(int));
		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), value, strlen(value));

		char* timeStamp = malloc(sizeof(long int));
		sprintf(timeStamp, "%d", (long int) time(NULL));

		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)), timeStamp, sizeof(long int));

		free(timeStamp);
		free(value);

		return 0;
	}

	char* value = malloc(tamanoValue);
	strcpy(value, pedirValue(tabla, key));

	int frameNum = frameLibre();
	*(frames+frameNum) = strlen(value);

	pagina* pagp = malloc(sizeof(pagina));
	t_list* paginasp = list_create();

	pagp->modificado = false;
	pagp->numeroFrame = frameNum;
	pagp->numeroPag = 0;
	pagp->timeStamp = (long int)time(NULL);

	list_add(paginasp, pagp);
	dictionary_put(tablaSegmentos, tabla, paginasp);

	char* timeStamp = malloc(sizeof(long int));
	sprintf(timeStamp, "%ld", pagp->timeStamp);

	memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame), key, sizeof(int));
	memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)), timeStamp, sizeof(long int));
	memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), value, strlen(value));

	free(timeStamp);
	free(value);
	return 0;
}

int realizarInsert(char* tabla, char* key, char* value)
{
	if(dictionary_has_key(tablaSegmentos, tabla))
	{
		t_list* tablaPag = dictionary_get(tablaSegmentos, tabla);

		for(int i = 0; i < list_size(tablaPag); i++)
		{
			pagina* pagy = list_get(tablaPag, i);

			char* laKey = malloc(sizeof(int));

			memcpy(laKey, (memoriaPrincipal+pagy->numeroFrame*tamanoFrame), sizeof(int));

			if(!strcmp(laKey, key))
			{
				char* timeStamp = malloc(sizeof(long int));
				sprintf(timeStamp, "%d", (long int) time(NULL));

				memcpy((memoriaPrincipal+(pagy->numeroFrame*tamanoFrame)+sizeof(int)+sizeof(long int)), (const char*) value, strlen(value));
				memcpy(memoriaPrincipal+pagy->numeroFrame*tamanoFrame+sizeof(int), timeStamp, sizeof(long int));

				*(frames+pagy->numeroFrame) = strlen(value);

				pagy->timeStamp = (long int) time(NULL);

				free(laKey);
				free(timeStamp);

				return 0;
			}
		}

		int frameNum = frameLibre();
		*(frames+frameNum) = strlen(value);

		pagina* pagp = malloc(sizeof(pagina));

		pagp->modificado = true;
		pagp->numeroFrame = frameNum;
		pagp->numeroPag = list_size(tablaPag);
		pagp->timeStamp = (long int)time(NULL);

		list_add(tablaPag, pagp);

		char* timeStamp = malloc(sizeof(long int));
		sprintf(timeStamp, "%ld", pagp->timeStamp);

		memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame, key, sizeof(int));
		memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int), timeStamp, sizeof(long int));
		memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int), value, strlen(value));

		free(timeStamp);

		return 0;
	}

	int frameNum = frameLibre();
	*(frames+frameNum) = strlen(value);

	pagina* pagp = malloc(sizeof(pagina));
	pagp->modificado = true;
	pagp->numeroFrame = frameNum;
	pagp->numeroPag = 0;
	pagp->timeStamp = (long int) time(NULL);

	t_list* paginas = list_create();
	list_add(paginas, pagp);

	char* timeStamp = malloc(sizeof(long int));
	sprintf(timeStamp, "%ld", pagp->timeStamp);

	dictionary_put(tablaSegmentos, tabla, paginas);

	memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame, key, sizeof(int));
	memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int), timeStamp, sizeof(long int));
	memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int), value, strlen(value));

	free(timeStamp);
}

int frameLibre()
{
	for(int i = 0; i < 1000/tamanoFrame; i++)
	{
		if(*(frames+i) == 0)
		{
			return i;
		}
	}
	printf("Ejecutar LRU");
	return ejecutarLRU();
}

char* pedirValue(char* tabla, char* laKey)
{
	// Serializo tabla y key
	/*
	int key = atoi(laKey);
	void* buffer = malloc( strlen(tabla) + sizeof(int) + 2*sizeof(int) ); // primeros dos terminos para TABLA; ultimo termino para KEY

	int tamanioTabla = strlen(tabla);
	memcpy(&buffer, &tamanioTabla, sizeof(int));
	memcpy(&buffer + sizeof(int), &tabla, strlen(tabla));

	int tamanioKey = sizeof(int);
	memcpy(&buffer + sizeof(int) + strlen(tabla), &tamanioKey, sizeof(int));
	memcpy(&buffer + 2*sizeof(int) + strlen(tabla), &key, sizeof(int));

	send(sd, buffer, strlen(tabla) + 3*sizeof(int), 0);

	//deserializo value
	char *tamanioValue = malloc(sizeof(int));
	read(sd, tamanioValue, sizeof(int));
	char *value = malloc(atoi(tamanioValue));
	read(sd, value, atoi(tamanioValue));

	return value;
	*/

	char* mensaje = malloc(sizeof(int) + sizeof(int) + strlen(tabla) + sizeof(int) + strlen(key));

	strcpy(mensaje, "0");

	char* num = malloc(sizeof(int));
	sprintf(num, "%d", strlen(tabla));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tabla);

	num = malloc(sizeof(int));
	sprintf(num, "%d", strlen(key));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, key);

	send(clienteFS, mensaje, strlen(mensaje), 0);

	free(mensaje);

	char* tamV = malloc(2);

	recv(clienteFS, tamV, sizeof(int) , 0);

	printf("%s\n", tamV);
	printf("%d\n", atoi(tamV));

	char* value = malloc(atoi(tamV));

	recv(clienteFS, value, atoi(tamV)+1 , 0);

	printf("\n%s\n", value);

	free(tamV);

	return value;
}

int ejecutarLRU()
{
	long int timeStamp = 0;
	int numF;
	int target;
	t_list* objetivo;

	void elMenor(char* key, void* value)
	{
		t_list* paginas = value;
		for(int i = 0; i < list_size(paginas); i++)
		{
			pagina* pag = list_get(paginas, i);
			if(timeStamp == 0 && !pag->modificado)
			{
			timeStamp = pag->timeStamp;
			numF = pag->numeroFrame;
			target = pag->numeroPag;
			objetivo = paginas;
			}
			else
			{
				if(pag->timeStamp < timeStamp && !pag->modificado)
				{
					timeStamp = pag->timeStamp;
					numF = pag->numeroFrame;
					target = pag->numeroPag;
					objetivo = paginas;
				}
			}
		}
	}
	dictionary_iterator(tablaSegmentos, elMenor);
	if(timeStamp == 0)
	{
		ejecutarJournaling();
		numF = 0;
	}
	else
	{
		list_remove_and_destroy_element(objetivo, target, NULL);
	}
	return numF;
}

void ejecutarJournaling()
{
	void journal(char* tabla, void* valor)
	{
		t_list* paginas = valor;
		for(int i = 0; i < list_size(paginas); i++)
		{
			pagina* pag = list_get(paginas, i);
			if(pag->modificado)
			{
				char* key = malloc(sizeof(int));
				memcpy(key, (memoriaPrincipal+pag->numeroFrame*tamanoFrame), sizeof(int));

				char* value = malloc(tamanoValue);
				memcpy(value, (memoriaPrincipal+pag->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), *(frames+pag->numeroFrame));

				char* mensaje = malloc(sizeof(int)+sizeof(int)+sizeof(tabla)+sizeof(int)+
						sizeof(key)+sizeof(int)+strlen(value));
				strcpy(mensaje, "1");

				char* num = malloc(sizeof(int));
				sprintf(num, "%d", sizeof(tabla));

				strcat(mensaje, num);
				free(num);

				strcat(mensaje, tabla);

				num = malloc(sizeof(int));
				sprintf(num, "%d", sizeof(key));

				strcat(mensaje, num);
				free(num);

				strcat(mensaje, key);

				num = malloc(sizeof(int));
				sprintf(num, "%d", strlen(value));

				strcat(mensaje, num);
				free(num);

				strcat(mensaje, value);

				//Magia Sockets

				free(mensaje);
			}
		}
	}
	for(int i = 0; i < 1000/tamanoFrame; i++)
	{
		*(frames+i) = 0;
	}
	dictionary_clean_and_destroy_elements(tablaSegmentos, NULL);
}

void realizarCreate(char* tabla, char* tipoConsistencia, char* numeroParticiones, char* tiempoCompactacion)
{
	char* mensaje = malloc(sizeof(int) + sizeof(int) + sizeof(tabla) + sizeof(int) + sizeof(tipoConsistencia)
			+ sizeof(int) + sizeof(numeroParticiones) + sizeof(int) + sizeof(tiempoCompactacion));

	strcpy(mensaje, "2");

	char* num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(tabla));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tabla);

	num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(tipoConsistencia));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tipoConsistencia);

	num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(numeroParticiones));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, numeroParticiones);

	num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(tiempoCompactacion));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tiempoCompactacion);

	//Magia Sockets

	free(mensaje);

	printf("\nSe envio la peticion\n");
}

void realizarDrop(char* tabla)
{
	if(dictionary_has_key(tablaSegmentos, tabla))
	{
		void* elemento = dictionary_remove(tablaSegmentos, tabla);
		free(elemento);
	}

	char* mensaje = malloc(sizeof(int) + sizeof(int) + sizeof(tabla));

	strcpy(mensaje, "5");

	char* num = malloc(sizeof(int));
	sprintf(num, "%d", strlen(tabla));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tabla);

	//Magia sockets

	// Serializo tabla
	/*
	void* buffer = malloc( strlen(tabla) + sizeof(int) );
	int tamanioTabla = strlen(tabla);
	memcpy(&buffer, &tamanioTabla, sizeof(int));
	memcpy(&buffer + sizeof(int), &tabla, strlen(tabla));

	send(sd, buffer, strlen(tabla) + sizeof(int), 0);
	*/

	free(mensaje);
}

void realizarDescribe(char* tabla)
{
	//3
	char* mensaje = malloc(sizeof(int) + sizeof(int) + sizeof(tabla));

	strcpy(mensaje, "3");

	char* num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(tabla));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tabla);

	//Magia sockets

	free(mensaje);

	char* metadata = malloc(sizeof(metadataTabla));

	metadataTabla* data = metadata;

	printf("\nTabla: %s", tabla);
	printf("\nParticiones: %d", data->particiones);
	printf("\nConsistencia: %s", data->consistencia);
	printf("\nTiempo Compactacion: %d", data->tiempoCompactacion);

	free(metadata);
}

void realizarDescribeGolbal()
{
	//4
	char* mensaje = malloc(sizeof(int));
	strcpy(mensaje, "4");

	//Magia sockets

	free(mensaje);

	mensaje = malloc(sizeof(t_dictionary));

	t_dictionary* tablas = mensaje;

	void mostrar(char* tabla, void* metadata)
	{
		metadataTabla* data = metadata;

		printf("\nTabla: %s", tabla);
		printf("\nParticiones: %d", data->particiones);
		printf("\nConsistencia: %s", data->consistencia);
		printf("\nTiempo Compactacion: %d", data->tiempoCompactacion);
	}

	dictionary_iterator(tablas, mostrar);

	free(mensaje);
}

void consola()
{
	while(1)
	{
		char* instruccion = malloc(1000);

		do {

			fgets(instruccion, 1000, stdin);

		} while (!strcmp(instruccion, "\n"));

		analizarInstruccion(instruccion);

		free(instruccion);
	}
}

void serServidor()
{
	serverAddress.sin_family = AF_INET;
	serverAddress.sin_addr.s_addr = INADDR_ANY;
	//serverAddress.sin_port = htons(t_archivoConfiguracion.PUERTO);
	serverAddress.sin_port = htons(4092);

	server = socket(AF_INET, SOCK_STREAM, 0);

	setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if(bind(server, (void*) &serverAddress, sizeof(serverAddress)) != 0)
	{
		perror("Fallo el bind");
	}

	printf( "Estoy escuchando\n");
	listen(server, 100);

	//conectarseAKernel();
	int i = 0;
	while(t_archivoConfiguracion.PUERTO_SEEDS[i] != NULL)
	{
		int cliente = socket(AF_INET, SOCK_STREAM, 0);
		int* sock = malloc(sizeof(int));
		sock = cliente;

		direccionCliente.sin_family = AF_INET;
		direccionCliente.sin_port = htons(atoi(t_archivoConfiguracion.PUERTO_SEEDS[i]));
		direccionCliente.sin_addr.s_addr = atoi(t_archivoConfiguracion.IP_SEEDS[i]);

		connect(cliente, (struct sockaddr *) &direccionCliente, sizeof(direccionCliente));
		//Mandar un 0 para q se sepa si es memoria o kernel
		list_add(clientes, sock);

		gossiping(cliente);

		i++;
	}
	conectarseAFS();
}

void conectarseAFS()
{
	clienteFS = socket(AF_INET, SOCK_STREAM, 0);
	serverAddressFS.sin_family = AF_INET;
	//serverAddressFS.sin_port = htons(t_archivoConfiguracion.PUERTO_FS);
	//serverAddressFS.sin_addr.s_addr = atoi(t_archivoConfiguracion.IP_FS);
	serverAddressFS.sin_port = htons(4093);
	serverAddress.sin_addr.s_addr = INADDR_ANY;

	if (connect(clienteFS, (struct sockaddr *) &serverAddressFS, sizeof(serverAddressFS)) == -1)
	{
		perror("Hubo un error en la conexion \n");
	}

	char* tamano = malloc(100);

	recv(clienteFS, tamano, sizeof(tamano), 0);

	printf("%s", tamano);

	tamanoValue = atoi(tamano);

	free(tamano);

	sem_post(&sem);
}

void conectarseAKernel()
{
	//clienteKernel = accept(server, (void*) &serverAddress, &tamanoDireccion);
	//printf("Recibi una conexion en %d\n", clienteKernel);

	//pthread_create(&threadKernel, NULL, (void*) controlarKernel, NULL);
	//pthread_join(threadKernel, NULL);
}

void aceptar()
{
	while(1)
	{
		int cliente = accept(server, (void*) &serverAddress, &tamanoDireccion);
		tratarCliente(cliente);
	}
}

void gossiping(int cliente)
{
	while(1)
	{
		sleep(t_archivoConfiguracion.RETARDO_GOSSIPING);
		send(cliente, clientes, sizeof(cliente), 0);
	}
}

void tratarCliente(int cliente)
{
	while(1)
	{
		t_list* lista;
		recv(cliente, lista, sizeof(lista), 0);
		list_add_all(clientes, lista);
	}
}
