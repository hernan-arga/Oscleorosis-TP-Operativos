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

t_dictionary* tablaSegmentos;
char* memoriaPrincipal;
int tamanoFrame;
int tamanoValue;
int* frames;

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

int main()
{
	tablaSegmentos = dictionary_create();
	memoriaPrincipal = malloc(1000);
	tamanoValue = 100;
	tamanoFrame = sizeof(int)+sizeof(long int)+tamanoValue;
	//Key , TimeStamp, Value
	int tablaFrames[1000/tamanoFrame];
	frames = tablaFrames;

	for(int i = 0; i < 1000/tamanoFrame; i++)
	{
		*(frames+i) = 0;
	}

	printf("\nTamaño frame: %d", tamanoFrame);

	while(1)
	{
		char* instruccion = malloc(1000);

		do
		{
			fgets(instruccion, 1000, stdin);
		}while(!strcmp(instruccion, " \n"));

		analizarInstruccion(instruccion);
		free(instruccion);
	}
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

		printf("Pedir a el FS");

		int frameNum = frameLibre();

		pagina* pagp = malloc(sizeof(pagina));
		pagp->modificado = false;
		pagp->numeroFrame = frameNum;
		pagp->numeroPag = list_size(tablaPag);
		pagp->timeStamp = (long int) time(NULL);

		list_add(tablaPag, pagp);

		char* value = malloc(tamanoValue);
		value = pedirValue(tabla, key);

		*(frames+frameNum) = sizeof(value);

		printf("%s", value);

		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame), key, sizeof(int));
		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), value, sizeof(value));

		char* timeStamp = malloc(sizeof(long int));
		sprintf(timeStamp, "%d", (long int) time(NULL));

		memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)), timeStamp, sizeof(long int));

		free(timeStamp);
		free(value);

		return 0;
	}

	char* value = malloc(tamanoValue);
	value = pedirValue(tabla, key);

	int frameNum = frameLibre();
	*(frames+frameNum) = sizeof(value);

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
	memcpy((memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int)), value, sizeof(value));

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

				memcpy((memoriaPrincipal+(pagy->numeroFrame*tamanoFrame)+sizeof(int)+sizeof(long int)), (const char*) value, sizeof(value));
				memcpy(memoriaPrincipal+pagy->numeroFrame*tamanoFrame+sizeof(int), timeStamp, sizeof(long int));

				*(frames+pagy->numeroFrame) = sizeof(value);

				pagy->timeStamp = (long int) time(NULL);

				free(laKey);
				free(timeStamp);

				return 0;
			}
		}

		int frameNum = frameLibre();
		*(frames+frameNum) = sizeof(value);

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
		memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int), value, sizeof(value));

		free(timeStamp);

		return 0;
	}

	int frameNum = frameLibre();
	*(frames+frameNum) = sizeof(value);

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
	memcpy(memoriaPrincipal+pagp->numeroFrame*tamanoFrame+sizeof(int)+sizeof(long int), value, sizeof(value));

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

char* pedirValue(char* tabla, char* key)
{
	printf("\nMagia de socket\n");
	char* value = malloc(100);
	strcpy(value, "NO esta");
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
						sizeof(key)+sizeof(int)+sizeof(value));
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
				sprintf(num, "%d", sizeof(value));

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

	strcpy(mensaje, "4");

	char* num = malloc(sizeof(int));
	sprintf(num, "%d", sizeof(tabla));

	strcat(mensaje, num);
	free(num);

	strcat(mensaje, tabla);

	//Magia sockets

	free(mensaje);
}

void realizarDescribe(char* tabla)
{

}

void realizarDescribeGolbal()
{

}
