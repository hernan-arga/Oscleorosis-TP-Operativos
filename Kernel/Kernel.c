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

struct Kernel_config{
	char ip[20];
	int puerto_memoria;
	int quantum;
	int multiprocesamiento;
	int refresh_metadata;
	int retardo_ciclo_ejecucion;
};

struct LQL{
	FILE*archivo = NULL;
};

struct Nodo_Script{
	struct LQL peticiones;
	struct Nodo_Script*sig;
};

struct Cola_Ready{

};

struct Script{
	struct LQL peticion;
	struct Script*sig;
	struct Script*ant;
};

typedef enum {
	SELECT, INSERT, CREATE, DESCRIBE, DROP, OPERACIONINVALIDA
} OPERACION;

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


void menu(){
	int opcionElegida;

	printf("Elija una opcion : \n");
	printf("1. SELECT \n	2. INSERT \n	3. CREATE\n		4. DESCRIBE \n		5. DROP\n		6. JOURNAL\n	7. ADD\n	8. RUN\n	9. METRICS\n");
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
		case 6:
			printf("Elegiste JOURNAL\n");
			break;
		case 7:
			printf("Elegiste ADD\n");
			break;
		case 8:
			printf("Elegiste RUN\n");
			break;
		case 9:
			printf("Elegiste METRICS\n");
			break;
		default:
			printf("ERROR\n");
			break;
	}
}

//Crear un planificador de RR con quantum configurable y que sea capaz de parsear los archivos LQL
void planificador(){
	FILE *kernel_config= fopen("Kernel_config.bin","rb");
	if (kernel_config==NULL) {
		printf("ERROR en el archivo de configuracion\n");
		fclose(kernel_config);
	}
	else{
		struct Kernel_config configuracion;
		char* nombre_del_archivo = malloc(1000);
		fread(&configuracion,sizeof(struct Kernel_config),1,kernel_config);
		fclose(kernel_config);
		do {
			printf("\t\nNecesito que ingreses el nombre del archivo a leer, con .LQL al final: ");
			fgets(nombre_del_archivo, 1000, stdin);
		} while (!strcmp(nombre_del_archivo, "\n"));
		FILE* archivo = fopen(nombre_del_archivo,"rb");
		free(nombre_del_archivo);
		if(archivo==NULL){
			printf("El archivo a ejecutar no contiene peticiones\n");
		}
		else{
				encolar_en_ready(archivo);

		}
	}
}

void encolar_en_ready(FILE* archivo){


}


while(!feof(archivo)){
	char tipo_de_peticion[2];
	for(int contador=0;contador<configuracion.quantum;contador++){
		fread(&tipo_de_peticion,sizeof(tipo_de_peticion),1,archivo);

	}
}
/*
void verificarPeticion(char* mensaje) {
	char* peticion = malloc(strlen(mensaje)+1);
	char* parametros = malloc(strlen(mensaje)+1);
	int seInsertaronParametros = separarPalabra(mensaje, &peticion, &parametros);
	if (seInsertaronParametros) {
		realizarPeticion(peticion, parametros);
	} else {
		printf("No se ingresaron parametros\n");
	}
	free(peticion);
	free(parametros);
}

//Las cadenas son especiales, ya que cuando paso char* paso el valor de la cadena, no su referencia.
//Para modificar cadenas se usa la doble referencia char**
int separarPalabra(char* mensaje, char** palabra, char** restoDelMensaje) {
	char delimitador[2] = " \n";
	strcpy(*palabra, strtok(mensaje, delimitador));
	//En la siguiente llamada strtok espera NULL en lugar de mensaje para saber que tiene que seguir operando con el resto
	char* loQueSigue = strtok(NULL, "\0");
	if (loQueSigue != NULL) {
		strcpy(*restoDelMensaje, loQueSigue);
	} else {
		return 0;
	}
	return 1;
}

//Separa los parametros e indica si la cantidad de los mismos es igual a la cantidad que se necesita
//Hay que arreglar que en lugar de que las palabras sean 30 fijo de tamaño sean dinamicos
int separarEnVector(char** parametros, char parametrosSeparados[][30],
		int cantidadDeElementos) {
	char delimitador[2] = " \n";
	//Para no modificar el valor de la variable "parametros" hago una copia
	char* copiaParametros = malloc(strlen(*parametros)+1);
	strcpy(copiaParametros, *parametros);
	int posicion = 0;
	//Es necesario liberar la memoria de la variable que sigue?
	char* token = strtok(copiaParametros, delimitador);
	while (token != NULL && posicion < cantidadDeElementos) {
		//Los vectores de char* son de solo lectura por eso vector de vectores de char para sobreescribir
		strcpy(parametrosSeparados[posicion], token);
		token = strtok(NULL, delimitador);
		posicion++;
	}
	free(copiaParametros);
	//si la cantidad de parametros ingresados es igual a lo necesario
	return (posicion == cantidadDeElementos && token == NULL);
}

void realizarPeticion(char* peticion, char* parametros) {
	OPERACION instruccion = tipoDePeticion(peticion);
	switch (instruccion) {
	case SELECT:
		printf("Seleccionaste Select\n");
		//Defino de que manera van a ser validos los parametros del select y luego paso el puntero de dicha funcion.
		//Los parametros son validos si el segundo (la key) es un numero, y la cantidadDeParametrosUsados solo se pasa para hacer
		//polimorfica la funcion criterioTiposCorrectos.
		int criterioSelect(char parametrosSeparados[][30], int cantidadDeParametrosUsados) {
			return esUnNumero(parametrosSeparados[1]);
		}

		if (parametrosValidos(2, parametros, (void *) criterioSelect))
			printf("ESTOY HACIENDO SELECT\n");
		break;
	case INSERT:
		printf("Seleccionaste Insert\n");
		int criterioInsert(char parametrosSeparados[][30],
				int cantidadDeParametrosUsados) {
			if (cantidadDeParametrosUsados == 4) {
				return esUnNumero(parametrosSeparados[1])
						&& esUnNumero(parametrosSeparados[3]);
			}
			return esUnNumero(parametrosSeparados[1]);
		}
		//puede o no estar el timestamp
		if (parametrosValidos(4, parametros, (void *) criterioInsert)
				|| parametrosValidos(3, parametros, (void *) criterioInsert))
			printf("ESTOY HACIENDO INSERT\n");
		break;
	case CREATE:
		printf("Seleccionaste Create\n");
		int criterioCreate(char parametrosSeparados[][30], int cantidadDeParametrosUsados) {
			return esUnNumero(parametrosSeparados[2])
					&& esUnNumero(parametrosSeparados[3])
					&& esUnTipoDeConsistenciaValida(parametrosSeparados[1]);
			return 1;
		}
		if (parametrosValidos(4, parametros, (void *) criterioCreate))
			printf("ESTOY HACIENDO CREATE\n");
		break;
	default:
		printf("Error operacion invalida\n");
	}
}

int parametrosValidos(int cantidadDeParametrosNecesarios, char* parametros,
		int (*criterioTiposCorrectos)(char[][30], int)) {
	return cantidadValidaParametros(parametros, cantidadDeParametrosNecesarios)
			&& tiposCorrectos(parametros, cantidadDeParametrosNecesarios,
					(void *) criterioTiposCorrectos);
}

int tiposCorrectos(char* parametros, int cantidadDeParametrosNecesarios, int (*criterioTiposCorrectos)(char[][30], int)) {
	char parametrosSeparados[cantidadDeParametrosNecesarios][30];
	separarEnVector(&parametros, parametrosSeparados,
			cantidadDeParametrosNecesarios);
	return criterioTiposCorrectos(parametrosSeparados,
			cantidadDeParametrosNecesarios);
}

int cantidadValidaParametros(char* parametros, int cantidadDeParametrosNecesarios) {
	char parametrosSeparados[cantidadDeParametrosNecesarios][30];
	int cantidadParametrosValida = separarEnVector(&parametros,
			parametrosSeparados, cantidadDeParametrosNecesarios);
	if (!cantidadParametrosValida)
		//hay que arreglar esto para que en el caso de insert solo lo muestre si no se cumple con 4 ni con 3
		printf("La cantidad de parametros no es valida\n");
	return cantidadParametrosValida;
}

OPERACION tipoDePeticion(char* peticion) {
	char* peticionUpperCase = malloc(strlen(peticion)+1);
	stringToUpperCase(peticion, &peticionUpperCase);
	if (!strcmp(peticionUpperCase, "SELECT")) {
		free(peticionUpperCase);
		return SELECT;
	} else {
		if (!strcmp(peticionUpperCase, "INSERT")) {
			free(peticionUpperCase);
			return INSERT;
		} else {
			if (!strcmp(peticionUpperCase, "CREATE")) {
				free(peticionUpperCase);
				return CREATE;
			} else {
				free(peticionUpperCase);
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

void stringToUpperCase(char* palabra, char** palabraEnMayusculas) {
	char* aux = malloc(strlen(palabra)+1);
	strcpy(aux, palabra);
	int i = 0;
	while (aux[i] != '\0') {
		aux[i] = toupper(aux[i]);
		i++;
	}
	strcpy(*palabraEnMayusculas, aux);
	free(aux);
}

//Lista de programas activos con PID cada uno, si alguno se termina de correr, el PID vuelve a estar libre para que otro programa entrante lo ocupe.*/
