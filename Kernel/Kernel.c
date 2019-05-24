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

struct RR_config{
	int quantum;
};

struct LQL{
};

struct ColaReady{
	struct LQL;
	struct ColaReady*sig;
};

struct Script{
	struct LQL;
	struct Script*sig;
	struct Script*ant;
};

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


void round_robin(){
	FILE *round_robin= fopen("round_robin.bin","rb");
	if (round_robin==NULL) {
		printf("ERROR: Archivo round_robin vacio\n");
	}
	else{
		struct RR_config configuracion;

		fread(&configuracion,sizeof(struct RR_config),1,round_robin);
		fclose(round_robin);
		for(int contador=0;contador<configuracion.quantum;contador++){
			FILE* archivo = fopen(archivo_LQL,"rb");
			if(archivo==NULL){
				printf("ERROR: El archivo a ejecutar no contiene peticiones\n");
			}
		}
	}
}

//Lista de programas activos con PID cada uno, si alguno se termina de correr, el PID vuelve a estar libre para que otro programa entrante lo ocupe.
