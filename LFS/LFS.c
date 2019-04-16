// FS

#include<stdlib.h>
#include<stdio.h>
#include<unistd.h>
#include<readline/readline.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>
#include<sys/time.h>
#include<sys/types.h>
#include<unistd.h>

int main()
{
	printf("Soy el file system \n");
	char mensaje[256] = "\"Te has conectado con el FS\"";

	int sock_servidor_de_memoria;
	sock_servidor_de_memoria = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccion_server_memoria_lfs;
	direccion_server_memoria_lfs.sin_family = AF_INET;
	direccion_server_memoria_lfs.sin_port = htons(9002);
	direccion_server_memoria_lfs.sin_addr.s_addr = INADDR_ANY;

	int activado = 1;
	setsockopt(sock_servidor_de_memoria, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if(bind(sock_servidor_de_memoria, (struct sockaddr*) &direccion_server_memoria_lfs, sizeof(direccion_server_memoria_lfs)) != 0)
	{
		perror("Fallo el bind");
		return -1;
	}

	listen(sock_servidor_de_memoria, 100);

	int sock_memoria;
	struct sockaddr_in direccion_memoria;
	unsigned int tamanio_direccion;

	sock_memoria = accept(sock_servidor_de_memoria,(void*) &direccion_memoria, &tamanio_direccion);

	send(sock_memoria, mensaje, sizeof(mensaje), 0);

	//Recibir Mensajes
	char* buffer = malloc(1000);

	while(1)
	{
		int bytesRecibidos = recv(sock_memoria, buffer, 1000, 0);
		if(bytesRecibidos <= 0)
		{
			perror("Error en recepcion de mensaje");
			return 1;
		}

		buffer[bytesRecibidos] = '\0';

		printf("Me llegaron %d bytes con %s\n", bytesRecibidos, buffer);
	}

	free(buffer);

	//Cerramos socket
	close(sock_servidor_de_memoria);

	return 0;
}


void menu(){
	int opcionElegida;

	printf("Elija una opcion : \n");
	printf("1. SELECT \n	2. INSERT \n	3. CREATE\n		4. DESCRIBE \n		5. DROP\n");
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
		default:
			printf("ERROR");
			break;
	}
}
