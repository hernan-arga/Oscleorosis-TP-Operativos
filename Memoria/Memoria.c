// MEMORIA

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
#include<commons/log.h>
#include<commons/string.h>
#include<commons/config.h>
#include<readline/readline.h>

// ABAJO MEMORIA COMO CLIENTE DEL SERVER FILE SYSTEM
int main(){
	printf("Soy Memoria \n");

	int socketClienteDelFS;
	socketClienteDelFS = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccionServer;
	direccionServer.sin_family = AF_INET;
	direccionServer.sin_port = htons(9002);
	direccionServer.sin_addr.s_addr = INADDR_ANY;

	if (connect(socketClienteDelFS, (struct sockaddr *) &direccionServer,
			sizeof(direccionServer)) == -1) {
		perror("Hubo un error en la conexion \n");
		return -1;
	}

	char buffer[256];
	recv(socketClienteDelFS, &buffer, sizeof(buffer), 0);

	printf("RECIBI INFORMACION: %s\n", buffer);

	//Mandar Mensajes
	while (1) {
		char mensaje[1000];
		scanf("%s", mensaje);

		send(socketClienteDelFS, mensaje, strlen(mensaje), 0);
	}

	close(socketClienteDelFS);

	//-------------------- ABAJO MEMORIA COMO SERVER DEL KERNEL CLIENTE

	char mensaje2[256] = "\"Te has conectado con la memoria\"";

		int socketServidorDelKernel;
		socketServidorDelKernel = socket(AF_INET, SOCK_STREAM, 0);

		struct sockaddr_in direccionKernel;
		direccionKernel.sin_family = AF_INET;
		direccionKernel.sin_port = htons(9003);
		direccionKernel.sin_addr.s_addr = INADDR_ANY;

		int activado = 1;
		setsockopt(socketServidorDelKernel, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

		if (bind(socketServidorDelKernel, (struct sockaddr*) &direccionKernel,
				sizeof(direccionKernel)) != 0) {
			perror("Fallo el bind");
			return -1;
		}

		listen(socketServidorDelKernel, 100);

		int sock_kernel;
		struct sockaddr_in direccion_kernel;
		unsigned int tamanio_coneccion;
		sock_kernel = accept(socketServidorDelKernel, (void*) &direccion_kernel, &tamanio_coneccion);

		//Mandar Mensaje
		send(sock_kernel, mensaje2, sizeof(mensaje2), 0);

		//Recibir Mensajes

		char* buffer2 = malloc(1000);

		while (1) {
			int bytesRecibidos = recv(sock_kernel, buffer2, 1000, 0);
			if (bytesRecibidos <= 0) {
				perror("Error en recepcion de mensaje");
				return 1;
			}

			buffer2[bytesRecibidos] = '\0';

			printf("Me llegaron %d bytes con %s\n", bytesRecibidos, buffer2);
		}

		free(buffer2);

		//Cerrar el socket
		close(socketServidorDelKernel);
		return 0;
}

void menu(){
	int opcionElegida;

	printf("Elija una opcion : \n");
	printf("1. SELECT \n	2. INSERT \n	3. CREATE\n		4. DESCRIBE \n		5. DROP\n		6. JOURNAL\n");
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
		default:
			printf("ERROR");
			break;
	}
}
