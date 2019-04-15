#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>

int main()
{
	printf("\tSoy Kernel, mis subprocesos se reactivan una vez mas.\n");
	char mensaje[256] = "\nBienvenida memoria, mis puertos son tus puertos.\n";

	//Crear Servidor
	int socketServidor;
	socketServidor = socket(AF_INET, SOCK_STREAM, 0);

	//Configuracion para evitar esperar que el addr se pueda volver a usar por un cierre inesperado
	int activado = 1;
	setsockopt(socketServidor,SOL_SOCKET,SO_REUSEADDR, &activado, sizeof(activado));

	struct sockaddr_in direccionServer;
	direccionServer.sin_family = AF_INET;
	direccionServer.sin_port = htons(9002);
	direccionServer.sin_addr.s_addr = INADDR_ANY;

	if(bind(socketServidor, (struct sockaddr*) &direccionServer, sizeof(direccionServer)) != 0)
	{
		perror("Fallo el bind");
		return -1;
	}

	listen(socketServidor, 100);

	printf("Estoy escuchando\n");

	int socketCliente;

	socketCliente = accept(socketServidor, NULL, NULL);

	//Mandar Mensaje
	send(socketCliente, mensaje, sizeof(mensaje), 0);

	//Recibir Mensajes

	char* buffer = malloc(1001);

	while(1)
	{
		int bytesRecibidos = recv(socketCliente, buffer, 1000, 0);
		if(bytesRecibidos <= 0)
		{
			perror("Error en recepcion de mensaje\n");
			return 1;
		}

		buffer[bytesRecibidos] = '\0';

		printf("Me llegaron %d bytes con %s\n", bytesRecibidos, buffer);
	}

	free(buffer);

	//Cerrar el socket
	close(socketServidor);
	return 0;

}

