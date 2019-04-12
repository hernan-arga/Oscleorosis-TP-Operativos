#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>

int main()
{
	int socketCliente;
	socketCliente = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccionServer;
	direccionServer.sin_family = AF_INET;
	direccionServer.sin_port = htons(9002);
	direccionServer.sin_addr.s_addr = INADDR_ANY;

	if(connect(socketCliente, (struct sockaddr *) &direccionServer, sizeof(direccionServer)) == -1)
	{
		perror("Hubo un error en la coneccion");
		return -1;
	}

	char buffer[256];
	recv(socketCliente, &buffer, sizeof(buffer), 0);

	printf("El servidor ha recivido informacion: %s\n", buffer);

	//Mandar Mensajes
	while(1)
	{
		char mensaje[1000];
		scanf("%s", mensaje);

		send(socketCliente, mensaje, strlen(mensaje), 0);
	}

	close(socketCliente);

	return 0;


}

