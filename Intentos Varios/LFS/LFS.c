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
	char mensaje[256] = "Te has conectado al servidor";

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

	close(sock_servidor_de_memoria);

	return 0;
}
