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

void CREATE()
{
	printf("Create");
}

void SELECT()
{
	printf("Select");
}

void DESCRIBE()
{
	printf("Describe");
}

void INSERT()
{
	printf("Insert");
}

void DROP()
{
	printf("Drop");
}

void JOURNAL()
{
	printf("Journal");
}

int main()
{
	char mensaje[256] = "Te has conectado al servidor";

	int sock_servidor_kernel;
	sock_servidor_kernel = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccion_server_memoria_kernel;
	direccion_server_memoria_kernel.sin_family = AF_INET;
	direccion_server_memoria_kernel.sin_port = htons(9002);
	direccion_server_memoria_kernel.sin_addr.s_addr = INADDR_ANY;

	int activado = 1;
	setsockopt(sock_servidor_kernel, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

	if(bind(sock_servidor_kernel, (struct sockaddr*) &direccion_server_memoria_kernel, sizeof(direccion_server_memoria_kernel)) != 0)
	{
		perror("Fallo el bind");
		return -1;
	}

	listen(sock_servidor_kernel, 100);

	int sock_kernel;
	struct sockaddr_in direccion_kernel;
	unsigned int tamanio_coneccion;
	sock_kernel = accept(sock_servidor_kernel, (void*) &direccion_kernel, &tamanio_coneccion);

	send(sock_kernel, mensaje, sizeof(mensaje), 0);

	int sock_cliente_de_lfs;
	sock_cliente_de_lfs = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccion_server_memoria_lfs;
	direccion_server_memoria_lfs.sin_family = AF_INET;
	direccion_server_memoria_lfs.sin_port = htons(9003);
	direccion_server_memoria_lfs.sin_addr.s_addr = INADDR_ANY;

	if(connect(sock_cliente_de_lfs, (struct sockaddr *) &direccion_server_memoria_lfs, sizeof(direccion_server_memoria_lfs)) == -1)
	{
		perror("Hubo un error en la coneccion");
		return -1;
	}

	char buffer[256];
	recv(sock_cliente_de_lfs, &buffer, sizeof(buffer), 0);

	printf("El servidor ha recivido informacion: %s\n", buffer);

	char *leido = malloc(50);
	while(strcmp(leido, "EXIT"))
	{
		scanf("%s", leido);
		if(!strcmp(leido, "CREATE"))
			CREATE();
	}

	close(sock_cliente_de_lfs);
	close(sock_servidor_kernel);
	free(leido);
	return 0;
}
