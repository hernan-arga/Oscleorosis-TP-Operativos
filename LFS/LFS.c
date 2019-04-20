// FS

#include <stdio.h>
#include <string.h> //strlen
#include <stdlib.h>
#include <errno.h>
#include <unistd.h> //close
#include <arpa/inet.h> //close
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/time.h> //FD_SET, FD_ISSET, FD_ZERO macros

#define TRUE 1
#define FALSE 0
#define PORT 4444

int main(int argc , char *argv[])
{
	int opt = TRUE;
	int master_socket , addrlen , new_socket , client_socket[30] ,
		max_clients = 30 , activity, i , valread , sd;
	int max_sd;
	struct sockaddr_in address;

	char buffer[1025]; //data buffer of 1K

	//set of socket descriptors
	fd_set readfds;

	//a message
	char *message = "Este es el mensaje del server\r\n";

	//initialise all client_socket[] to 0 so not checked
	for (i = 0; i < max_clients; i++)
	{
	  client_socket[i] = 0;
	}

	//create a master socket
	if( (master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0)
	{
	  perror("socket failed");
	  exit(EXIT_FAILURE);
	}

	//set master socket to allow multiple connections ,
	//this is just a good habit, it will work without this
	if( setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt,
		sizeof(opt)) < 0 )
	{
		perror("setsockopt");
		exit(EXIT_FAILURE);
	}

	//type of socket created
	address.sin_family = AF_INET;
	address.sin_addr.s_addr = INADDR_ANY;
	address.sin_port = htons( PORT );

	//bind the socket to localhost port 8888
	if (bind(master_socket, (struct sockaddr *)&address, sizeof(address))<0)
	{
		perror("bind failed en lfs");
		exit(EXIT_FAILURE);
	}
	printf("Listener on port %d \n", PORT);

	listen(master_socket, 100);

	//accept the incoming connection
	addrlen = sizeof(address);
	puts("Waiting for connections ...");

	while(TRUE)
	{
		//clear the socket set
		FD_ZERO(&readfds);

		//add master socket to set
		FD_SET(master_socket, &readfds);
		max_sd = master_socket;

		//add child sockets to set
		for ( i = 0 ; i < max_clients ; i++)
		{
			//socket descriptor
			sd = client_socket[i];

			//if valid socket descriptor then add to read list
			if(sd > 0)
				FD_SET( sd , &readfds);

			//highest file descriptor number, need it for the select function
			if(sd > max_sd)
				max_sd = sd;
		}

		//wait for an activity on one of the sockets , timeout is NULL ,
		//so wait indefinitely
		activity = select( max_sd + 1 , &readfds , NULL , NULL , NULL);

		if ((activity < 0) && (errno!=EINTR))
		{
			printf("select error");
		}

		//If something happened on the master socket ,
		//then its an incoming connection
		if (FD_ISSET(master_socket, &readfds))
		{
			new_socket = accept(master_socket,(struct sockaddr *)&address, (socklen_t*)&addrlen);
			if (new_socket <0)
			{

				perror("accept");
				exit(EXIT_FAILURE);
			}

			//inform user of socket number - used in send and receive commands
			printf("New connection , socket fd is : %d , ip is : %s , port : %d 	\n" , new_socket , inet_ntoa(address.sin_addr) , ntohs
				(address.sin_port));

			//send new connection greeting message
			if( send(new_socket, message, strlen(message), 0) != strlen(message) )
			{
				perror("send");
			}

			puts("Welcome message sent successfully");

			//add new socket to array of sockets
			for (i = 0; i < max_clients; i++)
			{
				//if position is empty
				if( client_socket[i] == 0 )
				{
					client_socket[i] = new_socket;
					printf("Adding to list of sockets as %d\n" , i);

					break;
				}
			}
		}

		//else its some IO operation on some other socket
		for (i = 0; i < max_clients; i++)
		{
			sd = client_socket[i];

			if (FD_ISSET( sd , &readfds))
			{
				//Check if it was for closing , and also read the
				//incoming message
				valread = read( sd , buffer, 1024);
				if ( valread == 0)
				{
					//Somebody disconnected , get his details and print
					getpeername(sd , (struct sockaddr*)&address , \
						(socklen_t*)&addrlen);
					printf("Host disconnected , ip %s , port %d \n" ,
						inet_ntoa(address.sin_addr) , ntohs(address.sin_port));

					//Close the socket and mark as 0 in list for reuse
					close( sd );
					client_socket[i] = 0;
				}

				//Echo back the message that came in
				else
				{
					//set the string terminating NULL byte on the end
					//of the data read
					char mensaje[] = "Le llego tu mensaje al File System";
					buffer[valread] = '\0';
					printf("Memoria %d: %s\n",sd, buffer);
					send(sd , mensaje , strlen(mensaje) , 0 );

				}
			}
		}
	}

	return 0;
}




/*
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
*/

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

