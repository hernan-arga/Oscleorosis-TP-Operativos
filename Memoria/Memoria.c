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
#include <errno.h>

#define TRUE 1
#define FALSE 0
#define PORT 4441

// ABAJO MEMORIA COMO CLIENTE DEL SERVER FILE SYSTEM
int main(int argc , char *argv[]){
	printf("Soy Memoria \n");

	int socketClienteDelFS;
	socketClienteDelFS = socket(AF_INET, SOCK_STREAM, 0);

	struct sockaddr_in direccionServer;
	direccionServer.sin_family = AF_INET;
	direccionServer.sin_port = htons(4444);
	direccionServer.sin_addr.s_addr = INADDR_ANY;

	if (connect(socketClienteDelFS, (struct sockaddr *) &direccionServer,
			sizeof(direccionServer)) == -1) {
		perror("Hubo un error en la conexion \n");
		return -1;
	}

	char buffer[256];
	recv(socketClienteDelFS, &buffer, sizeof(buffer), 0);

	printf("RECIBI INFORMACION DEL FS: %s\n", buffer);


	// Mandar Mensajes
	char mensaje[1000];
	scanf("%s", mensaje);
	send(socketClienteDelFS, mensaje, strlen(mensaje), 0);

	char buffer2[256];
	recv(socketClienteDelFS, &buffer2, sizeof(buffer2), 0);
	printf("fs dice : %s\n", buffer2);


/*
	// Mandar Mensajes
	while (1) {
		char mensaje[1000];
		scanf("%s", mensaje);
		send(socketClienteDelFS, mensaje, strlen(mensaje), 0);

		char buffer2[256];
		recv(socketClienteDelFS, &buffer2, sizeof(buffer2), 0);
		printf("fs dice : %s\n", buffer2);
	}
*/


//-------------------- ABAJO MEMORIA COMO SERVER DEL KERNEL CLIENTE


	int opt = TRUE;
	int master_socket , addrlen , new_socket , client_socket[30] ,
		max_clients = 30 , activity, i , valread , sd;
	int max_sd;
	struct sockaddr_in address;

	char buffer3[1025]; //data buffer of 1K

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
		perror("bind failed en memoria");
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
				valread = read( sd , buffer3, 1024);
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
					char mensaje[] = "Le llego tu mensaje a la memoria";

					printf("Kernel: %s\n", buffer3);
					send(sd , mensaje , strlen(mensaje) , 0 );
					buffer3[valread] = '\0';
				}
			}
		}
	}

	return 0;
}




/*
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

	char* buffer3 = malloc(1000);

	while (1) {
		int bytesRecibidos = recv(sock_kernel, buffer3, 1000, 0);
		if (bytesRecibidos <= 0) {
			perror("Error en recepcion de mensaje");
			return 1;
		}

		buffer2[bytesRecibidos] = '\0';

		printf("Me llegaron %d bytes con %s\n", bytesRecibidos, buffer3);
	}

	free(buffer3);

	//Cerrar el socket
	close(socketServidorDelKernel);
	close(socketClienteDelFS);

	return 0;
}

*/

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

