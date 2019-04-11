/*
 * \\ socket.c \\
 *
 *  Created on: 11 abr. 2019
 *      Author: utnso
 */

#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>

#include <commons/config.h>


int main(void)
{
    //crea un socket
    int socketNetwork;
    socketNetwork = socket(AF_INET, SOCK_STREAM, 0);

    //especifica la dirreccion para el socket
    struct sockddr_in direccionServidor;
    direccionServidor.sin_family = AF_INET;
    direccionServidor.sinport = htons(9999);
    direccionServidor.sin_addr.s_addr = INADDR_ANY;

    //establece la conexion
    int connection_status = connect(socketNetwork, (void*) &direccionServidor, sizeof(direccionServidor));
    if(connection_status != 0){
    	perror("NO SE PUDO CONECTAR");
    	return 1;
    }

    //para enviar mensaje al servidor
    while(1){
    	char message[500];
    	scanf("%s", message);

    	send(socketNetwork, message, strlen(message), 0);
    }

    //recibe mensaje del servidor
    char server_response[500];
    recv(socketNetwork, &server_response, sizeof(server_response), 0);
    printf("EL SERVER MANDO : %s\n", server_response);

    //cerramos socket
    close(sock);

    return 0;
}


