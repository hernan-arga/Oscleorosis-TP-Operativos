#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<arpa/inet.h>
#include<sys/types.h>
#include<sys/socket.h>
#include<netinet/in.h>

int main(void) {
	//Crear un socket
		int socketNetwork;
		socketNetwork = socket(AF_INET, SOCK_STREAM, 0);


		//Especificar la dirreccion para el socket
		struct sockddr_in dirreccionServidor;
		dirreccionServidor.sin_family = AF_INET;
		dirreccionServidor.sin_port = htons(9999);
		dirreccionServidor.sin_addr.s_addr = inet_addr("127.0.0.1");



		return 0;
}
