// #include <cstddef>
// #include <fstream>
// #include <cstdint>
// #include <cstdio>
// #include <ios>
// #include <iostream>
// #include <sys/socket.h>
// #include <netinet/in.h>
// #include <unistd.h>
// #include <cstring>
// #include <fstream>
// #include <arpa/inet.h> // 必须包含这个处理 IP 转换
// #include <inttypes.h>



 
// uint64_t ntohll(uint64_t val)
// {
//     return (((uint64_t) ntohl(val)) << 32) + ntohl(val >> 32);
// }


// int  main(void)
// {
//     std::cout << "server start!" << std::endl;
//     int server_id = socket(AF_INET, SOCK_STREAM, 0);

//     sockaddr_in address;
//     address.sin_family = AF_INET;
//     address.sin_addr.s_addr = INADDR_ANY;
//     address.sin_port = htons(8888);

//     bind(server_id, (struct sockaddr*)&address, sizeof(address));

//     listen(server_id, 5);
    
//     std::cout << "Server socket start! wait client! " << ntohs( address.sin_port) << std::endl;

//     sockaddr_in client_addr;
//     socklen_t client_addr_len = sizeof(client_addr);
//     int client_socket = accept(server_id, (struct sockaddr*)&client_addr, (socklen_t *) &client_addr_len);
//     if(client_socket < 0) {
//         std::cerr << "accept failed!" << std::endl;
//         return 1;
//     }
//     std::cout << "server successful! ip :" << inet_ntoa(client_addr.sin_addr) << std::endl;

//     //读取文件名长度(4字节)
//     int name_len = 0;
//     uint32_t net_name_len = 0;
//     read(client_socket, &net_name_len, sizeof(uint32_t));
//     name_len = ntohl(net_name_len);
//     std::cout << "server read file name len :" << name_len << std::endl;
//     //读取文件名
//     char file_name[256] = {0};
//     read(client_socket, file_name,  name_len);
//     std::cout << "server get file : " << file_name << std::endl;
    
//     //读取文件大小(8字节)
//     long file_size = 0;
//     uint64_t net_file_size = 0;
//     read(client_socket, &net_file_size, sizeof(uint64_t));
//     file_size = ntohll(net_file_size);
//     std::cout << "server get Expected file size :" << file_size << std::endl;

//     std::ofstream outfile(std::string("../file/revice_") + file_name,  std::ios::binary);
//     if(!outfile.is_open())
//     {
//         std::cerr << "Error: Could not open file for writing: " << std::string("revice_") + file_name << std::endl;
//         return 1;
//     }
//     char buffer[1024];
//     long total_received = 0;
//     while(total_received < file_size)
//     {
//         int bytes_read = read(client_socket, buffer, sizeof(buffer));
//         if(bytes_read <= 0) break;
//         outfile.write(buffer, bytes_read);
//         total_received += bytes_read;
//     }

//     outfile.close();
//     close(client_socket);
//     close(server_id);

//     return 0;
// }











