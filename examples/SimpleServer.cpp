/*
	SimpleServer.cpp
	Basic networking server example
	
	Accepts client connections and broadcasts messages.
	Run first, then run SimpleClient.cpp
*/

#include "../include/olcPGEX_Network.h"
#include "../include/MMO_Common.h"
#include <iostream>
#include <thread>
#include <chrono>

class GameServer : public olc::net::server_interface<GameMsg>
{
public:
	GameServer(uint16_t nPort) : olc::net::server_interface<GameMsg>(nPort)
	{

	}

protected:
	virtual bool OnClientConnect(std::shared_ptr<olc::net::connection<GameMsg>> client)
	{
		olc::net::message<GameMsg> msg;
		msg.header.id = GameMsg::Server_GetStatus;
		msg << std::string("Server says: Client Connected!");
		
		client->Send(msg);
		return true;
	}

	virtual void OnClientDisconnect(std::shared_ptr<olc::net::connection<GameMsg>> client)
	{
		std::cout << "Removing client [" << client->GetID() << "]\n";
	}

	virtual void OnMessage(std::shared_ptr<olc::net::connection<GameMsg>> client, olc::net::message<GameMsg>& msg)
	{
		switch (msg.header.id)
		{
		case GameMsg::Client_RegisterWithServer:
		{
			// Client has sent us a message, print it out
			std::cout << "[" << client->GetID() << "]: Server Ping Request\n";

			// Construct a new message for the client to receive
			olc::net::message<GameMsg> msgOut;
			msgOut.header.id = GameMsg::Server_GetPing;

			// Send it back as a ping response.
			client->Send(msgOut);
			break;
		}

		case GameMsg::Game_UpdatePlayer:
		{
			// Receive player update and broadcast to all clients
			std::cout << "[" << client->GetID() << "]: Player Update\n";

			// Broadcast to all clients
			MessageAllClients(msg, client);
			break;
		}
		}
	}
};

int main()
{
	GameServer server(60000);
	server.Start();

	while (1)
	{
		server.Update(-1, true);
		std::cout << "Server running...\n";
		std::this_thread::sleep_for(std::chrono::seconds(1));
	}

	return 0;
}
