/*
	SimpleClient.cpp
	Basic networking client example
	
	Connects to server and sends messages.
	Run SimpleServer.cpp first, then run this.
*/

#include "../include/olcPGEX_Network.h"
#include "../include/MMO_Common.h"
#include <iostream>
#include <thread>
#include <chrono>

class GameClient : public olc::net::client_interface<GameMsg>
{
public:
	// Helper function to get player position from user input
	void GetPlayerInput()
	{
		// In a real game, this would read from keyboard input
		// For this example, we'll just simulate movement
	}

	// Helper function to send player update
	void SendPlayerUpdate()
	{
		olc::net::message<GameMsg> msg;
		msg.header.id = GameMsg::Game_UpdatePlayer;

		sPlayerDescription player;
		player.x = 100.0f;
		player.y = 200.0f;
		player.vx = 1.0f;
		player.vy = 0.0f;
		player.nUniqueID = 1;
		player.nHealth = 100;
		strcpy(player.szName, "Player");

		msg << player;
		Send(msg);
	}

private:
};

int main()
{
	GameClient c;
	c.Connect("127.0.0.1", 60000);

	// Give it a moment to connect
	std::this_thread::sleep_for(std::chrono::seconds(1));

	bool bShutdown = false;
	while (!bShutdown)
	{
		if (c.IsConnected())
		{
			if (!c.Incoming().empty())
			{
				auto msg = c.Incoming().pop_front();

				switch (msg.msg.header.id)
				{
				case GameMsg::Server_GetStatus:
				{
					std::cout << "Server Status: ";
					std::string sMsg;
					msg.msg >> sMsg;
					std::cout << sMsg << "\n";
					break;
				}

				case GameMsg::Server_GetPing:
				{
					auto chrNow = std::chrono::high_resolution_clock::now();
					auto t1 = std::chrono::high_resolution_clock::now();
					std::cout << "Ping: " << std::chrono::duration<double>(t1 - chrNow).count() << "\n";
					break;
				}

				case GameMsg::Game_UpdatePlayer:
				{
					std::cout << "Player Update Received\n";
					break;
				}
				}
			}
			else
			{
				// Send a ping request
				olc::net::message<GameMsg> msg;
				msg.header.id = GameMsg::Client_RegisterWithServer;
				c.Send(msg);

				// Send player update
				c.SendPlayerUpdate();

				std::this_thread::sleep_for(std::chrono::seconds(2));
			}
		}
		else
		{
			bShutdown = true;
		}
	}

	return 0;
}
