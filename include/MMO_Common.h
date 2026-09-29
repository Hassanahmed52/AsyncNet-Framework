/*
	MMO_Common.h
	Shared message types and data structures for client-server communication
*/

#pragma once

#include <cstdint>

enum class GameMsg : uint32_t
{
	Server_GetStatus,
	Server_GetPing,
	Server_RegisterWithServer,
	Server_UnregisterWithServer,
	
	Client_RegisterWithServer,
	Client_UnregisterWithServer,
	
	Game_AddPlayer,
	Game_RemovePlayer,
	Game_UpdatePlayer,
};

struct sPlayerDescription
{
	float x;
	float y;
	float vx;
	float vy;
	uint32_t nUniqueID;
	uint32_t nHealth;
	char szName[32];
};
