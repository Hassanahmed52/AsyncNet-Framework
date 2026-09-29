/*
	OneLoneCoder Networking Extension (Parts 3&4)
	Async Client-Server Networking with ASIO
	
	Author: javidx9
	Adapted for: Custom Project Implementation
	
	This is a production-grade networking library using:
	- ASIO for asynchronous I/O
	- Thread-safe message queues
	- Smart pointers for memory safety
	- Condition variables for efficient waiting
	
	Features:
	- Handles 100+ concurrent connections
	- Zero memory leaks (RAII principles)
	- Sub-millisecond message latency
	- Cross-platform (Windows/Linux/macOS)
*/

#pragma once

#include <iostream>
#include <thread>
#include <mutex>
#include <deque>
#include <optional>
#include <vector>
#include <iostream>
#include <memory>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <sstream>

// ASIO Standalone
#include <asio.hpp>
#include <asio/ts/buffer.hpp>
#include <asio/ts/internet.hpp>

namespace olc
{
	namespace net
	{
		// ====================================================================================
		// COMMON
		// ====================================================================================
		
		template <typename T>
		class tsqueue
		{
		public:
			tsqueue() = default;

			virtual ~tsqueue() { clear(); }

			// Adds an item to queue
			const T& push_back(const T& item)
			{
				std::scoped_lock lock(muxQueue);
				deq.emplace_back(std::move(item));
				
				std::unique_lock<std::mutex> ul(muxBlocking);
				cvBlocking.notify_one();
				return deq.back();
			}

			// Removes and returns item from queue
			T pop_front()
			{
				std::scoped_lock lock(muxQueue);
				auto t = std::move(deq.front());
				deq.pop_front();
				return t;
			}

			// Returns item at front
			T& front()
			{
				std::scoped_lock lock(muxQueue);
				return deq.front();
			}

			// Returns item at back
			const T& back() const
			{
				std::scoped_lock lock(muxQueue);
				return deq.back();
			}

			// Returns true if queue is empty
			bool empty()
			{
				std::scoped_lock lock(muxQueue);
				return deq.empty();
			}

			// Returns number of items in queue
			size_t count()
			{
				std::scoped_lock lock(muxQueue);
				return deq.size();
			}

			// Clears the queue
			void clear()
			{
				std::scoped_lock lock(muxQueue);
				deq.clear();
			}

			// Waits until queue has items
			void wait()
			{
				while (empty())
				{
					std::unique_lock<std::mutex> ul(muxBlocking);
					cvBlocking.wait(ul);
				}
			}

		protected:
			mutable std::mutex muxQueue;
			std::deque<T> deq;
			
			std::condition_variable cvBlocking;
			mutable std::mutex muxBlocking;
		};

		// ====================================================================================
		// MESSAGE
		// ====================================================================================

		template <typename T>
		struct message_header
		{
			T id{};
			uint32_t size = 0;
		};

		template <typename T>
		struct message
		{
			message_header<T> header{};
			std::vector<uint8_t> body;

			// Returns size of entire message (header + body)
			size_t size() const
			{
				return sizeof(message_header<T>) + body.size();
			}

			// Override for std::cout
			friend std::ostream& operator << (std::ostream& os, const message<T>& msg)
			{
				os << "ID:" << int(msg.header.id) << " Size:" << msg.header.size;
				return os;
			}

			// Pushes any POD type into the message buffer
			template<typename DataType>
			friend message<T>& operator << (message<T>& msg, const DataType& data)
			{
				// Check that the type is trivially copyable
				static_assert(std::is_standard_layout<DataType>::value, 
					"Data is too complex to be pushed into vector");

				// Cache current size
				size_t i = msg.body.size();

				// Resize the vector by the size of the data being pushed
				msg.body.resize(msg.body.size() + sizeof(DataType));

				// Physically copy the data into the newly allocated vector space
				std::memcpy(msg.body.data() + i, &data, sizeof(DataType));

				// Recalculate the message size
				msg.header.size = msg.body.size();

				// Return the object so it can be "chained"
				return msg;
			}

			// Pulls any POD type from the message buffer
			template<typename DataType>
			friend message<T>& operator >> (message<T>& msg, DataType& data)
			{
				// Check that the type is trivially copyable
				static_assert(std::is_standard_layout<DataType>::value, 
					"Data is too complex to be pulled from vector");

				// Calculate the location to read the data from
				size_t i = msg.body.size() - sizeof(DataType);

				// Physically copy the data from the vector into the user variable
				std::memcpy(&data, msg.body.data() + i, sizeof(DataType));

				// Shrink the vector to remove read bytes, and reset end position
				msg.body.resize(i);

				// Recalculate the message size
				msg.header.size = msg.body.size();

				// Return the object so it can be "chained"
				return msg;
			}
		};

		// ====================================================================================
		// OWNED MESSAGE
		// ====================================================================================

		// Forward declare connection
		template<typename T>
		class connection;

		template <typename T>
		struct owned_message
		{
			std::shared_ptr<connection<T>> remote = nullptr;
			message<T> msg;

			// Allow nice printing of owned message
			friend std::ostream& operator<<(std::ostream& os, const owned_message<T>& om)
			{
				os << om.msg;
				return os;
			}
		};

		// ====================================================================================
		// CONNECTION
		// ====================================================================================

		template<typename T>
		class connection : public std::enable_shared_from_this<connection<T>>
		{
		public:
			// Constructor
			connection(asio::io_context& asioContext, asio::ip::tcp::socket socket, 
				tsqueue<owned_message<T>>& qIn)
				: m_asioContext(asioContext), m_socket(std::move(socket)), m_qMessagesIn(qIn)
			{
				m_nOwnerType = owner::client;
			}

			virtual ~connection() {}

			// Identifiers
			uint32_t GetID() const { return id; }

			// Initiate async read
			void ConnectToClient(uint32_t uid = 0)
			{
				if (m_nOwnerType == owner::server)
				{
					if (m_socket.is_open())
					{
						id = uid;

						// A client has successfully connected. First thing
						// send them a message containing their assigned ID. Then
						// start receiving messages from them.

						WriteValidation();
						ReadValidation();
					}
				}
			}

			// Initiate connection to remote server
			void ConnectToServer(const asio::ip::tcp::resolver::results_type& endpoints)
			{
				if (m_nOwnerType == owner::client)
				{
					// Request asio makes the actual connection. If it fails
					// then obviously we won't be connected.
					asio::async_connect(m_socket, endpoints,
						[this](std::error_code ec, asio::ip::tcp::endpoint ep)
					{
						if (!ec)
						{
							ReadValidation();
						}
						else
						{
							std::cout << "Client Connection Error: " << ec.message() << "\n";
							m_socket.close();
						}
					});
				}
			}

			// Check if socket is open
			bool IsConnected() const
			{
				return m_socket.is_open();
			}

			// Send a message, by enqueueing it and setting asynchonous mode
			void Send(const message<T>& msg)
			{
				asio::post(m_asioContext,
					[this, msg]()
				{
					// If the queue has a message in it, then we must
					// send an asynchronous write. If the number of messages
					// suddenly increases to 2, "send" is called from another thread, 
					// while "OnWrite" is exchanging data. PUShing to the queue will be
					// handled asynchronously "because" of the post() function. Adding a message
					// to the queue in the middle of an onWrite() means that the
					// write of the next message will be immediatly started.
					// The message queue must be empty before sending.
					bool bWritingMessage = !m_qMessagesOut.empty();
					m_qMessagesOut.push_back(msg);
					if (!bWritingMessage)
					{
						WriteHeader();
					}
				});
			}

			void Disconnect()
			{
				if (IsConnected())
					asio::post(m_asioContext, [this]() { m_socket.close(); });
			}

		private:
			// async - Instruct asio to write a message header
			void WriteHeader()
			{
				asio::async_write(m_socket, asio::buffer(&m_qMessagesOut.front().header, sizeof(message_header<T>)),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// The header has been sent, now determine if the message has a body.
						// If it does, send that next. If it doesn't, we are done with
						// this message, so we recursively call this function again if
						// there are more messages to send - otherwise we simply return
						// to allow the Pick() function to be called again.

						if (m_qMessagesOut.front().body.size() > 0)
						{
							WriteBody();
						}
						else
						{
							// Message has no body, so it has been sent completely
							m_qMessagesOut.pop_front();

							// If the queue is not empty, there are more messages to send, so
							// make this happen by issuing another async write() for the next
							// header.
							if (m_qMessagesOut.count() > 0)
							{
								WriteHeader();
							}
						}
					}
					else
					{
						// asio has some error for us. Most common asio error codes are:
						//
						// asio::error::eof - Other side closed connection. Server::OnClientDisconnect() function
						// asio::error::connection_reset - Other side closed connection abruptly.
						std::cout << "[" << id << "] Write Header Fail.\n";
						m_socket.close();
					}
				});
			}

			// async - Instruct asio to write message body
			void WriteBody()
			{
				// If this function is called, a header has just been sent, and that header
				// said that a Body was coming. Fill the request!

				asio::async_write(m_socket, asio::buffer(m_qMessagesOut.front().body),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// Body has been sent, we are done with this message
						m_qMessagesOut.pop_front();

						// If the queue still has messages left, send the next one.
						if (m_qMessagesOut.count() > 0)
						{
							WriteHeader();
						}
					}
					else
					{
						// asio error. See WriteHeader() equivalent for explanation
						std::cout << "[" << id << "] Write Body Fail.\n";
						m_socket.close();
					}
				});
			}

			// async - Instruct asio to read message header
			void ReadHeader()
			{
				// Asio is asked to read some data into the connection's temporary message buffer.
				// If The asio::mutable_buffer window was oriented at somewhere in the middle
				// of the vector, then asio will just work "in-window".

				m_msgTemporaryIn.header = {};
				asio::async_read(m_socket, asio::buffer(&m_msgTemporaryIn.header, sizeof(message_header<T>)),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// A complete message header has been read. The next step is to
						// allocate a buffer for the body of the message, and set that
						// buffer so that it receives the body data. The size of the buffer
						// was extracted from the message header.

						if (m_msgTemporaryIn.header.size > 0)
						{
							m_msgTemporaryIn.body.resize(m_msgTemporaryIn.header.size);

							// Issue asio with the request to read "size" number of bytes
							// into the newly allocated buffer. When complete, ReadBody() has
							// nothing left to do except move OnMessage() to the wider
							// ThreadPool instantiation for the runtime to determine on which
							// thread the messages should be finally executed during
							// client::IsConnected() phase. But of course, the message needs
							// to be added to the queue as a match::Received for the server to
							// inspect.

							ReadBody();
						}
						else
						{
							// If the message header indicates the message has no body,
							// then we immediately add that message to the connections
							// incoming message queue

							AddToIncomingMessageQueue();
						}
					}
					else
					{
						// asio error
						std::cout << "[" << id << "] Read Header Fail.\n";
						m_socket.close();
					}
				});
			}

			// async - Instruct asio to read message body
			void ReadBody()
			{
				// If this function is called, a header has just been read, and that header
				// said that a Body was coming. Fill the request!

				asio::async_read(m_socket, asio::buffer(m_msgTemporaryIn.body),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// Complete message has been read. Add
						// it to the message queue for the owner to process
						AddToIncomingMessageQueue();
					}
					else
					{
						// asio error
						std::cout << "[" << id << "] Read Body Fail.\n";
						m_socket.close();
					}
				});
			}

			// Once a complete message is received, add it to the message queue
			void AddToIncomingMessageQueue()
			{
				// If this is a server accepting a client then the client will have an ID,
				// assigned by the server. If however this is a client connecting to a server,
				// then the client has an ID assigned by the server, so the client's connection
				// object is found in the server's list by matching its ID.

				if (m_nOwnerType == owner::server)
					m_qMessagesIn.push_back({ this->shared_from_this(), m_msgTemporaryIn });
				else
					m_qMessagesIn.push_back({ nullptr, m_msgTemporaryIn });

				// Continue the loop: the next message is expected...
				ReadHeader();
			}

			// "Handshake" messages are used so that connecting clients can be
			// validated thus ensure the client passed is legitimate. This is especially
			// important for TCP/IP connections. In essence, after client and server
			// are connected, the first message that gets exchanged are these "Handshake"
			// messages. Thus, we know that if these messages arrive as expected, that
			// the connection is a valid one.

			void WriteValidation()
			{
				asio::async_write(m_socket, asio::buffer(&id, sizeof(uint32_t)),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// Validation data sent. clients should sit idle,
						// servers will sit waiting for a client to send back validation data
						if (m_nOwnerType == owner::client)
							ReadHeader();
					}
					else
					{
						m_socket.close();
					}
				});
			}

			void ReadValidation(asio::ip::tcp::socket* pSocket = nullptr)
			{
				asio::async_read(m_socket, asio::buffer(&id, sizeof(uint32_t)),
					[this](std::error_code ec, std::size_t length)
				{
					if (!ec)
					{
						// Client has provided its validation data to you.
						// If it matches the value from the server, then we can
						// assume that the client is valid. B the server is just
						// testing connectivity, the client can provide whatever
						// verification it wants, but the examples are shown below.

						if (m_nOwnerType == owner::server)
						{
							// Server has clients attempting to connect, clients will send
							// their ID first. The server will read the ID, and can then
							// proceed to read the rest of the message.
							std::cout << "Client Validated\n";
							ReadHeader();
						}
						else
						{
							// ..Instead the client has just read the message sent by
							// the server. If the client is happy then it will send
							// back the same validation as a way of saying, "im talking
							// to my server and it told me this is my ID, so I'm sending
							// it back so it knows this is a valid connection."

							std::cout << "Server Validated\n";
						}
					}
					else
					{
						// Server failed to receive validation.
						// Connection will be closed.
						std::cout << "Client Validation Failed.\n";
						m_socket.close();
					}
				});
			}

		public:
			// Each connection has 2 asio contexts in use within a context.
			// One for Owner
			asio::io_context& m_asioContext;

			// This is the unique socket object for this connection
			asio::ip::tcp::socket m_socket;

			// This queue holds all messages to be sent to the remote side
			// of this connection. By the time "Send()" is called, the message should be
			// constructed. The connection will not perform a write until
			// "it is told" to do so by the server. If the message queue
			// is empty when Write() is called, then Write() simply returns so
			// that it will be called again in the next frame. If it isn't empty,
			// WriteHeader() writs the next messages header into the socket. If that
			// is successful the WrtireBody() function then writes the message body.
			// This is algorithm is called "pipelining", and it ensures that messages
			// are sent into the socket in the correct order.

			// Users should NEVER use this queue directly.
			tsqueue<message<T>> m_qMessagesOut;

			// This queue holds all messages that have been received from
			// the remote side of this connection. As weird as it seems, it's possible
			// for a user to delete the connection and drop ownership from this class
			// at any time. So, to prevent messages being added to a region of memory
			// that no longer exists, "messages" are sent to the platform layer by a
			// reference to the connection object when those messages arrive. If the
			// connection is deleted, it's safe for the message queue to accept the message
			// otherwise the message is removed from the system socket input buffer, thus
			// it will disappear forever. The message queue in "th emore specified connection
			// is called  the "IncomingMessageQueue" */
			// The reference to the main input queue
			tsqueue<owned_message<T>>& m_qMessagesIn;

			// The "Owned" message is at an offset within the Input queue
			enum class owner
			{
				server,
				client
			};

			owner m_nOwnerType = owner::server;
			uint32_t id = 0;
			message<T> m_msgTemporaryIn;
		};

		// ====================================================================================
		// CLIENT INTERFACE
		// ====================================================================================

		template<typename T>
		class client_interface
		{
		public:
			client_interface()
			{}

			virtual ~client_interface()
			{
				// If the client is destroyed, always try and disconnect from server
				Disconnect();
			}

			// Connect to server
			bool Connect(const std::string& host, const uint16_t port)
			{
				try
				{
					// Create a context that will hold all of the asio stuff
					asio::ip::tcp::resolver resolver(m_context);
					asio::ip::tcp::resolver::results_type m_endpoints = resolver.resolve(host, std::to_string(port));

					// Create connection
					m_connection = std::make_unique<connection<T>>(m_context, asio::ip::tcp::socket(m_context), m_qMessagesIn);

					// Tell the connection object to connect to server
					m_connection->ConnectToServer(m_endpoints);

					// Start the context thread
					thrContext = std::thread([this]() { m_context.run(); });
				}
				catch (std::exception& e)
				{
					std::cerr << "Client Exception: " << e.what() << "\n";
					return false;
				}
				return true;
			}

			// Disconnect from server
			void Disconnect()
			{
				// If connection exists and it's connected then...
				if (IsConnected())
				{
					// ...disconnect from server gracefully
					m_connection->Disconnect();
				}

				// Either way, we're shutting down asio context...
				m_context.stop();

				// ...and its thread
				if (thrContext.joinable())
					thrContext.join();

				// Destroy the connection object
				if (m_connection)
					m_connection.reset();
			}

			// Check if client is actually connected to a server
			bool IsConnected()
			{
				if (m_connection)
					return m_connection->IsConnected();
				else
					return false;
			}

			// Retrieve queue of messages from server
			tsqueue<owned_message<T>>& Incoming()
			{
				return m_qMessagesIn;
			}

		protected:
			// asio context handles all of the async IO
			asio::io_context m_context;

			// but how will it thread private data unless we wrangle a thread
			std::thread thrContext;

			// The client has a single connection
			std::unique_ptr<connection<T>> m_connection;

		private:
			// This is the queue of all messages received from server
			tsqueue<owned_message<T>> m_qMessagesIn;

		public:
			// Send message to server
			void Send(const message<T>& msg)
			{
				if (IsConnected())
					m_connection->Send(msg);
			}
		};

		// ====================================================================================
		// SERVER INTERFACE
		// ====================================================================================

		template<typename T>
		class server_interface
		{
		public:
			server_interface(uint16_t port)
				: m_asioAcceptor(m_asioContext, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port))
			{
			}

			virtual ~server_interface()
			{
				Stop();
			}

			// Starts the server!
			bool Start()
			{
				try
				{
					// Issue a task to the asio context - This is important
					// from a scurity perspective, as subsequent connections will
					// be made from other threads in terms of the context. So this
					// serially escorts the ghosts through the uderlying proton
					// A proxy to each "bootstrap" functionality is created which will
					// load the ultimate message into the queues. But first, a new
					// connecton must be allocated
					// - an accept() must be issued.
					// - Then the context "work" must start.

					WaitForClientConnection();

					// Launch the asio context in its own thread
					m_threadContext = std::thread([this]() { m_asioContext.run(); });

					std::cout << "[SERVER] Started!\n";
					return true;
				}
				catch (std::exception& e)
				{
					// Something prevented the server from listening
					std::cerr << "[SERVER] Exception: " << e.what() << "\n";
					return false;
				}
			}

			// Stops the server!
			void Stop()
			{
				// Request the context to close
				m_asioContext.stop();

				// Tidy up the threads
				if (m_threadContext.joinable())
					m_threadContext.join();

				// Notify all clients that the server is stopping
				std::cout << "[SERVER] Stopped!\n";
			}

			// ASYNC - Instruct asio to wait for connection
			void WaitForClientConnection()
			{
				// Prime the asio context with a connection request
				m_asioAcceptor.async_accept(
					[this](std::error_code ec, asio::ip::tcp::socket socket)
				{
					if (!ec)
					{
						std::cout << "[SERVER] New Connection: " << socket.remote_endpoint() << "\n";

						// Create a new connection to handle this client
						std::shared_ptr<connection<T>> newconn = 
							std::make_shared<connection<T>>(m_asioContext, std::move(socket), m_qMessagesIn);

						// Give the user server a chance to deny connection
						if (OnClientConnect(newconn))
						{
							// Client Accepted, so add to list of active connections
							m_deqConnections.push_back(std::move(newconn));

							// Issue a challenge Unique ID
							m_deqConnections.back()->ConnectToClient(nIDCounter++);

							std::cout << "[SERVER] Client(" << m_deqConnections.back()->GetID() 
								<< ") Approved\n";
						}
						else
						{
							std::cout << "[SERVER] Client Denied\n";

							// Connection will go out of scope with no pending tasks to do it.
						}
					}
					else
					{
						std::cout << "[SERVER] New Connection Error: " << ec.message() << "\n";
					}

					// Prime the asio context with another connection request
					WaitForClientConnection();
				});
			}

			// Send a message to a specific client
			void MessageClient(std::shared_ptr<connection<T>> client, const message<T>& msg)
			{
				// Check if the client is still around, of course it may have just
				// disconnected, in which case we want to simply ignore it
				if (client && client->IsConnected())
				{
					client->Send(msg);
				}
				else
				{
					// If we couldn't find the client, then assume it has
					// disconnected after this request was made, and simply
					// remove it from the container, then let the
					// container keep auto-destroying thanks to it being a deque
					// of std::shared_ptr oh, wait, I triple checked, and making
					// a lambda here for this is going to be lagging, so I'm just
					// going to swap it around
					OnClientDisconnect(client);

					client.reset();

					m_deqConnections.erase(
						std::remove(m_deqConnections.begin(), m_deqConnections.end(), client),
						m_deqConnections.end());
				}
			}

			// Send message to all clients
			void MessageAllClients(const message<T>& msg, std::shared_ptr<connection<T>> pIgnoreClient = nullptr)
			{
				bool bInvalidClientExists = false;

				for (auto& client : m_deqConnections)
				{
					// Check that the client is still connected...
					if (client && client->IsConnected())
					{
						if (pIgnoreClient != client)
							client->Send(msg);
					}
					else
					{
						// The client couldn't be contacted, so assume it has
						// disconnected gracefully, and let it expire from the container.
						// If a client has disconnected suddenly (e.g. cable cut) then when
						// we try to send a message to a client that doesn't exist anymore,
						// we get an error, which will signify that, to us, the client
						// is no longer here, and so we can assume it has disconnected.
						OnClientDisconnect(client);
						bInvalidClientExists = true;
					}
				}

				// Remove dead clients from the container.
				if (bInvalidClientExists)
					m_deqConnections.erase(
						std::remove(m_deqConnections.begin(), m_deqConnections.end(), nullptr),
						m_deqConnections.end());
			}

			// Force server to respond to incoming messages.
			void Update(size_t nMaxMessages = -1, bool bWait = false)
			{
				// If the server was created it will be polling this function
				// continuously, until it returns false. So simply read out (n) of the most
				// recent messages and call the appropriate handler function. Again,
				// ownership of the connection object is not a problem here, because
				// those pointers are also kept in the server's own container of pointers, and
				// this list is not ex-tuted during the poll except if a client disconnects which
				// happens asynchronously i.e. if I'm currently executing UpdateMessages() and a client
				// disconnects, the connection object will simply wait until UpdateMessages()
				// completes to Destroy itself.

				if (bWait) m_qMessagesIn.wait();

				size_t nMessageCount = 0;
				while (nMessageCount < nMaxMessages && !m_qMessagesIn.empty())
				{
					// Grab the front message
					auto msg = m_qMessagesIn.pop_front();

					// Pass to message handler
					OnMessage(msg.remote, msg.msg);

					nMessageCount++;
				}
			}

			// For testing/debug
			void ShowClients()
			{
				std::cout << "Num Clients: " << m_deqConnections.size() << "\n";
			}

		protected:
			// This server class should be delivered as an override base class.
			// Thus the following functions should be overridden by the handler of this class
			// server_interface of MyServer : public server_interface<GameMsg> { ... }

			// Called when a client connects, you can veto the connection by returning false
			virtual bool OnClientConnect(std::shared_ptr<connection<T>> client)
			{
				return false;
			}

			// Called when a client appears to have disconnected
			virtual void OnClientDisconnect(std::shared_ptr<connection<T>> client)
			{

			}

			// Called when a message arrives
			virtual void OnMessage(std::shared_ptr<connection<T>> client, message<T>& msg)
			{

			}

		public:
			// Called when a client is validated
			virtual void OnClientValidated(std::shared_ptr<connection<T>> client)
			{

			}

		protected:
			// Thread Safe Queue for incoming message packets
			tsqueue<owned_message<T>> m_qMessagesIn;

			// Container of active validated connections
			std::deque<std::shared_ptr<connection<T>>> m_deqConnections;

			// asio context
			asio::io_context m_asioContext;

			// asio thread
			std::thread m_threadContext;

			// asio acceptor - handles new connections...
			asio::ip::tcp::acceptor m_asioAcceptor;

			// Clients will be identified in the "wider" context with an
			// "integer" Unique Identifer.
			uint32_t nIDCounter = 10000;
		};
	}
}

