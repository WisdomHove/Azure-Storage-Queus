using System; // Namespace for Console output
using System.Configuration; // Namespace for ConfigurationManager
using System.Threading.Tasks; // Namespace for Task
using Azure.Storage.Queues; // Namespace for Queue storage types
using Azure.Storage.Queues.Models; // Namespace for PeekedMessage

namespace Storage_Queus
{
    internal class Program
    {
        static void Main(string[] args)
        {

            //-------------------------------------------------
            // Create the queue service client
            //-------------------------------------------------
            public void CreateQueueClient(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to create and manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);
            }

            //-------------------------------------------------
            // Create a message queue
            //-------------------------------------------------
            public bool CreateQueue(string queueName)
            {
                try
                {
                    // Get the connection string from app settings
                    string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                    // Instantiate a QueueClient which will be used to create and manipulate the queue
                    QueueClient queueClient = new QueueClient(connectionString, queueName);

                    // Create the queue
                    queueClient.CreateIfNotExists();

                    if (queueClient.Exists())
                    {
                        Console.WriteLine($"Queue created: '{queueClient.Name}'");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception: {ex.Message}\n\n");
                    Console.WriteLine($"Make sure the Azurite storage emulator running and try again.");
                    return false;
                }
            }

            //-------------------------------------------------
            // Insert a message into a queue
            //-------------------------------------------------
            public void InsertMessage(string queueName, string message)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to create and manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                // Create the queue if it doesn't already exist
                queueClient.CreateIfNotExists();

                if (queueClient.Exists())
                {
                    // Send a message to the queue
                    queueClient.SendMessage(message);
                }

                Console.WriteLine($"Inserted: {message}");
            }

            //-------------------------------------------------
            // Peek at a message in the queue
            //-------------------------------------------------
            public void PeekMessage(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    // Peek at the next message
                    PeekedMessage[] peekedMessage = queueClient.PeekMessages();

                    // Display the message
                    Console.WriteLine($"Peeked message: '{peekedMessage[0].Body}'");
                }
            }

            //-------------------------------------------------
            // Update an existing message in the queue
            //-------------------------------------------------
            public void UpdateMessage(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    // Get the message from the queue
                    QueueMessage[] message = queueClient.ReceiveMessages();

                    // Update the message contents
                    queueClient.UpdateMessage(message[0].MessageId,
                            message[0].PopReceipt,
                            "Updated contents",
                            TimeSpan.FromSeconds(60.0)  // Make it invisible for another 60 seconds
                        );
                }
            }

            //-------------------------------------------------
            // Process and remove a message from the queue
            //-------------------------------------------------
            public void DequeueMessage(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    // Get the next message
                    QueueMessage[] retrievedMessage = queueClient.ReceiveMessages();

                    // Process (i.e. print) the message in less than 30 seconds
                    Console.WriteLine($"Dequeued message: '{retrievedMessage[0].Body}'");

                    // Delete the message
                    queueClient.DeleteMessage(retrievedMessage[0].MessageId, retrievedMessage[0].PopReceipt);
                }
            }

            //-------------------------------------------------
            // Perform queue operations asynchronously
            //-------------------------------------------------
            public async Task QueueAsync(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                // Create the queue if it doesn't already exist
                await queueClient.CreateIfNotExistsAsync();

                if (await queueClient.ExistsAsync())
                {
                    Console.WriteLine($"Queue '{queueClient.Name}' created");
                }
                else
                {
                    Console.WriteLine($"Queue '{queueClient.Name}' exists");
                }

                // Async enqueue the message
                await queueClient.SendMessageAsync("Hello, World");
                Console.WriteLine($"Message added");

                // Async receive the message
                QueueMessage[] retrievedMessage = await queueClient.ReceiveMessagesAsync();
                Console.WriteLine($"Retrieved message with content '{retrievedMessage[0].Body}'");

                // Async delete the message
                await queueClient.DeleteMessageAsync(retrievedMessage[0].MessageId, retrievedMessage[0].PopReceipt);
                Console.WriteLine($"Deleted message: '{retrievedMessage[0].Body}'");

                // Async delete the queue
                await queueClient.DeleteAsync();
                Console.WriteLine($"Deleted queue: '{queueClient.Name}'");
            }

            //-----------------------------------------------------
            // Process and remove multiple messages from the queue
            //-----------------------------------------------------
            public void DequeueMessages(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    // Receive and process 20 messages
                    QueueMessage[] receivedMessages = queueClient.ReceiveMessages(20, TimeSpan.FromMinutes(5));

                    foreach (QueueMessage message in receivedMessages)
                    {
                        // Process (i.e. print) the messages in less than 5 minutes
                        Console.WriteLine($"De-queued message: '{message.Body}'");

                        // Delete the message
                        queueClient.DeleteMessage(message.MessageId, message.PopReceipt);
                    }
                }
            }

            //-----------------------------------------------------
            // Get the approximate number of messages in the queue
            //-----------------------------------------------------
            public void GetQueueLength(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    QueueProperties properties = queueClient.GetProperties();

                    // Retrieve the cached approximate message count.
                    int cachedMessagesCount = properties.ApproximateMessagesCount;

                    // Display number of messages.
                    Console.WriteLine($"Number of messages in queue: {cachedMessagesCount}");
                }
            }

            //-------------------------------------------------
            // Delete the queue
            //-------------------------------------------------
            public void DeleteQueue(string queueName)
            {
                // Get the connection string from app settings
                string connectionString = ConfigurationManager.AppSettings["StorageConnectionString"];

                // Instantiate a QueueClient which will be used to manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);

                if (queueClient.Exists())
                {
                    // Delete the queue
                    queueClient.Delete();
                }

                Console.WriteLine($"Queue deleted: '{queueClient.Name}'");
            }

        }

    }
}
