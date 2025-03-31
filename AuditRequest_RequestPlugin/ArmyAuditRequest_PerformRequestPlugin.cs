using DataRequestPipeline.Core;       // Contains RequestContext
using DataRequestPipeline.DataContracts;
using Microsoft.Data.SqlClient;        // NuGet package Microsoft.Data.SqlClient
using System.ComponentModel.Composition;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RequestPlugins
{
    [Export(typeof(IPerformRequestPlugin))]
    public class ArmyAuditRequest_PerformRequestPlugin : IPerformRequestPlugin
    {
        public async Task ExecuteAsync(RequestContext context)
        {
            Console.WriteLine("AuditRequestPlugin: Starting SQL execution stage...");

            // Define the path to the JSON config file.
            // Adjust this path as necessary; here we assume it's in the same folder as the DLL.
            string configFilePath = "plugins\\PerformRequest\\sqlRequestConfig.json";
            if (!File.Exists(configFilePath))
            {
                Logger.Log("SQL Request Config not found!");
                throw new FileNotFoundException("SQL Request config file not found.", configFilePath);
            }

            // Read and deserialize the config.
            string json = await File.ReadAllTextAsync(configFilePath);
            SqlRequestConfig config = JsonSerializer.Deserialize<SqlRequestConfig>(json);
            if (config == null)
            {
                throw new Exception("Failed to deserialize SQL Request config.");
            }

            // Use the connection string from the config if provided,
            // otherwise fallback to the connection string from the context.
            string connectionString = !string.IsNullOrWhiteSpace(config.ConnectionString)
                ? config.ConnectionString
                : context.InputConnectionString;

            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new Exception("No connection string provided in config or context.");
            }

            Logger.Log("Connection String : " + connectionString);

            // Execute each SQL file in the order specified.
            foreach (var sqlFile in config.SqlFiles)
            {
                Console.WriteLine($"AuditRequestPlugin: Executing SQL file: {sqlFile}");


                if (!File.Exists(sqlFile))
                {
                    Console.WriteLine($"{Path.GetFullPath(sqlFile)} does not exist");
                    throw new FileNotFoundException("SQL file not found", Path.GetFullPath(sqlFile));
                }
                Logger.Log("AuditRequestPlugin : Executing " + sqlFile);
                string sqlCommandText = await File.ReadAllTextAsync(sqlFile);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync();
                    using (SqlCommand command = new SqlCommand(sqlCommandText, connection))
                    {
                        command.CommandTimeout = 0; // Optional: disable timeout if needed.
                        int rowsAffected = await command.ExecuteNonQueryAsync();
                        Console.WriteLine($"AuditRequestPlugin: Executed {sqlFile}. Rows affected: {rowsAffected}");
                        Logger.Log($"AuditRequestPlugin: Executed {sqlFile}. Rows affected: {rowsAffected}");
                    }
                }
            }

            Console.WriteLine("AuditRequestPlugin: All SQL files executed successfully.");
        }

        public async Task RollbackAsync(RequestContext context)
        {
            Console.WriteLine("AuditRequestPlugin: Rolling back SQL execution...");
            // Add rollback logic here (e.g., dropping tables or reverting changes)
            await Task.Delay(250); // Simulate asynchronous rollback work.
            Console.WriteLine("AuditRequestPlugin: Rollback complete.");
        }
    }

    // Helper class to represent the SQL request configuration.
    public class SqlRequestConfig
    {
        [JsonPropertyName("connectionString")]
        public string ConnectionString { get; set; }

        [JsonPropertyName("sqlFiles")]
        public string[] SqlFiles { get; set; }
    }
}
