using CAFRS.Encryption.FIPS;
using DataRequestPipeline.Core;
using DataRequestPipeline.DataContracts;      // Contains ExportContext
using Microsoft.Data.SqlClient;
using System.ComponentModel.Composition;
using System.Text.Json;

namespace ExportPlugins
{
    [Export(typeof(IExportPlugin))]
    public class ExportPluginA : IExportPlugin
    {
        public async Task ExecuteAsync(ExportContext context)
        {
            // Parse command line options.
            string configFile = null;
            CAFRSAESEncryptionEngine m_EncryptionEngine = new CAFRSAESEncryptionEngine();
            DecryptDatabases(m_EncryptionEngine, configFile);
        }
        private void DecryptDatabases(CAFRSAESEncryptionEngine m_EncryptionEngine, string jsonFilePath)
        {


            if (!File.Exists(jsonFilePath))
            {
                Console.WriteLine("Config file not found: " + jsonFilePath);
                return;
            }

            try
            {
                // Read and deserialize the JSON configuration.
                string jsonContent = File.ReadAllText(jsonFilePath);
                var config = JsonSerializer.Deserialize<Config>(jsonContent);

                if (config == null || config.Databases == null || config.Databases.Count == 0)
                {
                    Console.WriteLine("No database configurations found.");
                    return;
                }

                // Iterate through each database configuration.
                foreach (var db in config.Databases)
                {
                    Console.WriteLine("-------------------------------------------------");
                    Console.WriteLine("Processing database with connection string:");
                    Console.WriteLine(db.ConnectionString);

                    // Iterate through each table within the current database.
                    foreach (var table in db.Tables)
                    {
                        // Use the table-specific query if provided; otherwise, use the database-level query.
                        // If neither is provided, default to SELECT * from the table.
                        string query = !string.IsNullOrWhiteSpace(table.Query) ? table.Query :
                                       (!string.IsNullOrWhiteSpace(db.Query) ? db.Query : $"SELECT * FROM {table.TableName}");

                        Console.WriteLine("-------------------------------------------------");
                        Console.WriteLine("Table: " + table.TableName);
                        Console.WriteLine("Output File: " + table.OutputFile);
                        Console.WriteLine("Query: " + query);

                        ProcessDatabaseTable(db, table, m_EncryptionEngine);

                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred while reading the config file: " + ex.Message);
            }
        }

        /// <summary>
        /// Processes a single table from the database using the provided configuration.
        /// </summary>
        /// <param name="dbConfig">The database configuration including the connection string and optional default query.</param>
        /// <param name="tableConfig">The table configuration containing the table name, output file name, and an optional query.</param>
        private void ProcessDatabaseTable(DatabaseConfig dbConfig, TableConfig tableConfig, CAFRSAESEncryptionEngine encEngine)
        {
            // Determine which query to use.
            string query;
            if (!string.IsNullOrWhiteSpace(tableConfig.Query))
            {
                query = tableConfig.Query;
            }
            else if (!string.IsNullOrWhiteSpace(dbConfig.Query))
            {
                query = dbConfig.Query;
            }
            else
            {
                query = $"SELECT * FROM {tableConfig.TableName}";
            }

            Console.WriteLine("Processing table: " + tableConfig.TableName);
            Console.WriteLine("Using query: " + query);

            List<string> errorLog = new List<string>();
            List<bool> decryptionEnabledColumns = new List<bool>();

            try
            {
                using (SqlConnection connection = new SqlConnection(dbConfig.ConnectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand(query, connection))
                    using (SqlDataReader reader = command.ExecuteReader())
                    using (StreamWriter writer = new StreamWriter(tableConfig.OutputFile))
                    {
                        // Write header row.
                        int fieldCount = reader.FieldCount;
                        string[] headers = new string[fieldCount];
                        for (int i = 0; i < fieldCount; i++)
                        {
                            headers[i] = reader.GetName(i);
                        }
                        string headerRow = string.Join(",", headers);
                        writer.WriteLine(headerRow);
                        Console.WriteLine("Header: " + headerRow);

                        int rowNumber = 1; // Counter for data rows.
                        bool detectionRowProcessed = false;

                        while (reader.Read())
                        {
                            string[] inputColumns = new string[fieldCount];
                            string[] outputColumns = new string[fieldCount];

                            // Retrieve all column values as strings.
                            for (int i = 0; i < fieldCount; i++)
                            {
                                inputColumns[i] = reader.GetValue(i)?.ToString();
                            }

                            if (!detectionRowProcessed)
                            {
                                // First data row: try decrypting every column.
                                for (int i = 0; i < fieldCount; i++)
                                {
                                    try
                                    {
                                        outputColumns[i] = TryDecrypt(inputColumns[i], encEngine);
                                        decryptionEnabledColumns.Add(true);
                                    }
                                    catch (Exception)
                                    {
                                        outputColumns[i] = inputColumns[i];
                                        decryptionEnabledColumns.Add(false);
                                    }
                                }
                                detectionRowProcessed = true;
                            }
                            else
                            {
                                // For subsequent rows, only attempt decryption on columns flagged as encrypted.
                                for (int i = 0; i < fieldCount; i++)
                                {
                                    if (i < decryptionEnabledColumns.Count && decryptionEnabledColumns[i])
                                    {
                                        try
                                        {
                                            outputColumns[i] = TryDecrypt(inputColumns[i], encEngine);
                                        }
                                        catch (Exception ex)
                                        {
                                            errorLog.Add($"Row {rowNumber}, Column {i + 1}: {ex.Message}");
                                            outputColumns[i] = inputColumns[i];
                                        }
                                    }
                                    else
                                    {
                                        outputColumns[i] = inputColumns[i];
                                    }
                                }
                            }

                            // Log row details.
                            string inputRow = string.Join(",", inputColumns);
                            string outputRow = string.Join(",", outputColumns);
                            Console.WriteLine($"Row {rowNumber}: INPUT - {inputRow} OUTPUT - {outputRow}");

                            writer.WriteLine(outputRow);
                            rowNumber++;
                        }
                    }
                }

                if (errorLog.Count > 0)
                {
                    Console.WriteLine("Errors encountered while processing table: " + tableConfig.TableName);
                    foreach (var error in errorLog)
                    {
                        Console.WriteLine(error);
                    }
                }
                else
                {
                    Console.WriteLine("No decryption errors encountered for table: " + tableConfig.TableName);
                }
                Console.WriteLine("Output written to: " + tableConfig.OutputFile);
            }
            catch (Exception ex)
            {
                Console.WriteLine("An error occurred processing table " + tableConfig.TableName + ": " + ex.Message);
            }
        }
        /// <summary>
        /// A stub decryption method.
        /// In this example, if the input string starts with "ENC:" we assume it is encrypted and we "decrypt" it by removing the prefix.
        /// Otherwise, an exception is thrown.
        /// </summary>
        /// <param name="input">The input string to decrypt.</param>
        /// <returns>The decrypted string.</returns>
        string TryDecrypt(string input, CAFRSAESEncryptionEngine decryptLibrary)
        {
            try
            {
                string decryptedData = decryptLibrary.DecryptString(input, decryptLibrary.PasswordStore.GetPassword("PII", PasswordStore.PasswordState.Decrypted));

                return decryptedData;

            }
            catch (Exception ex)
            {
                throw new Exception("Decryption failed: " + ex.Message);
            }
        }

        public async Task RollbackAsync(ExportContext context)
        {
            Console.WriteLine("ExportPluginA: Rolling back export...");
            await Task.Delay(200);
            Console.WriteLine("ExportPluginA: Rollback complete.");
        }
    }
}
