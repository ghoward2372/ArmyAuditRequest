using DataRequestPipeline.DataContracts;
using System.ComponentModel.Composition;


namespace CleanPlugins
{
    [Export(typeof(ICleanPlugin))]
    public class AuditRequestCleanPlugin : ICleanPlugin
    {
        public async Task ExecuteAsync(DataContext context)
        {
            Console.WriteLine("CleanPluginA: Starting cleaning operation...");

            // Example cleaning: trim the data string.
            if (!string.IsNullOrEmpty(context.Data))
            {
                context.Data = context.Data.Trim();
            }

            await Task.Delay(300); // Simulate asynchronous work.
            Console.WriteLine("CleanPluginA: Cleaning complete.");
        }

        public async Task RollbackAsync(DataContext context)
        {
            Console.WriteLine("CleanPluginA: Rolling back cleaning operation...");
            // Optionally, restore previous state or log rollback actions.
            await Task.Delay(150);
            Console.WriteLine("CleanPluginA: Rollback complete.");
        }
    }
}
