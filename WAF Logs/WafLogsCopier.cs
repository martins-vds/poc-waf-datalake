using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.Extensions.DataLake;
using System.IO.Compression;
using System.Threading.Tasks;

namespace WAF_Logs
{
    public static class WafLogsCopier
    {
        [FunctionName(nameof(CopyWafLogs))]
        public static async Task CopyWafLogs(
            [BlobTrigger("insights-logs-webapplicationfirewalllogs/{name}", Connection = "WAFLogsStorage")]
            Stream myBlob, 
            string name,
            [DataLakeStore(
              AccountFQDN = "%fqdn%",
              ApplicationId = "%applicationid%",
              ClientSecret = "%clientsecret%",
              TenantID = "%tenantid%")]
            IAsyncCollector<DataLakeStoreOutput> asyncCollector,
            ILogger log)
        {
            var fileStream = myBlob;

            if (IsCompressedFile(name))
            {
                try
                {
                    var decompressed = await Decompress(name, myBlob, log);

                    name = decompressed.fileName;
                    fileStream = decompressed.fileStream;
                }
                catch(Exception ex)
                {
                    log.LogError("Failed to decompress blob:\n Name:{name}\n Reason:{reason}", name, ex.Message);
                    throw;
                }
                
            }

            var dataLakeStoreOutput = new DataLakeStoreOutput()
            {
                FileName = "/mydata/" + name,
                FileStream = fileStream
            };

            await asyncCollector.AddAsync(dataLakeStoreOutput);

            log.LogInformation($"Processed blob\n Name:{name} \n Size: {fileStream.Length} Bytes");
        }

        private static bool IsCompressedFile(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
            {
                return false;
            }

            return fileName.EndsWith(".gz");
        }

        private static async Task<(string fileName, Stream fileStream)> Decompress(string name, Stream compressedFile, ILogger log)
        {
            if (!IsCompressedFile(name))
            {
                return (null, null);
            }

            MemoryStream decompressedFile = new MemoryStream();
            using GZipStream gZipStream = new GZipStream(compressedFile, CompressionMode.Decompress, true);

            log.LogDebug("Decompressing blob:\n Name:{name} ", name);
            await gZipStream.CopyToAsync(decompressedFile);
            decompressedFile.Seek(0, SeekOrigin.Begin);

            log.LogDebug("Changing file extension", name);
            string newName = ChangeFileExtension(name);
            log.LogDebug("New file name:\n {name}", newName);

            return (newName, decompressedFile);
        }

        private static string ChangeFileExtension(string name)
        {
            return name.Replace(".gz", ".json");
        }
    }
}
