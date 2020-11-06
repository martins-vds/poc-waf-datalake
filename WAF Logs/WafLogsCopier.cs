using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.WebJobs.Extensions.DataLake;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Blob;

namespace WAF_Logs
{
    public static class WafLogsCopier
    {
        [FunctionName(nameof(CopyToDataLake))]
        public static async Task CopyToDataLake(
            [BlobTrigger("insights-logs-webapplicationfirewalllogs/{blobName}.{blobExtension}", Connection = "WAFLogsStorage")]
            Stream inboundBlob,
            string blobName,
            string blobExtension,
            [DataLakeStore(
              AccountFQDN = "%fqdn%",
              ApplicationId = "%applicationid%",
              ClientSecret = "%clientsecret%",
              TenantID = "%tenantid%")]
            IAsyncCollector<DataLakeStoreOutput> asyncCollector,
            ILogger log)
        {
            var fileStream = inboundBlob;

            if (IsCompressedFile(blobExtension))
            {
                try
                {
                    var decompressed = await Decompress(inboundBlob);
                    decompressed.Seek(0, SeekOrigin.Begin);
                    
                    fileStream = decompressed;
                }
                catch(Exception ex)
                {
                    log.LogError("Failed to decompress blob:\n Name:{name}\n Reason:{reason}", blobName, ex.Message);
                    throw;
                }
            }

            var dataLakeStoreOutput = new DataLakeStoreOutput()
            {
                FileName = "/mydata/" + $"{blobName}.json",
                FileStream = fileStream
            };

            await asyncCollector.AddAsync(dataLakeStoreOutput);

            log.LogInformation($"Processed blob\n Name:{blobName} \n Size: {fileStream.Length} Bytes");
        }

        [FunctionName(nameof(CopyToStorage))]
        public static async Task CopyToStorage(
            [BlobTrigger("insights-logs-webapplicationfirewalllogs/{blobName}.{blobExtension}", Connection = "WAFLogsStorage")]
            Stream inboundBlob,
            string blobName,
            string blobExtension,
            [Blob("insights-logs-webapplicationfirewalllogs-uncompressed/{blobName}.json", FileAccess.Write, Connection = "WAFLogsStorage")]
            Stream outboundBlob,
            ILogger log)
        {
            var fileStream = inboundBlob;

            if (IsCompressedFile(blobExtension))
            {
                try
                {
                    var decompressed = await Decompress(inboundBlob, outboundBlob);
                }
                catch (Exception ex)
                {
                    log.LogError("Failed to decompress blob:\n Name:{name}\n Reason:{reason}", blobName, ex.Message);
                    throw;
                }
            }
            else
            {
                await inboundBlob.CopyToAsync(outboundBlob);
            }

            log.LogInformation($"Processed blob\n Name:{blobName} \n Size: {fileStream.Length} Bytes");
        }

        private static bool IsCompressedFile(string ext)
        {
            if (string.IsNullOrEmpty(ext))
            {
                return false;
            }

            return ext.Equals("gz", StringComparison.OrdinalIgnoreCase);
        }

        private static async Task<Stream> Decompress(Stream compressedFile, Stream decompressedFile = null)
        {
            if(decompressedFile == null)
            {
                decompressedFile = new MemoryStream();
            }

            using GZipStream gZipStream = new GZipStream(compressedFile, CompressionMode.Decompress, true);

            await gZipStream.CopyToAsync(decompressedFile);
            
            return decompressedFile;
        }

        private static string ChangeFileExtension(string name)
        {
            return name.Replace(".gz", ".json");
        }
    }
}
