namespace Genesys___analytics_conversations_details_query
{
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Options;
    using Microsoft.VisualBasic;
    using Newtonsoft.Json.Linq;
    using System.Collections.Concurrent;
    using System.Data;
    using System.Diagnostics;
    using System.Diagnostics.Contracts;
    using System.Diagnostics.Metrics;
    using System.Net;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Net.Http.Json;
    using System.Reflection.Metadata;
    using System.Text;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using static System.Formats.Asn1.AsnWriter;
    using static System.Net.Mime.MediaTypeNames;
    using static System.Runtime.InteropServices.JavaScript.JSType;


    public class Worker : BackgroundService
    {

        private readonly IConfiguration _configuration;
        private readonly string _connectionString;

        private readonly ILogger<Worker> _logger;
        private readonly IHttpClientFactory _httpClientFactory;

        private const string DBOld = "Data Source=localhost\\MSSQLSERVER01;Database=Genesys;Persist Security Info=False;User ID=appsdev_sa;Password=Thr33c0m@12;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False;Command Timeout=0;";

        //Japan
        private const string TOKEN_URL = "https://login.mypurecloud.jp/oauth/token";
        private const string API_URL_BASE_URL = "https://api.mypurecloud.jp/api/v2";
        private const string CLIENT_ID = "84ce4bc3-8901-4678-b795-ee8db069f00f";
        private const string CLIENT_SECRET = "G22C__DncPniplVj8r9PGZJG1bh-c1495LQKXCIYvZWZw9SB4JmZfSKpMT3s";

        //Singapore
        //private const string TOKEN_URL = "https://login.apse1.pure.cloud/oauth/token";
        //private const string API_URL_BASE_URL = "https://api.apse1.pure.cloud/api/v2";
        //private const string CLIENT_ID = "05567e2b-9880-499a-9ebe-032e3d14f4e1";
        //private const string CLIENT_SECRET = "G31C__RpQ1oKcrxpgwrwcVw1XQNH9oCZVs8GqokQEN5NGWmHcPHY_2q2rqRk";

        ////API
        private const string API_URL_CONVERSIONS = API_URL_BASE_URL + "/analytics/conversations/details/query";
        private const string API_URL_CONVERSIONSV2 = API_URL_BASE_URL + "/conversations/";
        private const string API_URL_USERS = API_URL_BASE_URL + "/users?pageSize=5000&pageNumber=1&expand=groups";
        private const string API_URL_USERS_BULK = API_URL_BASE_URL + "/users/bulk";
        private const string API_URL_USERS_GET = API_URL_BASE_URL + "/users/";
        private const string API_URL_QUEUE = API_URL_BASE_URL + "/routing/queues?pageSize=5000&pageNumber=1";
        private const string API_URL_QUEUE_GET = API_URL_BASE_URL + "/routing/queues/";
        private const string API_URL_USER_PRESENCE = API_URL_BASE_URL + "/analytics/users/details/query";
        private const string API_URL_GROUP = API_URL_BASE_URL + "/groups";
        private const string API_URL_MEMBER = API_URL_BASE_URL + "/groups/";
        private const string API_URL_USER_PRESENCE_DEFINITION = API_URL_BASE_URL + "/presencedefinitions/";
        private const string API_URL_OUTBOUND_CAMPAIGN = API_URL_BASE_URL + "/outbound/campaigns?pageSize=500&pageNumber=1";
        private const string API_URL_WRAPUP_CODE = API_URL_BASE_URL + "/routing/wrapupcodes?pageSize=500&pageNumber=1";

        private string API_NAME;
        private int FirstRun = 1;
        private DateTime? Last_Run;

        //private string TOKEN_URL;
        //private string API_URL_BASE_URL;
        //private string CLIENT_ID;
        //private string CLIENT_SECRET;

        //private string API_URL_CONVERSIONS;
        //private string API_URL_CONVERSIONSV2;
        //private string API_URL_USERS;
        //private string API_URL_USERS_BULK;
        //private string API_URL_USERS_GET;
        //private string API_URL_QUEUE;
        //private string API_URL_QUEUE_GET;
        //private string API_URL_USER_PRESENCE;
        //private string API_URL_GROUP;
        //private string API_URL_MEMBER;
        //private string API_URL_USER_PRESENCE_DEFINITION;
        //private string API_URL_OUTBOUND_CAMPAIGN;
        //private string API_URL_WRAPUP_CODE;

        private string INTERVAL = "2026-07-27T00:00:00.000Z/2026-07-28T00:00:00.000Z";

        public Worker(ILogger<Worker> logger, IHttpClientFactory httpClientFactory, IConfiguration configuration)
        {
            _logger = logger;
            _httpClientFactory = httpClientFactory;
            _configuration = configuration;
            _connectionString = _configuration.GetConnectionString("GenesysDatabase");
            API_NAME = _configuration["APINAME"];
            Last_Run = null;
        }

        private async Task SqlQueryLogsAsync(string apiName, string logs)
        {
            try
            {
                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync();

                await using var cmd = new SqlCommand(@"
                    INSERT INTO UploadAPILogs
                    (
                        Name,
                        Logs,
                        Datetime
                    )
                    VALUES
                    (
                        @APIName,
                        @Logs,
                        GETDATE()
                    )", conn);

                cmd.CommandTimeout = 300;

                cmd.Parameters.Add("@APIName", SqlDbType.NVarChar, 200)
                    .Value = (object?)apiName ?? DBNull.Value;

                cmd.Parameters.Add("@Logs", SqlDbType.NVarChar, -1)
                    .Value = (object?)logs ?? DBNull.Value;

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"SqlQueryLogsAsync Error: {ex.Message}");
            }
        }

        private async Task SqlQueryAsync(string query)
        {
            if (string.IsNullOrWhiteSpace(query))
                return;

            try
            {
                await using var conn = new SqlConnection(_connectionString);
                await conn.OpenAsync();

                await using var cmd = new SqlCommand(query, conn)
                {
                    CommandTimeout = 300
                };

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    "SqlQueryAsync",
                    $"Error: {ex.Message}{Environment.NewLine}Query: {query}"
                );

                throw;
            }
        }

        //EOD
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            if (FirstRun == 1)
            {
                await SqlQueryAsync("update UploadAPI set Status = 1");
                FirstRun = 0;
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                using (SqlCommand sqlCommand = new SqlCommand("select *, CONVERT(varchar(23), DATEADD(hour, -8, CAST(CAST(LatestUploadDate AS date) AS datetime)),126) + 'Z/' + CONVERT(varchar(23), DATEADD(hour, -8, DATEADD(DAY, 1, CAST(CAST(LatestUploadDate AS date) AS datetime))),126) + 'Z' AS DateInterval " +
                "from UploadAPI where status = 1 and Name = '" + API_NAME + "' AND DATEADD(DAY, [Interval], LatestUploadDate) < GETDATE()", connection))
                {
                    connection.OpenAsync();
                    using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
                    {
                        while (sqlDataReader.Read())
                        {
                            DateTime LatestUploadDate = (DateTime)sqlDataReader["LatestUploadDate"];
                            string DateInterval = (string)sqlDataReader["DateInterval"];
                            INTERVAL = DateInterval;


                            await SqlQueryAsync("update UploadAPI set Status = 0 where Name = '" + API_NAME + "'");

                            try
                            {
                                var token = await GetCachedToken();

                                await SqlQueryLogsAsync(API_NAME, "Start");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("DeleteTables", "Start");
                                await DeleteTables();
                                await SqlQueryLogsAsync("DeleteTables", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiConversations", "Start");
                                await CallApiConversationsHourly(token);
                                await SqlQueryLogsAsync("CallApiConversations", "End");

                                await Task.Delay(3000);

                                await SqlQueryAsync("INSERT INTO ConversationsID SELECT DISTINCT ConversationId FROM Conversations_Old");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiConversationv2", "Start");
                                await CallApiConversationv2(token);
                                await SqlQueryLogsAsync("CallApiConversationv2", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiWrapupCode", "Start");
                                await CallApiWrapupCode(token);
                                await SqlQueryLogsAsync("CallApiWrapupCode", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiGroup", "Start");
                                await CallApiGroup(token);
                                await SqlQueryLogsAsync("CallApiGroup", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiUsers", "Start");
                                await CallApiUsers(token);
                                await SqlQueryLogsAsync("CallApiUsers", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiQueue", "Start");
                                await CallApiQueue(token);
                                await SqlQueryLogsAsync("CallApiQueue", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiUsersByUserId", "Start");
                                await CallApiUsersByUserId(token);
                                await SqlQueryLogsAsync("CallApiUsersByUserId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiQueuesByQueueId", "Start");
                                await CallApiQueuesByQueueId(token);
                                await SqlQueryLogsAsync("CallApiQueuesByQueueId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiOutboundCampaign", "Start");
                                await CallApiOutboundCampaign(token);
                                await SqlQueryLogsAsync("CallApiOutboundCampaign", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiUserPresenceByUserId", "Start");
                                await CallApiUserPresenceByUserId(token);
                                await SqlQueryLogsAsync("CallApiUserPresenceByUserId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiUsersByUserIdv2", "Start");
                                await CallApiUsersByUserIdv2(token);
                                await SqlQueryLogsAsync("CallApiUsersByUserIdv2", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiPresenceDefinitionByPresenceId", "Start");
                                await CallApiPresenceDefinitionByPresenceId(token);
                                await SqlQueryLogsAsync("CallApiPresenceDefinitionByPresenceId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiOutboundCampaignByContactListId", "Start");
                                await CallApiOutboundCampaignByContactListId(token, DateInterval);
                                await SqlQueryLogsAsync("CallApiOutboundCampaignByContactListId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("CallApiOutboundCampaignByContactListId", "Start");
                                await CallApiOutboundCampaignByContactListId(token);
                                await SqlQueryLogsAsync("CallApiOutboundCampaignByContactListId", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync("DeleteTables", "Start");
                                await InsertTables(LatestUploadDate);
                                await SqlQueryLogsAsync("DeleteTables", "End");

                                await Task.Delay(3000);

                                await SqlQueryLogsAsync(API_NAME, "End");
                                _logger.LogInformation("Job executed at: {time}", DateTimeOffset.Now);

                                await SqlQueryAsync("update UploadAPI set Status = 1, LatestUploadDate = DATEADD(day, 1 ,LatestUploadDate) where Name = '" + API_NAME + "'");

                                await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Error running job");
                                await SqlQueryLogsAsync(API_NAME, "Error: " + ex.Message);
                            }

                        }

                    }
                }
            }

            await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        }

        //////INTRA
        //protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //{
        //    using var connection = new SqlConnection(_connectionString);
        //    await connection.OpenAsync();

        //    var now = DateTime.Now;
        //    var currentTime = now.TimeOfDay;
        //    var LastRun = Last_Run;

        //    if (FirstRun == 1)
        //    {
        //        await SqlQueryAsync("update UploadAPI set Status = 1");
        //        FirstRun = 0;
        //    }

        //    while (!stoppingToken.IsCancellationRequested)
        //    {
        //        if (!LastRun.HasValue || (DateTime.Now - LastRun.Value).TotalMinutes >= 30)
        //        {
        //            using (SqlCommand sqlCommand = new SqlCommand("select *, CONVERT(varchar(23), DATEADD(hour, -8, cast(CAST(GETDATE() AS date) as datetime)), 126)  + 'Z/' " +
        //                "+ CONVERT(varchar(23), DATEADD(HOUR, -8, DATEADD( MINUTE, (DATEDIFF(MINUTE, 0, GETDATE()) / 30) * 30, 0)), 126) + 'Z' AS DateInterval " +
        //                "from UploadAPI where status = 1 and Name = '" + API_NAME + "'", connection))
        //            {
        //                connection.OpenAsync();
        //                using (SqlDataReader sqlDataReader = sqlCommand.ExecuteReader())
        //                {
        //                    while (sqlDataReader.Read())
        //                    {
        //                        DateTime LatestUploadDate = (DateTime)sqlDataReader["LatestUploadDate"];
        //                        string DateInterval = (string)sqlDataReader["DateInterval"];
        //                        INTERVAL = DateInterval;

        //                        await SqlQueryAsync("update UploadAPI set Status = 0 where Name = '" + API_NAME + "'");

        //                        try
        //                        {
        //                            var token = await GetCachedToken();

        //                            DateTime DTnow = DateTime.Now;


        //                            LastRun = new DateTime(
        //                                DTnow.Year,
        //                                DTnow.Month,
        //                                DTnow.Day,
        //                                DTnow.Hour,
        //                                (DTnow.Minute / 30) * 30,
        //                                0);

        //                            await SqlQueryLogsAsync(API_NAME, "Start");

        //                            await Task.Delay(3000);

        //                            if (LatestUploadDate < DTnow)
        //                            {
        //                                await SqlQueryLogsAsync("DeleteTables", "Start");
        //                                await DeleteTables();
        //                                await SqlQueryLogsAsync("DeleteTables", "End");

        //                                await Task.Delay(3000);

        //                                await SqlQueryLogsAsync("CallApiConversations", "Start");
        //                                await CallApiConversationsHourly(token);
        //                                await SqlQueryLogsAsync("CallApiConversations", "End");

        //                                await Task.Delay(3000);

        //                                await SqlQueryAsync("INSERT INTO ConversationsID SELECT DISTINCT ConversationId FROM Conversations_Old");

        //                                await Task.Delay(3000);

        //                                await SqlQueryAsync("update UploadAPI set LatestUploadDate = DATEADD(MINUTE, 30, GETDATE()) where Name = '" + API_NAME + "'");
        //                            }

        //                            await Task.Delay(3000);

        //                            await SqlQueryLogsAsync("CallApiConversationv2", "Start");
        //                            await CallApiConversationv2INTRA(token);
        //                            await SqlQueryLogsAsync("CallApiConversationv2", "End");

        //                            await Task.Delay(3000);

        //                            await SqlQueryLogsAsync(API_NAME, "End");
        //                            _logger.LogInformation("Job executed at: {time}", DateTimeOffset.Now);

        //                            await SqlQueryAsync("update UploadAPI set Status = 1, LatestUploadDate = GETDATE() where Name = '" + API_NAME + "'");

        //                            await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        //                        }
        //                        catch (Exception ex)
        //                        {
        //                            await SqlQueryAsync("update UploadAPI set Status = 1 where Name = '" + API_NAME + "'");
        //                            _logger.LogError(ex, "Error running job");
        //                            await SqlQueryLogsAsync(API_NAME, "Error: " + ex.Message);
        //                        }

        //                    }

        //                }

        //            }
        //        }
        //        await Task.Delay(TimeSpan.FromMinutes(5), stoppingToken);
        //    }
        //}


        //private async Task<string> GetToken()
        //{
        //    var client = _httpClientFactory.CreateClient();

        //    var auth = Convert.ToBase64String(
        //        Encoding.UTF8.GetBytes($"{CLIENT_ID}:{CLIENT_SECRET}")
        //    );

        //    client.DefaultRequestHeaders.Authorization =
        //        new AuthenticationHeaderValue("Basic", auth);

        //    var content = new StringContent(
        //        "grant_type=client_credentials",
        //        Encoding.UTF8,
        //        "application/x-www-form-urlencoded"
        //    );

        //    var response = await client.PostAsync(TOKEN_URL, content);
        //    var json = await response.Content.ReadAsStringAsync();

        //    using var doc = JsonDocument.Parse(json);
        //    return doc.RootElement.GetProperty("access_token").GetString();
        //}

        private string _token;
        private DateTime _tokenCreated;
        private readonly SemaphoreSlim _tokenLock = new SemaphoreSlim(1, 1);

        private async Task<string> GetCachedToken()
        {
            if (!string.IsNullOrEmpty(_token) &&
                DateTime.UtcNow < _tokenCreated.AddHours(12))
                return _token;

            await _tokenLock.WaitAsync();

            try
            {
                // Check again after acquiring the lock
                if (!string.IsNullOrEmpty(_token) &&
                    DateTime.UtcNow < _tokenCreated.AddHours(23))
                    return _token;

                _token = await GetToken();
                _tokenCreated = DateTime.UtcNow;

                return _token;
            }
            finally
            {
                _tokenLock.Release();
            }
        }

        private async Task<string> GetToken()
        {
            const int maxRetries = 10;

            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    var client = _httpClientFactory.CreateClient();

                    var auth = Convert.ToBase64String(
                        Encoding.UTF8.GetBytes($"{CLIENT_ID}:{CLIENT_SECRET}")
                    );

                    client.DefaultRequestHeaders.Authorization =
                        new AuthenticationHeaderValue("Basic", auth);

                    var content = new StringContent(
                        "grant_type=client_credentials",
                        Encoding.UTF8,
                        "application/x-www-form-urlencoded"
                    );

                    var response = await client.PostAsync(TOKEN_URL, content);

                    // Throw if status code is not 2xx
                    response.EnsureSuccessStatusCode();

                    var json = await response.Content.ReadAsStringAsync();

                    using var doc = JsonDocument.Parse(json);

                    return doc.RootElement.GetProperty("access_token").GetString()!;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex,
                        "GetToken failed. Attempt {Attempt} of {MaxRetries}.",
                        attempt, maxRetries);

                    if (attempt == maxRetries)
                        throw;

                    // Wait 5 seconds before retrying
                    await Task.Delay(TimeSpan.FromSeconds(5));
                }
            }

            throw new InvalidOperationException("Failed to obtain access token.");
        }

        private async Task DeleteTables()
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            string DeleteTablesSQLQry = "EXEC OSSGenesysTruncateTables";
            SqlCommand DeleteTablesSQLCmd = new SqlCommand(DeleteTablesSQLQry, connection);
            DeleteTablesSQLCmd.CommandTimeout = 300;
            DeleteTablesSQLCmd.ExecuteNonQuery();
        }

        private async Task InsertTables(DateTime LatestUploadDate)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            string DeleteTablesSQLQry = "EXEC OSSGenesysInsertTables '" + LatestUploadDate.ToString("MM/dd/yyyy") + "','" + LatestUploadDate.ToString("MM/dd/yyyy") + "'";
            SqlCommand DeleteTablesSQLCmd = new SqlCommand(DeleteTablesSQLQry, connection);
            DeleteTablesSQLCmd.CommandTimeout = 300;
            DeleteTablesSQLCmd.ExecuteNonQuery();
        }

        private async Task InsertTables()
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            string DeleteTablesSQLQry = "EXEC OSSGenesysInsertTables";
            SqlCommand DeleteTablesSQLCmd = new SqlCommand(DeleteTablesSQLQry, connection);
            DeleteTablesSQLCmd.CommandTimeout = 300;
            DeleteTablesSQLCmd.ExecuteNonQuery();
        }

        private static string GetString(JsonElement e, string prop) => e.TryGetProperty(prop, out var v) ? v.GetString() : null;

        private static Guid? GetGuid(JsonElement e, string prop) => e.TryGetProperty(prop, out var v) && Guid.TryParse(v.GetString(), out var g) ? g : null;

        //private static DateTime? GetDate(JsonElement e, string prop) => e.TryGetProperty(prop, out var v) && DateTime.TryParse(v.GetString(), out var d) ? d : null;

        private static DateTime? GetDate(JsonElement e, string prop)
        {
            if (!e.TryGetProperty(prop, out var v) || v.ValueKind == JsonValueKind.Null)
                return null;

            var str = v.GetString();
            if (string.IsNullOrWhiteSpace(str))
                return null;

            if (DateTimeOffset.TryParse(str, out var dto))
                return dto.UtcDateTime;

            return null;
        }

        private static int? GetBoolAsInt(JsonElement e, string prop)
        {
            if (!e.TryGetProperty(prop, out var v))
                return null;

            return v.GetBoolean() ? 1 : 0;
        }

        private static int? GetInt(JsonElement e, string prop) => e.TryGetProperty(prop, out var v) && int.TryParse(v.ToString(), out var i) ? i : (int?)null;

        private static decimal? GetDecimal(JsonElement e, string prop)
        {
            return e.TryGetProperty(prop, out var v)
                ? v.GetDecimal()
                : (decimal?)null;
        }


        private static string ToSqlDateTime(DateTime? date)
        {
            if (!date.HasValue || date.Value == new DateTime(1900, 1, 1))
                return "NULL";

            return $"'{date.Value:yyyy-MM-dd HH:mm:ss.fff}'";
        }

        public static string ToSqlString(int? value)
        {
            return value?.ToString() ?? "NULL";
        }

        #region Outbound Campaign

        private async Task CallApiWrapupCode(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            for (int currentAttempt = 1;
                 currentAttempt <= maxAttempts;
                 currentAttempt++)
            {
                try
                {
                    var response = await client.GetAsync(API_URL_WRAPUP_CODE);

                    int statusCode = (int)response.StatusCode;

                    // ============================================
                    // RETRY HTTP 400 - 499
                    // ============================================
                    if (statusCode >= 400 && statusCode < 500)
                    {
                        int retrySeconds = 2;

                        // 429 - Genesys Rate Limit
                        if (response.StatusCode ==
                            HttpStatusCode.TooManyRequests)
                        {
                            retrySeconds = 3;

                            if (response.Headers.TryGetValues(
                                "Retry-After",
                                out var values))
                            {
                                var retryAfter =
                                    values.FirstOrDefault();

                                if (int.TryParse(
                                    retryAfter,
                                    out int parsedSeconds))
                                {
                                    retrySeconds = parsedSeconds;
                                }
                            }
                        }
                        else
                        {
                            // Other 4xx
                            // 2, 4, 6, 8... seconds
                            retrySeconds =
                                Math.Min(60, currentAttempt * 2);
                        }

                        string errorResponse =
                            await response.Content.ReadAsStringAsync();

                        Console.WriteLine(
                            $"Wrapup Code API returned HTTP {statusCode}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}. " +
                            $"Retrying in {retrySeconds} seconds. " +
                            $"Response: {errorResponse}");

                        response.Dispose();

                        // Stop after max attempts
                        if (currentAttempt >= maxAttempts)
                        {
                            throw new HttpRequestException(
                                $"Wrapup Code API failed after " +
                                $"{maxAttempts} attempts. " +
                                $"HTTP Status: {statusCode}. " +
                                $"Response: {errorResponse}");
                        }

                        await Task.Delay(
                            TimeSpan.FromSeconds(retrySeconds));

                        continue;
                    }

                    // ============================================
                    // RETRY HTTP 500 - 599
                    // ============================================
                    if (statusCode >= 500 && statusCode < 600)
                    {
                        int retrySeconds =
                            Math.Min(60, currentAttempt * 3);

                        Console.WriteLine(
                            $"Wrapup Code API returned HTTP {statusCode}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}. " +
                            $"Retrying in {retrySeconds} seconds.");

                        response.Dispose();

                        if (currentAttempt >= maxAttempts)
                        {
                            throw new HttpRequestException(
                                $"Wrapup Code API failed after " +
                                $"{maxAttempts} attempts. " +
                                $"HTTP Status: {statusCode}");
                        }

                        await Task.Delay(
                            TimeSpan.FromSeconds(retrySeconds));

                        continue;
                    }

                    // ============================================
                    // SUCCESS
                    // ============================================
                    response.EnsureSuccessStatusCode();

                    var result =
                        await response.Content.ReadAsStringAsync();

                    response.Dispose();

                    if (!string.IsNullOrWhiteSpace(result))
                    {
                        await SaveToDatabaseWrapupCodev2(result);
                    }

                    Console.WriteLine(
                        $"Wrapup Code API successful on attempt " +
                        $"{currentAttempt}.");

                    return;
                }
                catch (HttpRequestException ex)
                {
                    Console.WriteLine(
                        $"HTTP request error. " +
                        $"Attempt {currentAttempt}/{maxAttempts}: " +
                        $"{ex.Message}");

                    if (currentAttempt >= maxAttempts)
                        throw;

                    int retrySeconds =
                        Math.Min(60, currentAttempt * 3);

                    await Task.Delay(
                        TimeSpan.FromSeconds(retrySeconds));
                }
                catch (TaskCanceledException ex)
                {
                    Console.WriteLine(
                        $"Request timeout. " +
                        $"Attempt {currentAttempt}/{maxAttempts}: " +
                        $"{ex.Message}");

                    if (currentAttempt >= maxAttempts)
                        throw;

                    int retrySeconds =
                        Math.Min(60, currentAttempt * 3);

                    await Task.Delay(
                        TimeSpan.FromSeconds(retrySeconds));
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"Unexpected error. " +
                        $"Attempt {currentAttempt}/{maxAttempts}: " +
                        $"{ex.Message}");

                    if (currentAttempt >= maxAttempts)
                        throw;

                    await Task.Delay(
                        TimeSpan.FromSeconds(2));
                }
            }
        }


        private async Task SaveToDatabaseWrapupCodev1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                foreach (var ent in doc.RootElement.GetProperty("entities").EnumerateArray())
                {
                    string WrapupId = ent.TryGetProperty("id", out var ui) ? ui.GetString() : null;
                    string WrapupnName = ent.TryGetProperty("name", out var ufn) ? ufn.GetString() : null;

                    string entitiesSQLQry = "INSERT INTO WrapupCode " +
                        " SELECT '" + WrapupId + "', '" + WrapupnName + "'";

                    SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                    entitiesSQLCmd.CommandTimeout = 300;

                    entitiesSQLCmd.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseWrapupCode", "Error: " + ex.Message);
            }
        }

        private async Task SaveToDatabaseWrapupCodev2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var wrapupCodes = CreateTable(
                    ("WrapupId", typeof(string)),
                    ("Name", typeof(string))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                if (!doc.RootElement.TryGetProperty("entities", out var entities) ||
                    entities.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'entities' array.");
                }


                // =========================================================
                // PARSE JSON
                // =========================================================

                foreach (var entity in entities.EnumerateArray())
                {
                    string wrapupId = S(entity, "id");
                    string wrapupName = S(entity, "name");

                    if (string.IsNullOrWhiteSpace(wrapupId))
                        continue;

                    wrapupCodes.Rows.Add(
                        Db(wrapupId),
                        Db(wrapupName)
                    );
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (wrapupCodes.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        wrapupCodes,
                        "WrapupCode");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseWrapupCodev2),
                    $"SUCCESS | " +
                    $"WrapupCodes={wrapupCodes.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseWrapupCodev2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }

        private async Task CallApiOutboundCampaign(string token)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;
            int currentAttempt = 0;

            while (currentAttempt < maxAttempts)
            {
                currentAttempt++;

                try
                {
                    var response = await client.GetAsync(API_URL_OUTBOUND_CAMPAIGN);

                    // ============================================================
                    // HTTP 400 - 500: RETRY
                    // ============================================================
                    int statusCode = (int)response.StatusCode;

                    if (statusCode >= 400 && statusCode <= 500)
                    {
                        int delaySeconds = 2;

                        // For 429, use Genesys Retry-After header
                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            if (response.Headers.TryGetValues("Retry-After", out var values))
                            {
                                var firstValue = values.FirstOrDefault();

                                if (int.TryParse(firstValue, out int retryAfter))
                                {
                                    delaySeconds = retryAfter;
                                }
                            }
                        }
                        else
                        {
                            // Exponential backoff:
                            // Attempt 1 = 2 sec
                            // Attempt 2 = 4 sec
                            // Attempt 3 = 8 sec
                            // Attempt 4 = 16 sec
                            delaySeconds = (int)Math.Pow(2, currentAttempt - 1);
                        }

                        var errorBody = await response.Content.ReadAsStringAsync();

                        await SqlQueryLogsAsync(
                            "CallApiOutboundCampaign",
                            $"HTTP {statusCode} received. " +
                            $"Attempt {currentAttempt}/{maxAttempts}. " +
                            $"Retrying in {delaySeconds}s. " +
                            $"Response: {errorBody}");

                        // If this was the last attempt, throw
                        if (currentAttempt >= maxAttempts)
                        {
                            response.EnsureSuccessStatusCode();
                        }

                        await Task.Delay(TimeSpan.FromSeconds(delaySeconds));

                        // Retry same API call
                        continue;
                    }

                    // ============================================================
                    // SUCCESS
                    // ============================================================
                    response.EnsureSuccessStatusCode();

                    var result = await response.Content.ReadAsStringAsync();

                    if (!string.IsNullOrWhiteSpace(result))
                    {
                        await SaveToDatabaseOutboundCampaignv2(result);
                    }

                    return;
                }
                catch (HttpRequestException ex)
                {
                    // ============================================================
                    // NETWORK / CONNECTION ERROR
                    // ============================================================
                    await SqlQueryLogsAsync(
                        "CallApiOutboundCampaign",
                        $"Network error. " +
                        $"Attempt {currentAttempt}/{maxAttempts}. " +
                        $"Error: {ex.Message}");

                    if (currentAttempt >= maxAttempts)
                    {
                        throw;
                    }

                    int delaySeconds = (int)Math.Pow(2, currentAttempt - 1);

                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                }
                catch (TaskCanceledException ex)
                {
                    // ============================================================
                    // TIMEOUT
                    // ============================================================
                    await SqlQueryLogsAsync(
                        "CallApiOutboundCampaign",
                        $"Request timeout. " +
                        $"Attempt {currentAttempt}/{maxAttempts}. " +
                        $"Error: {ex.Message}");

                    if (currentAttempt >= maxAttempts)
                    {
                        throw;
                    }

                    int delaySeconds = (int)Math.Pow(2, currentAttempt - 1);

                    await Task.Delay(TimeSpan.FromSeconds(delaySeconds));
                }
                catch (Exception ex)
                {
                    // ============================================================
                    // OTHER ERRORS
                    // ============================================================
                    await SqlQueryLogsAsync(
                        "CallApiOutboundCampaign",
                        $"Error. " +
                        $"Attempt {currentAttempt}/{maxAttempts}. " +
                        $"Error: {ex.Message}");

                    if (currentAttempt >= maxAttempts)
                    {
                        throw;
                    }

                    await Task.Delay(TimeSpan.FromSeconds(2));
                }
            }
        }

        private async Task SaveToDatabaseOutboundCampaignv1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                foreach (var ent in doc.RootElement.GetProperty("entities").EnumerateArray())
                {
                    string CampaignId = ent.TryGetProperty("id", out var ui) ? ui.GetString() : null;
                    string CampaignName = ent.TryGetProperty("name", out var ufn) ? ufn.GetString() : null;
                    string DialingMode = ent.TryGetProperty("dialingMode", out var dm) ? dm.GetString() : null;

                    string ContactListId = null;
                    string ContactListName = null;

                    if (ent.TryGetProperty("contactList", out JsonElement contactListBlock))
                    {
                        ContactListId = GetString(contactListBlock, "id");
                        ContactListName = GetString(contactListBlock, "name");
                    }

                    string entitiesSQLQry = "INSERT INTO OutboundCampaign " +
                        " SELECT '" + CampaignId + "', '" + CampaignName + "', '" + ContactListId + "', '" + ContactListName + "', '" + DialingMode + "'";

                    SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                    entitiesSQLCmd.CommandTimeout = 300;

                    entitiesSQLCmd.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseOutboundCampaign", "Error: " + ex.Message);
            }

        }

        private async Task SaveToDatabaseOutboundCampaignv2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var campaigns = CreateTable(
                    ("CampaignId", typeof(string)),
                    ("CampaignName", typeof(string)),
                    ("ContactListId", typeof(string)),
                    ("ContactListName", typeof(string)),
                    ("DialingMode", typeof(string))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                var root = doc.RootElement;

                if (!root.TryGetProperty("entities", out var entities) ||
                    entities.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'entities' array.");
                }


                // =========================================================
                // PARSE CAMPAIGNS
                // =========================================================

                foreach (var campaign in entities.EnumerateArray())
                {
                    string campaignId = S(campaign, "id");

                    if (string.IsNullOrWhiteSpace(campaignId))
                        continue;

                    string campaignName = S(campaign, "name");
                    string dialingMode = S(campaign, "dialingMode");

                    string contactListId = null;
                    string contactListName = null;


                    // =====================================================
                    // CONTACT LIST
                    // =====================================================

                    if (campaign.TryGetProperty(
                            "contactList",
                            out var contactList) &&
                        contactList.ValueKind == JsonValueKind.Object)
                    {
                        contactListId = S(contactList, "id");
                        contactListName = S(contactList, "name");
                    }


                    // =====================================================
                    // ADD ROW
                    // =====================================================

                    campaigns.Rows.Add(
                        Db(campaignId),
                        Db(campaignName),
                        Db(contactListId),
                        Db(contactListName),
                        Db(dialingMode)
                    );
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (campaigns.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        campaigns,
                        "OutboundCampaign");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseOutboundCampaignv2),
                    $"SUCCESS | " +
                    $"Campaigns={campaigns.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseOutboundCampaignv2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }


        private async Task CallApiOutboundCampaignByContactListId(string token)
        {
            const int maxParallel = 10;
            const int maxRetries = 5;

            var contacts = new List<(string ListId, string ContactId)>();

            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using var cmd = new SqlCommand(
                    "EXEC OSSGenesysGetDialerContactListIdwContactId", conn);

                cmd.CommandTimeout = 300;

                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    if (reader.IsDBNull(0) || reader.IsDBNull(1))
                        continue;

                    contacts.Add((
                        reader.GetValue(0)?.ToString(),
                        reader.GetValue(1)?.ToString()
                    ));
                }
            }

            if (contacts.Count == 0)
                return;

            using var client = _httpClientFactory.CreateClient();

            client.Timeout = TimeSpan.FromMinutes(5);
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            // One API request at a time.
            var apiGate = new SemaphoreSlim(1, 1);

            await Parallel.ForEachAsync(
                contacts,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = maxParallel
                },
                async (contact, ct) =>
                {
                    string listId = contact.ListId;
                    string contactId = contact.ContactId;

                    string key =
                        $"List={listId}, Contact={contactId}";

                    for (int attempt = 1; attempt <= maxRetries; attempt++)
                    {
                        try
                        {
                            string url =
                                $"{API_URL_BASE_URL}/outbound/contactlists/" +
                                $"{Uri.EscapeDataString(listId)}/contacts/" +
                                $"{Uri.EscapeDataString(contactId)}";

                            HttpResponseMessage response;

                            await apiGate.WaitAsync(ct);

                            try
                            {
                                await Task.Delay(300, ct);

                                response = await client.GetAsync(url, ct);
                            }
                            finally
                            {
                                apiGate.Release();
                            }

                            using (response)
                            {
                                // 404 = SKIP
                                if (response.StatusCode ==
                                    HttpStatusCode.NotFound)
                                {
                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"404 SKIP: {key}");

                                    return;
                                }

                                // 429 = RETRY
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    int waitSeconds = 3;

                                    if (response.Headers.TryGetValues(
                                        "Retry-After",
                                        out var values))
                                    {
                                        int.TryParse(
                                            values.FirstOrDefault(),
                                            out waitSeconds);
                                    }

                                    waitSeconds =
                                        Math.Max(1, waitSeconds);

                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"429: {key}, " +
                                        $"Attempt={attempt}/{maxRetries}, " +
                                        $"Wait={waitSeconds}s");

                                    if (attempt < maxRetries)
                                    {
                                        await Task.Delay(
                                            TimeSpan.FromSeconds(waitSeconds),
                                            ct);

                                        continue;
                                    }

                                    return;
                                }

                                // 5XX = RETRY
                                if ((int)response.StatusCode >= 500)
                                {
                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"HTTP {(int)response.StatusCode}: " +
                                        $"{key}, Attempt={attempt}/{maxRetries}");

                                    if (attempt < maxRetries)
                                    {
                                        await Task.Delay(
                                            TimeSpan.FromSeconds(
                                                Math.Min(30, attempt * 2)),
                                            ct);

                                        continue;
                                    }

                                    return;
                                }

                                // Other 4XX = SKIP
                                if ((int)response.StatusCode >= 400)
                                {
                                    string error =
                                        await response.Content
                                            .ReadAsStringAsync(ct);

                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"HTTP {(int)response.StatusCode}: " +
                                        $"{key}, Response={error}");

                                    return;
                                }

                                // SUCCESS
                                string json =
                                    await response.Content
                                        .ReadAsStringAsync(ct);

                                if (string.IsNullOrWhiteSpace(json))
                                {
                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"EMPTY RESPONSE: {key}");

                                    return;
                                }

                                // =================================================
                                // DATABASE
                                // =================================================

                                using var sql =
                                    new SqlConnection(_connectionString);

                                await sql.OpenAsync(ct);

                                try
                                {
                                    await CallApiOutboundCampaignDatav1(
                                        sql,
                                        json,
                                        listId);
                                }
                                catch (Exception ex)
                                {
                                    await SqlQueryLogsAsync(
                                        nameof(CallApiOutboundCampaignByContactListId),
                                        $"DATABASE/PARSE ERROR: {key}, " +
                                        $"Error={ex}");

                                    return;
                                }

                                await SqlQueryLogsAsync(
                                    nameof(CallApiOutboundCampaignByContactListId),
                                    $"SUCCESS: {key}");

                                return;
                            }
                        }
                        catch (TaskCanceledException ex)
                        {
                            await SqlQueryLogsAsync(
                                nameof(CallApiOutboundCampaignByContactListId),
                                $"TIMEOUT: {key}, " +
                                $"Attempt={attempt}/{maxRetries}, " +
                                $"Error={ex.Message}");

                            if (attempt < maxRetries)
                                await Task.Delay(
                                    TimeSpan.FromSeconds(
                                        Math.Min(30, attempt * 2)),
                                    ct);
                        }
                        catch (HttpRequestException ex)
                        {
                            await SqlQueryLogsAsync(
                                nameof(CallApiOutboundCampaignByContactListId),
                                $"NETWORK ERROR: {key}, " +
                                $"Attempt={attempt}/{maxRetries}, " +
                                $"Error={ex.Message}");

                            if (attempt < maxRetries)
                                await Task.Delay(
                                    TimeSpan.FromSeconds(
                                        Math.Min(30, attempt * 2)),
                                    ct);
                        }
                        catch (Exception ex)
                        {
                            await SqlQueryLogsAsync(
                                nameof(CallApiOutboundCampaignByContactListId),
                                $"UNEXPECTED ERROR: {key}, " +
                                $"Attempt={attempt}/{maxRetries}, " +
                                $"Error={ex}");

                            return;
                        }
                    }

                    await SqlQueryLogsAsync(
                        nameof(CallApiOutboundCampaignByContactListId),
                        $"FAILED: {key} after {maxRetries} attempts");
                });
        }



        private async Task CallApiOutboundCampaignByContactListId(
    string token,
    string dateInterval)
        {
            const int pageSize = 100;
            const int maxParallel = 10;
            const int delayMs = 500;
            const int maxRetry = 50;

            // =========================================================
            // PARSE DATE INTERVAL
            // =========================================================
            var interval = dateInterval.Split('/');

            if (interval.Length != 2)
                throw new ArgumentException("Invalid DateInterval.");

            DateTime start = DateTime.Parse(
                interval[0],
                null,
                System.Globalization.DateTimeStyles.AdjustToUniversal);

            DateTime end = DateTime.Parse(
                interval[1],
                null,
                System.Globalization.DateTimeStyles.AdjustToUniversal);

            // =========================================================
            // GET CONTACT LISTS
            // =========================================================
            var contactLists = new List<string>();

            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using var cmd = new SqlCommand(
                    "EXEC OSSGenesysGetDialerContactListId",
                    conn);

                cmd.CommandTimeout = 300;

                using var reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                    {
                        string contactListId = reader.GetString(0);

                        if (!string.IsNullOrWhiteSpace(contactListId))
                            contactLists.Add(contactListId);
                    }
                }
            }

            // Remove duplicate contact lists
            contactLists = contactLists
                .Distinct()
                .ToList();

            // =========================================================
            // HTTP CLIENT
            // =========================================================
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            client.Timeout = Timeout.InfiniteTimeSpan;

            // =========================================================
            // PROCESS CONTACT LISTS IN PARALLEL
            // =========================================================
            await Parallel.ForEachAsync(
                contactLists,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = maxParallel
                },
                async (contactListId, ct) =>
                {
                    try
                    {
                        // =====================================================
                        // LOOP HOURS
                        // =====================================================
                        for (
                            DateTime hour = start;
                            hour < end;
                            hour = hour.AddHours(1))
                        {
                            // 00, 10, 20, 30, 40, 50
                            for (int minuteBucket = 0;
                                 minuteBucket < 6;
                                 minuteBucket++)
                            {
                                string filterValue =
                                    $"{hour:yyyy-MM-ddTHH}:{minuteBucket}";

                                int pageNumber = 1;
                                int pageCount = 1;

                                // =================================================
                                // PAGINATION
                                // =================================================
                                while (pageNumber <= pageCount)
                                {
                                    await Task.Delay(delayMs, ct);

                                    string requestKey =
                                        $"ContactList={contactListId}, " +
                                        $"Filter={filterValue}, " +
                                        $"Page={pageNumber}";

                                    // =================================================
                                    // REQUEST BODY
                                    // =================================================
                                    var body = new
                                    {
                                        criteria = new
                                        {
                                            filterType = "AND",
                                            clauses = new[]
                                            {
                                        new
                                        {
                                            filterType = "AND",
                                            predicates = new[]
                                            {
                                                new
                                                {
                                                    column =
                                                        "callRecords.MIN.lastAttempt",

                                                    columnType =
                                                        "alphabetic",

                                                    @operator =
                                                        "CONTAINS",

                                                    value =
                                                        filterValue,

                                                    inverted = false
                                                }
                                            }
                                        }
                                            }
                                        },

                                        pageNumber,
                                        pageSize,

                                        contactSorts = new[]
                                        {
                                    new
                                    {
                                        fieldName =
                                            "callRecords.MIN.lastAttempt",

                                        direction = "DESC",

                                        numeric = false
                                    }
                                        }
                                    };

                                    string json = null;

                                    bool apiSuccess = false;

                                    // =================================================
                                    // API RETRY - MAX 50
                                    // =================================================
                                    for (int retry = 1;
                                         retry <= maxRetry;
                                         retry++)
                                    {
                                        ct.ThrowIfCancellationRequested();

                                        try
                                        {
                                            using var response =
                                                await client.PostAsJsonAsync(
                                                    $"{API_URL_BASE_URL}/outbound/contactlists/{contactListId}/contacts/search",
                                                    body,
                                                    ct);

                                            int statusCode =
                                                (int)response.StatusCode;

                                            // =================================================
                                            // 404
                                            //
                                            // ONLY 404 IS SKIPPED.
                                            // DO NOT RETRY.
                                            // MOVE TO NEXT FILTER/PAGE.
                                            // =================================================
                                            if (response.StatusCode ==
                                                HttpStatusCode.NotFound)
                                            {
                                                string errorBody =
                                                    await response.Content
                                                        .ReadAsStringAsync(ct);

                                                await SqlQueryLogsAsync(
                                                    nameof(
                                                        CallApiOutboundCampaignByContactListId),
                                                    $"404 NOT FOUND - SKIPPING. " +
                                                    $"{requestKey}. " +
                                                    $"Response={errorBody}");

                                                // Mark as skipped so we don't
                                                // save anything and don't retry.
                                                apiSuccess = false;

                                                // pageNumber will be incremented
                                                // outside the retry block.
                                                // We use a special flag below.
                                                break;
                                            }

                                            // =================================================
                                            // SUCCESS
                                            // =================================================
                                            if (response.IsSuccessStatusCode)
                                            {
                                                json =
                                                    await response.Content
                                                        .ReadAsStringAsync(ct);

                                                if (!string.IsNullOrWhiteSpace(json))
                                                {
                                                    apiSuccess = true;

                                                    await SqlQueryLogsAsync(
                                                        nameof(
                                                            CallApiOutboundCampaignByContactListId),
                                                        $"API SUCCESS. " +
                                                        $"{requestKey}. " +
                                                        $"Attempt={retry}/{maxRetry}");

                                                    break;
                                                }

                                                await SqlQueryLogsAsync(
                                                    nameof(
                                                        CallApiOutboundCampaignByContactListId),
                                                    $"EMPTY RESPONSE. " +
                                                    $"{requestKey}. " +
                                                    $"Attempt={retry}/{maxRetry}");
                                            }

                                            // =================================================
                                            // 429 RATE LIMIT
                                            // =================================================
                                            if (response.StatusCode ==
                                                HttpStatusCode.TooManyRequests)
                                            {
                                                int waitSeconds = 3;

                                                if (response.Headers.TryGetValues(
                                                    "Retry-After",
                                                    out var values))
                                                {
                                                    string retryAfter =
                                                        values.FirstOrDefault();

                                                    if (int.TryParse(
                                                        retryAfter,
                                                        out int parsedSeconds))
                                                    {
                                                        waitSeconds =
                                                            Math.Max(
                                                                1,
                                                                parsedSeconds);
                                                    }
                                                }

                                                await SqlQueryLogsAsync(
                                                    nameof(
                                                        CallApiOutboundCampaignByContactListId),
                                                    $"429 RATE LIMIT. " +
                                                    $"{requestKey}. " +
                                                    $"Attempt={retry}/{maxRetry}. " +
                                                    $"Waiting={waitSeconds}s.");

                                                await Task.Delay(
                                                    TimeSpan.FromSeconds(
                                                        waitSeconds),
                                                    ct);

                                                continue;
                                            }

                                            // =================================================
                                            // OTHER HTTP ERRORS
                                            //
                                            // 400, 401, 403, 405, 409, 429, 500...
                                            // Everything except 404 is retried.
                                            // =================================================
                                            if (!response.IsSuccessStatusCode)
                                            {
                                                string errorBody =
                                                    await response.Content
                                                        .ReadAsStringAsync(ct);

                                                await SqlQueryLogsAsync(
                                                    nameof(
                                                        CallApiOutboundCampaignByContactListId),
                                                    $"HTTP {statusCode}. " +
                                                    $"{requestKey}. " +
                                                    $"Attempt={retry}/{maxRetry}. " +
                                                    $"Response={errorBody}");

                                                if (retry < maxRetry)
                                                {
                                                    int waitSeconds =
                                                        Math.Min(
                                                            60,
                                                            2 * retry);

                                                    await Task.Delay(
                                                        TimeSpan.FromSeconds(
                                                            waitSeconds),
                                                        ct);
                                                }

                                                continue;
                                            }
                                        }
                                        catch (HttpRequestException ex)
                                        {
                                            await SqlQueryLogsAsync(
                                                nameof(
                                                    CallApiOutboundCampaignByContactListId),
                                                $"HTTP REQUEST ERROR. " +
                                                $"{requestKey}. " +
                                                $"Attempt={retry}/{maxRetry}. " +
                                                $"Error={ex.Message}");

                                            if (retry < maxRetry)
                                            {
                                                await Task.Delay(
                                                    TimeSpan.FromSeconds(
                                                        Math.Min(
                                                            60,
                                                            retry * 2)),
                                                    ct);
                                            }
                                        }
                                        catch (TaskCanceledException ex)
                                        {
                                            await SqlQueryLogsAsync(
                                                nameof(
                                                    CallApiOutboundCampaignByContactListId),
                                                $"REQUEST TIMEOUT. " +
                                                $"{requestKey}. " +
                                                $"Attempt={retry}/{maxRetry}. " +
                                                $"Error={ex.Message}");

                                            if (retry < maxRetry)
                                            {
                                                await Task.Delay(
                                                    TimeSpan.FromSeconds(5),
                                                    ct);
                                            }
                                        }
                                        catch (Exception ex)
                                        {
                                            await SqlQueryLogsAsync(
                                                nameof(
                                                    CallApiOutboundCampaignByContactListId),
                                                $"API EXCEPTION. " +
                                                $"{requestKey}. " +
                                                $"Attempt={retry}/{maxRetry}. " +
                                                $"Error={ex.Message}");

                                            if (retry < maxRetry)
                                            {
                                                await Task.Delay(
                                                    TimeSpan.FromSeconds(5),
                                                    ct);
                                            }
                                        }
                                    }

                                    // =================================================
                                    // CHECK 404
                                    //
                                    // If API returned 404, skip this page/filter
                                    // and continue with the next one.
                                    // =================================================
                                    if (!apiSuccess &&
                                        string.IsNullOrWhiteSpace(json))
                                    {
                                        await SqlQueryLogsAsync(
                                            nameof(
                                                CallApiOutboundCampaignByContactListId),
                                            $"SKIPPING FAILED REQUEST. " +
                                            $"{requestKey}. " +
                                            $"API failed after {maxRetry} attempts.");

                                        break;
                                    }

                                    // =================================================
                                    // EMPTY RESPONSE
                                    // =================================================
                                    if (string.IsNullOrWhiteSpace(json))
                                    {
                                        await SqlQueryLogsAsync(
                                            nameof(
                                                CallApiOutboundCampaignByContactListId),
                                            $"EMPTY RESPONSE. " +
                                            $"{requestKey}");

                                        break;
                                    }

                                    // =================================================
                                    // GET PAGE COUNT
                                    // =================================================
                                    try
                                    {
                                        using var doc =
                                            JsonDocument.Parse(json);

                                        if (pageNumber == 1)
                                        {
                                            if (doc.RootElement.TryGetProperty(
                                                "pageCount",
                                                out JsonElement pageCountElement))
                                            {
                                                pageCount =
                                                    pageCountElement.GetInt32();
                                            }
                                            else
                                            {
                                                pageCount = 0;
                                            }

                                            if (pageCount == 0)
                                            {
                                                await SqlQueryLogsAsync(
                                                    nameof(
                                                        CallApiOutboundCampaignByContactListId),
                                                    $"NO DATA. {requestKey}");

                                                break;
                                            }
                                        }
                                    }
                                    catch (JsonException ex)
                                    {
                                        await SqlQueryLogsAsync(
                                            nameof(
                                                CallApiOutboundCampaignByContactListId),
                                            $"INVALID JSON. " +
                                            $"{requestKey}. " +
                                            $"Error={ex.Message}");

                                        break;
                                    }

                                    // =================================================
                                    // DATABASE SAVE
                                    // =================================================
                                    bool saveSuccess = false;

                                    for (int saveRetry = 1;
                                         saveRetry <= maxRetry;
                                         saveRetry++)
                                    {
                                        try
                                        {
                                            using var sqlConn =
                                                new SqlConnection(
                                                    _connectionString);


                                            await sqlConn.OpenAsync(ct);

                                            await CallApiOutboundCampaignData(
                                                sqlConn,
                                                json,
                                                contactListId);

                                            saveSuccess = true;

                                            await SqlQueryLogsAsync(
                                                nameof(
                                                    CallApiOutboundCampaignByContactListId),
                                                $"DATABASE SAVE SUCCESS. " +
                                                $"{requestKey}. " +
                                                $"SaveAttempt={saveRetry}/{maxRetry}");

                                            break;
                                        }
                                        catch (Exception ex)
                                        {
                                            await SqlQueryLogsAsync(
                                                nameof(
                                                    CallApiOutboundCampaignByContactListId),
                                                $"DATABASE SAVE FAILED. " +
                                                $"{requestKey}. " +
                                                $"SaveAttempt={saveRetry}/{maxRetry}. " +
                                                $"Error={ex.Message}");

                                            if (saveRetry < maxRetry)
                                            {
                                                await Task.Delay(
                                                    TimeSpan.FromSeconds(
                                                        Math.Min(
                                                            30,
                                                            saveRetry * 2)),
                                                    ct);
                                            }
                                        }
                                    }

                                    // =================================================
                                    // DATABASE FAILED
                                    //
                                    // Do NOT move to next page.
                                    // =================================================
                                    if (!saveSuccess)
                                    {
                                        await SqlQueryLogsAsync(
                                            nameof(
                                                CallApiOutboundCampaignByContactListId),
                                            $"DATABASE SAVE FAILED AFTER " +
                                            $"{maxRetry} ATTEMPTS. " +
                                            $"{requestKey}. " +
                                            $"Stopping filter.");

                                        break;
                                    }

                                    // =================================================
                                    // COMPLETE SUCCESS
                                    // =================================================
                                    await SqlQueryLogsAsync(
                                        nameof(
                                            CallApiOutboundCampaignByContactListId),
                                        $"COMPLETE SUCCESS. " +
                                        $"{requestKey}. " +
                                        $"Moving to next page.");

                                    pageNumber++;
                                }
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        await SqlQueryLogsAsync(
                            nameof(
                                CallApiOutboundCampaignByContactListId),
                            $"Cancelled. ContactList={contactListId}");
                    }
                    catch (Exception ex)
                    {
                        await SqlQueryLogsAsync(
                            nameof(
                                CallApiOutboundCampaignByContactListId),
                            $"ContactList={contactListId}. " +
                            $"Error={ex}");
                    }
                });
        }





        public async Task CallApiOutboundCampaignDatav1(SqlConnection connection, string json, string contactListId)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                string id = root.TryGetProperty("id", out var idProp)
                    ? idProp.ToString()
                    : "";

                // Build data column
                string fields = "";

                if (root.TryGetProperty("data", out var data))
                {
                    fields = string.Join(",",
                        data.EnumerateObject()
                            .Select(x => $"{x.Name}|{x.Value}"));
                }

                // Campaign
                string campaignId = null;
                string campaignName = null;

                if (root.TryGetProperty("callRecords", out var callRecords) &&
                    callRecords.TryGetProperty("MIN", out var min) &&
                    min.TryGetProperty("campaign", out var campaign))
                {
                    campaignId =
                        campaign.TryGetProperty("id", out var cid)
                            ? cid.ToString()
                            : null;

                    campaignName =
                        campaign.TryGetProperty("name", out var cname)
                            ? cname.ToString()
                            : null;
                }

                const string sql = @"
                    INSERT INTO OutboundCampaignData
                    (
                        ContactId,
                        ContactListId,
                        Data,
                        LastResultMIN,
                        LastAttemptMIN,
                        LastResultMOBILE,
                        LastAttemptMOBILE,
                        CampaignId,
                        CampaignName
                    )
                    VALUES
                    (
                        @Id,
                        @ContactListId,
                        @Data,
                        '',
                        NULL,
                        '',
                        NULL,
                        @CampaignId,
                        @CampaignName
                    )";

                using var cmd = new SqlCommand(sql, connection);

                cmd.CommandTimeout = 300;

                cmd.Parameters.AddWithValue("@Id", id);
                cmd.Parameters.AddWithValue("@ContactListId", contactListId);
                cmd.Parameters.AddWithValue("@Data", fields);
                cmd.Parameters.AddWithValue(
                    "@CampaignId",
                    (object)campaignId ?? DBNull.Value);
                cmd.Parameters.AddWithValue(
                    "@CampaignName",
                    (object)campaignName ?? DBNull.Value);

                await cmd.ExecuteNonQueryAsync();


            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("CallApiOutboundCampaignData", "Error: " + ex.Message + ", " + contactListId);
            }

        }


    public async Task CallApiOutboundCampaignData(
            SqlConnection connection,
            string json,
            string contactListId)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                if (!root.TryGetProperty("entities", out var entities))
                    return;

                foreach (var e in entities.EnumerateArray())
                {
                    string id = e.TryGetProperty("id", out var idProp)
                        ? idProp.ToString()
                        : "";

                    // Build data column
                    string fields = "";

                    if (e.TryGetProperty("data", out var data))
                    {
                        fields = string.Join(",",
                            data.EnumerateObject()
                                .Select(x => $"{x.Name}|{x.Value}"));
                    }

                    // Campaign
                    string campaignId = null;
                    string campaignName = null;

                    if (e.TryGetProperty("callRecords", out var callRecords) &&
                        callRecords.TryGetProperty("MIN", out var min) &&
                        min.TryGetProperty("campaign", out var campaign))
                    {
                        campaignId =
                            campaign.TryGetProperty("id", out var cid)
                                ? cid.ToString()
                                : null;

                        campaignName =
                            campaign.TryGetProperty("name", out var cname)
                                ? cname.ToString()
                                : null;
                    }

                    const string sql = @"
                        INSERT INTO OutboundCampaignData
                        (
                            ContactId,
                            ContactListId,
                            Data,
                            LastResultMIN,
                            LastAttemptMIN,
                            LastResultMOBILE,
                            LastAttemptMOBILE,
                            CampaignId,
                            CampaignName
                        )
                        VALUES
                        (
                            @Id,
                            @ContactListId,
                            @Data,
                            '',
                            NULL,
                            '',
                            NULL,
                            @CampaignId,
                            @CampaignName
                        )";

                    using var cmd = new SqlCommand(sql, connection);

                    cmd.CommandTimeout = 300;

                    cmd.Parameters.AddWithValue("@Id", id);
                    cmd.Parameters.AddWithValue("@ContactListId", contactListId);
                    cmd.Parameters.AddWithValue("@Data", fields);

                    cmd.Parameters.AddWithValue(
                        "@CampaignId",
                        (object)campaignId ?? DBNull.Value);

                    cmd.Parameters.AddWithValue(
                        "@CampaignName",
                        (object)campaignName ?? DBNull.Value);

                    await cmd.ExecuteNonQueryAsync();

                    await SqlQueryAsync(
                        "UPDATE APIAttempCount " +
                        "SET AttemptCount = AttemptCount + 1 " +
                        "WHERE APIName = 'CallApiOutboundCampaignData'");
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    "CallApiOutboundCampaignData",
                    $"Error: {ex.Message}, {contactListId}");
            }
        }



        private async Task CallApiOutboundCampaignByContactListIdAgentless(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            while (true)
            {
                var contacts = new List<(string ContactListId, string DialerContactId)>();

                using (var conn = new SqlConnection(_connectionString))
                {
                    await conn.OpenAsync();

                    var cmd = new SqlCommand(@"
                        SELECT DISTINCT
                            dialerContactListId,
                            dialerContactId
                        FROM Attributes
                        WHERE dialerCampaignId IN
                        (
                            SELECT CampaignId
                            FROM OutboundCampaign
                            WHERE DialingMode = 'agentless'
                        )
                        AND dialerContactId NOT IN
                        (
                            SELECT ContactId
                            FROM OutboundCampaignDataAgentless
                        )", conn);

                    cmd.CommandTimeout = 300;

                    using var reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        contacts.Add((
                            reader.GetString(0),
                            reader.GetString(1)
                        ));
                    }
                }

                // Nothing left to process
                if (contacts.Count == 0)
                    break;

                foreach (var contact in contacts)
                {
                    try
                    {
                        var url =
                            $"{API_URL_BASE_URL}/outbound/contactlists/{contact.ContactListId}/contacts/{contact.DialerContactId}";

                        var response = await client.GetAsync(url);

                        if (!response.IsSuccessStatusCode)
                            continue;

                        var json = await response.Content.ReadAsStringAsync();

                        await CallApiOutboundCampaignDataAgentless(
                            json,
                            contact.ContactListId,
                            contact.DialerContactId);
                    }
                    catch (Exception ex)
                    {
                        await SqlQueryLogsAsync(
                            "CallApiOutboundCampaignByContactListId",
                            $"Error: {ex.Message}, ContactListId={contact.ContactListId}, DialerContactId={contact.DialerContactId}");
                    }
                }
            }
        }

        public async Task CallApiOutboundCampaignDataAgentless(string json, string contactListId, string DialerContactId)
        {

            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                var id = root.GetProperty("id").GetString();

                string fields = "";
                string MINlastResult = null;
                DateTime? MINlastAttempt = null;
                string MOBILElastResult = null;
                DateTime? MOBILElastAttempt = null;

                string campaignId = null;
                string campaignName = null;

                if (root.TryGetProperty("data", out JsonElement dataBlock))
                {
                    foreach (JsonProperty customField in dataBlock.EnumerateObject())
                    {
                        fields += $"{customField.Name}|{customField.Value},";
                    }
                    fields = fields.TrimEnd(',');
                }


                if (root.TryGetProperty("callRecords", out JsonElement callRecordsBlock))
                {
                    if (callRecordsBlock.TryGetProperty("MIN", out JsonElement MINBlock))
                    {
                        MINlastResult = GetString(MINBlock, "lastResult");
                        MINlastAttempt = GetDate(MINBlock, "lastAttempt");

                        if (MINBlock.TryGetProperty("campaign", out JsonElement campaignBlock))
                        {
                            campaignId = GetString(campaignBlock, "id");
                            campaignName = GetString(campaignBlock, "name");
                        }
                    }

                    if (callRecordsBlock.TryGetProperty("MOBILE", out JsonElement MOBILEBlock))
                    {
                        MOBILElastResult = GetString(MOBILEBlock, "lastResult");
                        MOBILElastAttempt = GetDate(MOBILEBlock, "lastAttempt");
                    }
                }

                string OCDataSQLQry = "INSERT INTO OutboundCampaignDataAgentless " +
                      " SELECT '" + id + "', '" + contactListId + "', '" + fields.Replace("'", "''") + "', '" + MINlastResult + "', " + ToSqlDateTime(MINlastAttempt) + ", '" + MOBILElastResult + "', " + ToSqlDateTime(MOBILElastAttempt) + ", '" + campaignId + "', '" + campaignName + "'";


                SqlCommand OCDataSQLCmd = new SqlCommand(OCDataSQLQry, connection);

                OCDataSQLCmd.CommandTimeout = 300;

                OCDataSQLCmd.ExecuteNonQuery();

                await SqlQueryAsync("update APIAttempCount set AttemptCount = AttemptCount + 1 where APIName = 'CallApiOutboundCampaignDataAgentless'");

            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("CallApiOutboundCampaignData", "Error: " + ex.Message + ", " + contactListId);
            }



        }

        #endregion

        #region Conversationv2

        private async Task CallApiConversationv2(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxParallel = 10;
            const int delayMs = 300;
            const int maxAttempts = 10;

            // Track conversations that are permanently unavailable
            // during this execution.
            var deadConversationIds =
                new ConcurrentDictionary<string, byte>();

            while (true)
            {
                var conversationIds = new List<string>();

                // =========================================================
                // GET NEXT 100 UNPROCESSED CONVERSATIONS
                // =========================================================
                using (var conn = new SqlConnection(_connectionString))
                {
                    await conn.OpenAsync();

                    var cmd = new SqlCommand(@"
                        SELECT TOP (1000)
                            c.ConversationId
                        FROM ConversationsID c
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM Conversations t
                            WHERE t.ConversationId = c.ConversationId
                        )
                        ORDER BY NEWID();
                    ", conn);

                    cmd.CommandTimeout = 300;

                    using var reader =
                        await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        string id = reader.GetString(0);

                        if (!deadConversationIds.ContainsKey(id))
                        {
                            conversationIds.Add(id);
                        }
                    }
                }

                // =========================================================
                // NOTHING LEFT TO PROCESS
                // =========================================================
                if (conversationIds.Count == 0)
                    break;

                int processedInThisBatch = 0;

                // =========================================================
                // PARALLEL PROCESSING
                // =========================================================
                await Parallel.ForEachAsync(
                    conversationIds,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel
                    },
                    async (conversationId, ct) =>
                    {
                        bool downloadSuccess = false;
                        int currentAttempt = 0;

                        while (!downloadSuccess &&
                               currentAttempt < maxAttempts)
                        {
                            currentAttempt++;

                            try
                            {
                                // =============================================
                                // SMALL PACING DELAY
                                // =============================================
                                await Task.Delay(delayMs, ct);

                                // =============================================
                                // API REQUEST
                                // =============================================
                                var response =
                                    await client.GetAsync(
                                        $"{API_URL_CONVERSIONSV2}{conversationId}",
                                        ct);

                                int statusCode =
                                    (int)response.StatusCode;

                                // =============================================
                                // 429 RATE LIMIT
                                // =============================================
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    int backoffSeconds = 3;

                                    if (response.Headers.TryGetValues(
                                        "Retry-After",
                                        out var values))
                                    {
                                        var firstValue =
                                            values.FirstOrDefault();

                                        if (int.TryParse(
                                            firstValue,
                                            out int parsedSeconds))
                                        {
                                            backoffSeconds =
                                                parsedSeconds;
                                        }
                                    }

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2",
                                        $"429 Rate Limit. " +
                                        $"ConversationId={conversationId}. " +
                                        $"Attempt={currentAttempt}/{maxAttempts}. " +
                                        $"Retrying in {backoffSeconds}s."
                                    );

                                    response.Dispose();

                                    if (currentAttempt >= maxAttempts)
                                    {
                                        deadConversationIds.TryAdd(
                                            conversationId,
                                            0);

                                        break;
                                    }

                                    await Task.Delay(
                                        TimeSpan.FromSeconds(
                                            backoffSeconds),
                                        ct);

                                    continue;
                                }

                                // =============================================
                                // 404 - CONVERSATION DOES NOT EXIST
                                // =============================================
                                if (response.StatusCode ==
                                    HttpStatusCode.NotFound)
                                {
                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2",
                                        $"404 NotFound. " +
                                        $"ConversationId={conversationId} " +
                                        $"is no longer accessible."
                                    );

                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    response.Dispose();

                                    break;
                                }

                                // =============================================
                                // HTTP 400 - 500
                                // =============================================
                                if (statusCode >= 400 &&
                                    statusCode <= 500)
                                {
                                    string errorBody =
                                        await response.Content
                                            .ReadAsStringAsync(ct);

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2",
                                        $"HTTP {statusCode}. " +
                                        $"ConversationId={conversationId}. " +
                                        $"Attempt={currentAttempt}/{maxAttempts}. " +
                                        $"Response={errorBody}"
                                    );

                                    response.Dispose();

                                    // Stop retrying after max attempts
                                    if (currentAttempt >= maxAttempts)
                                    {
                                        await SqlQueryLogsAsync(
                                            "CallApiConversationv2",
                                            $"FAILED after {maxAttempts} attempts. " +
                                            $"ConversationId={conversationId}. " +
                                            $"HTTP {statusCode}."
                                        );

                                        deadConversationIds.TryAdd(
                                            conversationId,
                                            0);

                                        break;
                                    }

                                    // =====================================
                                    // EXPONENTIAL BACKOFF
                                    //
                                    // Attempt 1 = 2 sec
                                    // Attempt 2 = 4 sec
                                    // Attempt 3 = 8 sec
                                    // Attempt 4 = 16 sec
                                    // Attempt 5 = 32 sec
                                    // =====================================
                                    int waitSeconds =
                                        (int)Math.Pow(
                                            2,
                                            currentAttempt - 1) * 2;

                                    await Task.Delay(
                                        TimeSpan.FromSeconds(
                                            waitSeconds),
                                        ct);

                                    continue;
                                }

                                // =============================================
                                // SUCCESS
                                // =============================================
                                response.EnsureSuccessStatusCode();

                                string json =
                                    await response.Content
                                        .ReadAsStringAsync(ct);

                                if (string.IsNullOrWhiteSpace(json))
                                {
                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2",
                                        $"Empty response. " +
                                        $"ConversationId={conversationId}"
                                    );

                                    break;
                                }

                                // =============================================
                                // SAVE TO DATABASE
                                // =============================================
                                using var sqlConn =
                                    new SqlConnection(
                                        _connectionString);

                                await sqlConn.OpenAsync(ct);

                                await SaveToDatabaseConversationv2(
                                    sqlConn,
                                    json);

                                downloadSuccess = true;

                                Interlocked.Increment(
                                    ref processedInThisBatch);
                            }
                            catch (HttpRequestException ex)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiConversationv2",
                                    $"HttpRequestException. " +
                                    $"ConversationId={conversationId}. " +
                                    $"Attempt={currentAttempt}/{maxAttempts}. " +
                                    $"Error={ex.Message}"
                                );

                                if (currentAttempt >= maxAttempts)
                                {
                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    break;
                                }

                                int waitSeconds =
                                    (int)Math.Pow(
                                        2,
                                        currentAttempt - 1) * 2;

                                await Task.Delay(
                                    TimeSpan.FromSeconds(
                                        waitSeconds),
                                    ct);
                            }
                            catch (TaskCanceledException ex)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiConversationv2",
                                    $"Request timeout/cancelled. " +
                                    $"ConversationId={conversationId}. " +
                                    $"Attempt={currentAttempt}/{maxAttempts}. " +
                                    $"Error={ex.Message}"
                                );

                                if (currentAttempt >= maxAttempts)
                                {
                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    break;
                                }

                                await Task.Delay(
                                    TimeSpan.FromSeconds(2),
                                    ct);
                            }
                            catch (Exception ex)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiConversationv2",
                                    $"Exception. " +
                                    $"ConversationId={conversationId}. " +
                                    $"Attempt={currentAttempt}/{maxAttempts}. " +
                                    $"Error={ex.Message}"
                                );

                                if (currentAttempt >= maxAttempts)
                                {
                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    break;
                                }

                                int waitSeconds =
                                    (int)Math.Pow(
                                        2,
                                        currentAttempt - 1) * 2;

                                await Task.Delay(
                                    TimeSpan.FromSeconds(
                                        waitSeconds),
                                    ct);
                            }
                        }
                    });

                // =========================================================
                // SAFETY EXIT
                // =========================================================
                if (processedInThisBatch == 0 &&
                    deadConversationIds.Count >=
                    conversationIds.Count)
                {
                    await SqlQueryLogsAsync(
                        "CallApiConversationv2",
                        "Batch safety exit triggered. " +
                        "All retrieved conversation IDs failed " +
                        "or are no longer available.");

                    break;
                }
            }
        }


        private async Task CallApiConversationv2INTRA(string token)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxParallel = 10;
            const int delayMs = 300;
            const int maxAttempts = 10;

            // Track consistently failing conversation IDs
            // to prevent infinite SQL loops during this run.
            var deadConversationIds =
                new ConcurrentDictionary<string, byte>();

            while (true)
            {
                var conversationIds = new List<string>();

                // Get conversation IDs
                using (var conn = new SqlConnection(_connectionString))
                {
                    await conn.OpenAsync();

                    using var cmd = new SqlCommand(
                        "EXEC OSSGenesysINTRAGetConversationID",
                        conn);

                    cmd.CommandTimeout = 300;

                    using var reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        string id = reader.GetString(0);

                        if (!deadConversationIds.ContainsKey(id))
                        {
                            conversationIds.Add(id);
                        }
                    }
                }

                // Nothing left to process
                if (conversationIds.Count == 0)
                    break;

                int processedInThisBatch = 0;

                await Parallel.ForEachAsync(
                    conversationIds,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel
                    },
                    async (conversationId, ct) =>
                    {
                        bool downloadSuccess = false;
                        int currentAttempt = 0;

                        while (!downloadSuccess &&
                               currentAttempt < maxAttempts)
                        {
                            currentAttempt++;

                            try
                            {
                                // Small pacing delay
                                await Task.Delay(delayMs, ct);

                                var url =
                                    $"{API_URL_CONVERSIONSV2}{conversationId}";

                                using var response =
                                    await client.GetAsync(url, ct);

                                int statusCode = (int)response.StatusCode;

                                // =====================================================
                                // 1. HTTP 429 - RATE LIMIT
                                // =====================================================
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    int backoffSeconds = 3;

                                    if (response.Headers.TryGetValues(
                                        "Retry-After",
                                        out var values))
                                    {
                                        var firstValue =
                                            values.FirstOrDefault();

                                        if (int.TryParse(
                                            firstValue,
                                            out int parsedSeconds))
                                        {
                                            backoffSeconds =
                                                parsedSeconds;
                                        }
                                    }

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2INTRA",
                                        $"429 Rate Limit. " +
                                        $"ConversationId={conversationId}, " +
                                        $"Wait={backoffSeconds}s, " +
                                        $"Attempt={currentAttempt}/{maxAttempts}");

                                    if (currentAttempt < maxAttempts)
                                    {
                                        await Task.Delay(
                                            TimeSpan.FromSeconds(
                                                backoffSeconds),
                                            ct);
                                    }

                                    continue;
                                }

                                // =====================================================
                                // 2. HTTP 404 - NOT FOUND
                                // =====================================================
                                if (response.StatusCode ==
                                    HttpStatusCode.NotFound)
                                {
                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2INTRA",
                                        $"404 NotFound. " +
                                        $"ConversationId={conversationId} " +
                                        $"is no longer accessible.");

                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    return;
                                }

                                // =====================================================
                                // 3. HTTP 400-499
                                // =====================================================
                                if (statusCode >= 400 &&
                                    statusCode < 500)
                                {
                                    string errorBody =
                                        await response.Content
                                            .ReadAsStringAsync(ct);

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2INTRA",
                                        $"HTTP {statusCode}. " +
                                        $"ConversationId={conversationId}, " +
                                        $"Attempt={currentAttempt}/{maxAttempts}. " +
                                        $"Body={errorBody}");

                                    if (currentAttempt < maxAttempts)
                                    {
                                        // Retry 4xx
                                        int retryDelay =
                                            2000 * currentAttempt;

                                        await Task.Delay(
                                            retryDelay,
                                            ct);

                                        continue;
                                    }

                                    // All attempts exhausted
                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    return;
                                }

                                // =====================================================
                                // 4. HTTP 500-599
                                // =====================================================
                                if (statusCode >= 500 &&
                                    statusCode < 600)
                                {
                                    string errorBody =
                                        await response.Content
                                            .ReadAsStringAsync(ct);

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2INTRA",
                                        $"HTTP {statusCode} Server Error. " +
                                        $"ConversationId={conversationId}, " +
                                        $"Attempt={currentAttempt}/{maxAttempts}. " +
                                        $"Body={errorBody}");

                                    if (currentAttempt < maxAttempts)
                                    {
                                        // Exponential backoff:
                                        // 2s, 4s, 6s, 8s...
                                        int retryDelay =
                                            2000 * currentAttempt;

                                        await Task.Delay(
                                            retryDelay,
                                            ct);

                                        continue;
                                    }

                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    return;
                                }

                                // =====================================================
                                // 5. OTHER UNSUCCESSFUL STATUS
                                // =====================================================
                                if (!response.IsSuccessStatusCode)
                                {
                                    string errorBody =
                                        await response.Content
                                            .ReadAsStringAsync(ct);

                                    await SqlQueryLogsAsync(
                                        "CallApiConversationv2INTRA",
                                        $"HTTP {statusCode}. " +
                                        $"ConversationId={conversationId}, " +
                                        $"Attempt={currentAttempt}/{maxAttempts}. " +
                                        $"Body={errorBody}");

                                    if (currentAttempt < maxAttempts)
                                    {
                                        await Task.Delay(
                                            2000 * currentAttempt,
                                            ct);

                                        continue;
                                    }

                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);

                                    return;
                                }

                                // =====================================================
                                // 6. SUCCESS
                                // =====================================================
                                string json =
                                    await response.Content
                                        .ReadAsStringAsync(ct);

                                using var sqlConn =
                                    new SqlConnection(
                                        _connectionString);

                                await sqlConn.OpenAsync(ct);

                                await SaveToDatabaseConversationv2(
                                    sqlConn,
                                    json);

                                downloadSuccess = true;

                                Interlocked.Increment(
                                    ref processedInThisBatch);
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch (Exception ex)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiConversationv2INTRA",
                                    $"Exception. " +
                                    $"ConversationId={conversationId}, " +
                                    $"Attempt={currentAttempt}/{maxAttempts}. " +
                                    $"Error={ex.Message}");

                                if (currentAttempt < maxAttempts)
                                {
                                    await Task.Delay(
                                        2000 * currentAttempt,
                                        ct);
                                }
                                else
                                {
                                    deadConversationIds.TryAdd(
                                        conversationId,
                                        0);
                                }
                            }
                        }
                    });

                // =============================================================
                // SAFETY EXIT
                // =============================================================
                if (processedInThisBatch == 0 &&
                    deadConversationIds.Count >= conversationIds.Count)
                {
                    await SqlQueryLogsAsync(
                        "CallApiConversationv2INTRA",
                        "Batch safety exit triggered. " +
                        "All retrieved conversation IDs failed or " +
                        "are no longer accessible.");

                    break;
                }
            }
        }


        private async Task CallApiConversationv2RawJSON(string token)
        {
            const int sqlBatchSize = 1000;
            const int maxParallel = 4;
            const int maxRetries = 10;
            const int requestDelayMs = 750;

            using var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            var deadConversationIds = new ConcurrentDictionary<string, byte>();

            // Global API throttling
            var apiLock = new SemaphoreSlim(1, 1);

            while (true)
            {
                var conversationIds = new List<string>();

                using (var conn = new SqlConnection(_connectionString))
                {
                    await conn.OpenAsync();

                    var cmd = new SqlCommand($@"
                        SELECT DISTINCT TOP ({sqlBatchSize})
                            c.ConversationId
                        FROM ConversationsID c
                        WHERE NOT EXISTS
                        (
                            SELECT 1
                            FROM ConversationsIDwRawJSON t
                            WHERE t.ConversationId = c.ConversationId
                            AND EndTime IS NOT NULL
                        )", conn);

                    cmd.CommandTimeout = 300;

                    using var reader = await cmd.ExecuteReaderAsync();

                    while (await reader.ReadAsync())
                    {
                        var id = reader.GetString(0);

                        if (!deadConversationIds.ContainsKey(id))
                            conversationIds.Add(id);
                    }
                }

                if (conversationIds.Count == 0)
                    break;

                int processed = 0;

                await Parallel.ForEachAsync(
                    conversationIds,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel
                    },
                    async (conversationId, ct) =>
                    {
                        for (int retry = 1; retry <= maxRetries; retry++)
                        {
                            try
                            {
                                HttpResponseMessage response;

                                // ---------- Global API throttle ----------
                                await apiLock.WaitAsync(ct);

                                try
                                {
                                    await Task.Delay(requestDelayMs, ct);

                                    response = await client.GetAsync(
                                        $"{API_URL_CONVERSIONSV2}{conversationId}",
                                        ct);
                                }
                                finally
                                {
                                    apiLock.Release();
                                }

                                // ---------- Success ----------
                                if (response.IsSuccessStatusCode)
                                {
                                    string json = await response.Content.ReadAsStringAsync(ct);

                                    using var sqlConn = new SqlConnection(_connectionString);
                                    await sqlConn.OpenAsync(ct);

                                    await SaveToDatabaseConversationv2RawJSON(
                                        sqlConn,
                                        json);

                                    Interlocked.Increment(ref processed);

                                    return;
                                }

                                // ---------- 429 ----------
                                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                                {
                                    int waitSeconds;

                                    if (response.Headers.RetryAfter?.Delta != null)
                                    {
                                        waitSeconds =
                                            (int)response.Headers.RetryAfter.Delta.Value.TotalSeconds;
                                    }
                                    else
                                    {
                                        waitSeconds =
                                            Math.Min(60, (int)Math.Pow(2, retry));
                                    }

                                    await SqlQueryLogsAsync(
                                        nameof(CallApiConversationv2RawJSON),
                                        $"429 Conversation={conversationId}. Retry {retry}/{maxRetries}. Waiting {waitSeconds}s.");

                                    await Task.Delay(
                                        TimeSpan.FromSeconds(waitSeconds),
                                        ct);

                                    continue;
                                }

                                // ---------- 404 ----------
                                if (response.StatusCode == HttpStatusCode.NotFound)
                                {
                                    deadConversationIds.TryAdd(conversationId, 0);

                                    await SqlQueryLogsAsync(
                                        nameof(CallApiConversationv2RawJSON),
                                        $"404 Conversation={conversationId}");

                                    return;
                                }

                                // ---------- Other HTTP ----------
                                string error =
                                    await response.Content.ReadAsStringAsync(ct);

                                await SqlQueryLogsAsync(
                                    nameof(CallApiConversationv2RawJSON),
                                    $"HTTP {(int)response.StatusCode} Conversation={conversationId}: {error}");

                                deadConversationIds.TryAdd(conversationId, 0);
                                return;
                            }
                            catch (HttpRequestException ex)
                            {
                                await SqlQueryLogsAsync(
                                    nameof(CallApiConversationv2RawJSON),
                                    $"Network Error {conversationId} Retry={retry}: {ex.Message}");
                            }
                            catch (TaskCanceledException ex)
                            {
                                await SqlQueryLogsAsync(
                                    nameof(CallApiConversationv2RawJSON),
                                    $"Timeout {conversationId} Retry={retry}: {ex.Message}");
                            }
                            catch (Exception ex)
                            {
                                await SqlQueryLogsAsync(
                                    nameof(CallApiConversationv2RawJSON),
                                    ex.ToString());

                                deadConversationIds.TryAdd(conversationId, 0);
                                return;
                            }

                            await Task.Delay(
                                TimeSpan.FromSeconds(Math.Min(30, retry * 2)),
                                ct);
                        }

                        deadConversationIds.TryAdd(conversationId, 0);
                    });

                if (processed == 0)
                {
                    await SqlQueryLogsAsync(
                        nameof(CallApiConversationv2RawJSON),
                        "No successful conversations processed. Exiting.");

                    break;
                }
            }
        }

        private async Task CallApiConversationv2Progressive(string token)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            const int maxParallel = 10;
            const int delayMs = 300;
            const int sqlBatchSize = 1000;

            // Track consistently failing or missing IDs in memory during this run 
            // to prevent deleted or archived conversation rows from causing an infinite SQL loop.
            var deadConversationIds = new ConcurrentDictionary<string, byte>();

            while (true)
            {
                var conversationIds = new List<string>();

                using (var conn = new SqlConnection(_connectionString))
                {
                    await conn.OpenAsync();

                    var cmd = new SqlCommand($@"
                        SELECT DISTINCT TOP ({sqlBatchSize})
                            A.ConversationId
                        FROM Participants_Old AS A
                        INNER JOIN Sessions_Old AS B
                            ON A.ParticipantId = B.ParticipantId
                        WHERE EXISTS
                        (
                            SELECT 1
                            FROM OutboundCampaign AS OC
                            WHERE OC.CampaignId = B.SessionsOutboundCampaignId
                              AND OC.DialingMode = 'progressive'
                        ) AND
                        NOT EXISTS
                        (
                            SELECT 1
                            FROM Conversations t
                            WHERE t.ConversationId = A.ConversationId
                        );
                    ", conn);

                    cmd.CommandTimeout = 300;

                    using var reader = await cmd.ExecuteReaderAsync();
                    while (await reader.ReadAsync())
                    {
                        string id = reader.GetString(0);
                        if (!deadConversationIds.ContainsKey(id))
                        {
                            conversationIds.Add(id);
                        }
                    }
                }

                // Exit loop if no unprocessed rows remain
                if (conversationIds.Count == 0)
                    break;

                // Atomic counter to track if at least one entry committed successfully in this batch pass
                int processedInThisBatch = 0;

                await Parallel.ForEachAsync(
                    conversationIds,
                    new ParallelOptions
                    {
                        MaxDegreeOfParallelism = maxParallel
                    },
                    async (conversationId, ct) =>
                    {
                        bool downloadSuccess = false;
                        const int maxAttempts = 3;
                        int currentAttempt = 0;

                        while (!downloadSuccess && currentAttempt < maxAttempts)
                        {
                            currentAttempt++;

                            try
                            {
                                // Stagger delay before each API request
                                await Task.Delay(delayMs, ct);

                                var response = await client.GetAsync($"{API_URL_CONVERSIONSV2}{conversationId}", ct);

                                // 1. Handle Genesys Rate Limiting (HTTP 429 Too Many Requests)
                                if (response.StatusCode == HttpStatusCode.TooManyRequests)
                                {
                                    int backoffSeconds = 3; // Default fallback delay

                                    if (response.Headers.TryGetValues("Retry-After", out var values))
                                    {
                                        var firstValue = System.Linq.Enumerable.FirstOrDefault(values);
                                        if (int.TryParse(firstValue, out int parsedSeconds))
                                        {
                                            backoffSeconds = parsedSeconds;
                                        }
                                    }

                                    await SqlQueryLogsAsync("CallApiConversationv2",
                                        $"Rate Limit Encountered for conversationId={conversationId}. Backing off for {backoffSeconds}s... (Attempt {currentAttempt}/{maxAttempts})");

                                    // Back off for the exact duration requested by Genesys Cloud
                                    await Task.Delay(TimeSpan.FromSeconds(backoffSeconds), ct);
                                    continue; // Retry the current conversation ID lookup
                                }

                                // 2. Handle 404 Not Found (Conversation data purged or expired from the platform)
                                if (response.StatusCode == HttpStatusCode.NotFound)
                                {
                                    await SqlQueryLogsAsync("CallApiConversationv2", $"404 NotFound. Conversation {conversationId} no longer accessible in Genesys. Skipping.");
                                    deadConversationIds.TryAdd(conversationId, 0);
                                    return; // Drop out of task context safely
                                }

                                // Exit for other critical HTTP errors (401 Unauthorized, 500 Server Error)
                                if (!response.IsSuccessStatusCode)
                                {
                                    var errorBody = await response.Content.ReadAsStringAsync(ct);
                                    await SqlQueryLogsAsync("CallApiConversationv2", $"HTTP {(int)response.StatusCode} error for conversationId={conversationId}. Body: {errorBody}");
                                    deadConversationIds.TryAdd(conversationId, 0);
                                    return;
                                }

                                // 3. Process Successful Response Data
                                var json = await response.Content.ReadAsStringAsync(ct);

                                using var sqlConn = new SqlConnection(_connectionString);
                                await sqlConn.OpenAsync(ct);

                                await SaveToDatabaseConversationv2(sqlConn, json);

                                downloadSuccess = true; // Complete while-loop
                                Interlocked.Increment(ref processedInThisBatch);
                            }
                            catch (Exception ex)
                            {
                                await SqlQueryLogsAsync("CallApiConversationv2",
                                    $"Exception on conversationId={conversationId} (Attempt {currentAttempt}/{maxAttempts}): {ex.Message}");

                                if (currentAttempt < maxAttempts)
                                {
                                    await Task.Delay(2000, ct); // 2-second buffer before non-429 connection exception retry
                                }
                                else
                                {
                                    deadConversationIds.TryAdd(conversationId, 0);
                                }
                            }
                        }
                    });

                // Loop Break Safety Valve: If an entire batch is pulled but nothing could write or advance state,
                // stop execution to keep your program from running in an infinite CPU loop.
                if (processedInThisBatch == 0 && deadConversationIds.Count >= conversationIds.Count)
                {
                    await SqlQueryLogsAsync("CallApiConversationv2", "Batch safety exit triggered. All retrieved conversation IDs are dead or missing.");
                    break;
                }
            }
        }

        private async Task SaveToDatabaseConversationv2RawJSON(SqlConnection connection, string json)
        {

            var doc = JsonDocument.Parse(json);

            var root = doc.RootElement;

            var conversationId = Guid.Parse(root.GetProperty("id").GetString());

            try
            {

                string conversationsSQLQry = "INSERT INTO ConversationsIDwRawJSON " +
                    " SELECT '" + conversationId + "', '" + json + "'";

                SqlCommand conversationsSQLCmd = new SqlCommand(conversationsSQLQry, connection);

                conversationsSQLCmd.CommandTimeout = 300;

                conversationsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseConversationv2", "Error: " + ex.Message);
            }
        }

        private async Task SaveToDatabaseConversationv2(SqlConnection connection, string json)
        {

            json = json.Replace("'", "");

            using JsonDocument doc = JsonDocument.Parse(json);

            JsonElement root = doc.RootElement;

            var conversationId = Guid.Parse(root.GetProperty("id").GetString());

            try
            {
                DateTime? startTime = GetDate(root, "startTime");
                DateTime? endTime = GetDate(root, "endTime");
                string address = GetString(root, "address");
                string RecordingState = GetString(root, "recordingState");
                string UtilizationLabelId = GetString(root, "utilizationLabelId");
                string SelfUri = GetString(root, "selfUri");
                string rawJson = json;

                string conversationsSQLQry = "INSERT INTO Conversations " +
                    " SELECT '" + conversationId + "', " + ToSqlDateTime(startTime) + ", " + ToSqlDateTime(endTime) + ", '" + address
                    + "', '" + RecordingState + "', '" + UtilizationLabelId + "', '" + SelfUri + "', '', getdate()";

                SqlCommand conversationsSQLCmd = new SqlCommand(conversationsSQLQry, connection);

                conversationsSQLCmd.CommandTimeout = 300;

                conversationsSQLCmd.ExecuteNonQuery();

                if (root.TryGetProperty("participants", out var participants))
                {
                    foreach (var participant in participants.EnumerateArray())
                    {
                        await SaveParticipant(connection, conversationId, participant);
                    }
                }

                if (root.TryGetProperty("recentTransfers", out var recentTransfers))
                {
                    foreach (var recentTransfer in recentTransfers.EnumerateArray())
                    {
                        await SaveRecentTransfers(connection, conversationId, recentTransfer);
                    }
                }

                await SqlQueryAsync("update APIAttempCount set AttemptCount = AttemptCount + 1 where APIName = 'SaveToDatabaseConversationv2'");

            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseConversationv2", "Error: " + conversationId + " : " + ex.Message);
            }
        }



        private async Task SaveParticipant(SqlConnection connection, Guid conversationId, JsonElement p)
        {


            string participantsSQLQry = "";
            string attributesSQLQry = "";
            string wrapupsSQLQry = "";

            try
            {

                Guid participantId = Guid.Parse(p.GetProperty("id").GetString());

                DateTime? startTime = GetDate(p, "startTime");
                DateTime? endTime = GetDate(p, "endTime");
                DateTime? connectedTime = GetDate(p, "connectedTime");
                string name = GetString(p, "name");
                string userId = GetString(p, "userId");
                string userUri = GetString(p, "userUri");
                string externalContactId = GetString(p, "externalContactId");
                string externalContactInitialDivisionId = GetString(p, "externalContactInitialDivisionId");
                string queueId = GetString(p, "queueId");
                string queueName = GetString(p, "queueName");
                string purpose = GetString(p, "purpose");
                string participantType = GetString(p, "participantType");
                string address = GetString(p, "address");
                string ani = GetString(p, "ani");
                string aniName = GetString(p, "aniName");
                string dnis = GetString(p, "dnis");
                DateTime? startAcwTime = GetDate(p, "startAcwTime");
                DateTime? endAcwTime = GetDate(p, "endAcwTime");

                participantsSQLQry = "INSERT INTO Participants " +
                " SELECT '" + participantId + "', '" + conversationId + "', " + ToSqlDateTime(startTime) + ", " + ToSqlDateTime(endTime) + ", " + ToSqlDateTime(connectedTime)
                + ", '" + name + "', '" + userId + "', '" + userUri + "', '" + externalContactId
                + "', '" + externalContactInitialDivisionId + "', '" + queueId + "', '" + queueName + "', '" + purpose
                + "', '" + participantType + "', '" + address + "', '" + ani + "', '" + aniName
                + "', '" + dnis + "', " + ToSqlDateTime(endTime) + ", " + ToSqlDateTime(endTime);


                SqlCommand participantsSQLCmd = new SqlCommand(participantsSQLQry, connection);

                participantsSQLCmd.CommandTimeout = 300;

                participantsSQLCmd.ExecuteNonQuery();

                if (p.TryGetProperty("calls", out var calls))
                {
                    foreach (var call in calls.EnumerateArray())
                    {
                        await SaveCall(connection, participantId, call);
                    }
                }

                if (p.TryGetProperty("callbacks", out var callbacks))
                {
                    foreach (var callback in callbacks.EnumerateArray())
                    {
                        await SaveCallBack(connection, participantId, callback);
                    }
                }

                if (p.TryGetProperty("attributes", out var att) && att.ValueKind == JsonValueKind.Object)
                {
                    using var connectionatt = new SqlConnection(_connectionString);

                    await connectionatt.OpenAsync();

                    string dialerContactId = GetString(att, "dialerContactId");
                    string dialerContactListId = GetString(att, "dialerContactListId");
                    string dialerCampaignId = GetString(att, "dialerCampaignId");
                    string dialerInteractionId = GetString(att, "dialerInteractionId");

                    string ConcernMIN = GetString(att, "ConcernMIN");
                    string Category = GetString(att, "Category");
                    string Product = GetString(att, "Product");
                    string CSATCategory = GetString(att, "CSATCategory");
                    string CallerID = GetString(att, "CallerID");
                    string CallerNumber = GetString(att, "CallerNumber");

                    attributesSQLQry = "INSERT INTO Attributes " +
                    " SELECT '" + conversationId + "', '" + participantId + "', '" + dialerContactId + "', '" + dialerContactListId + "', '" + dialerCampaignId + "', '" + dialerInteractionId + 
                    "', '" +ConcernMIN + "', '" + Category + "', '" + Product + "', '" + CSATCategory + "', '" + CallerID + "', '" + CallerNumber + "'";

                    SqlCommand attributesSQLCmd = new SqlCommand(attributesSQLQry, connectionatt);


                    attributesSQLCmd.CommandTimeout = 300;

                    attributesSQLCmd.ExecuteNonQuery();
                }

                if (p.TryGetProperty("wrapup", out var wrap) && wrap.ValueKind == JsonValueKind.Object)
                {
                    using var connectionwrap = new SqlConnection(_connectionString);

                    await connectionwrap.OpenAsync();

                    string code = GetString(wrap, "code");
                    int? durationSeconds = GetInt(wrap, "durationSeconds");
                    string durationSql = durationSeconds.HasValue ? durationSeconds.Value.ToString(): "NULL";

                    DateTime? endTimewrap = GetDate(wrap, "endTime");

                    wrapupsSQLQry = "INSERT INTO Wrapups_Participant " +
                    " SELECT '" + conversationId + "', '" + participantId + "', '" + code + "', " + durationSql + ", " + ToSqlDateTime(endTime);

                    SqlCommand wraupupsSQLCmd = new SqlCommand(wrapupsSQLQry, connectionwrap);


                    wraupupsSQLCmd.CommandTimeout = 300;

                    wraupupsSQLCmd.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    "SaveParticipant",
                    "ConversationId: " + conversationId +
                    " | Error: " + ex.Message +
                    " | SQL: " + participantsSQLQry +
                    " | SQL: " + attributesSQLQry +
                    " | SQL: " + wrapupsSQLQry);
            }
        }



        private async Task SaveRecentTransfers(SqlConnection connection, Guid conversationId, JsonElement r)
        {
            try
            {
                Guid recentTransfersId = Guid.Parse(r.GetProperty("id").GetString());

                string state = GetString(r, "state");
                DateTime? dateIssued = GetDate(r, "dateIssued");
                string transferType = GetString(r, "transferType");

                Guid? destUserId = null;
                string destAddress = null;
                Guid? initUserId = null;

                if (r.TryGetProperty("destination", out var dest) && dest.ValueKind == JsonValueKind.Object)
                {
                    destUserId = GetGuid(dest, "userId");
                    destAddress = GetString(dest, "address");
                }

                if (r.TryGetProperty("initiator", out var init) && init.ValueKind == JsonValueKind.Object)
                {
                    initUserId = GetGuid(init, "userId");
                }

                string recentTransfersSQLQry = "INSERT INTO RecentTransfers " +
                " SELECT '" + recentTransfersId + "', '" + conversationId + "', '" + state + "', " + ToSqlDateTime(dateIssued)
                + ", '" + transferType + "', '" + destUserId + "', '" + destAddress + "', '" + initUserId + "'";

                SqlCommand recentTransfersSQLCmd = new SqlCommand(recentTransfersSQLQry, connection);

                recentTransfersSQLCmd.CommandTimeout = 300;

                recentTransfersSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveRecentTransfers", "Error: " + ex.Message);
            }

        }

        private async Task SaveCall(SqlConnection connection, Guid participantId, JsonElement c)
        {
            try
            {

                Guid callId = Guid.Parse(c.GetProperty("id").GetString());

                string state = GetString(c, "state");
                string initialState = GetString(c, "initialState");
                string direction = GetString(c, "direction");
                int? muted = GetBoolAsInt(c, "muted");
                int? confined = GetBoolAsInt(c, "confined");
                int? held = GetBoolAsInt(c, "held");
                int? securePause = GetBoolAsInt(c, "securePause");
                string provider = GetString(c, "provider");
                DateTime? connectedTime = GetDate(c, "connectedTime");
                DateTime? disconnectedTime = GetDate(c, "disconnectedTime");
                string disconnectType = GetString(c, "disconnectType");
                string peerId = GetString(c, "peerId");
                string scriptId = GetString(c, "scriptId");
                string transferSource = GetString(c, "transferSource");

                string callsSQLQry = "INSERT INTO Calls " +
                " SELECT 'Calls', '" + callId + "', '" + participantId + "', '" + state + "', '" + initialState + "', '" + direction
                + "', " + muted + ", " + confined + ", " + held + ", " + securePause
                + ", '" + provider + "', " + ToSqlDateTime(connectedTime) + ", " + ToSqlDateTime(disconnectedTime) + ", '" + disconnectType
                + "', '" + peerId + "', '" + scriptId + "', '" + transferSource + "'";

                SqlCommand callsSQLCmd = new SqlCommand(callsSQLQry, connection);

                callsSQLCmd.CommandTimeout = 300;

                callsSQLCmd.ExecuteNonQuery();

                if (c.TryGetProperty("segments", out var segments))
                {
                    foreach (var segment in segments.EnumerateArray())
                    {
                        await SaveSegments(connection, participantId, callId, segment);
                    }
                }

                if (c.TryGetProperty("disconnectReasons", out var disconnectReasons))
                {
                    foreach (var disconnectReason in disconnectReasons.EnumerateArray())
                    {
                        await SaveDisconnectReasons(connection, participantId, callId, disconnectReason);
                    }
                }

                if (c.TryGetProperty("afterCallWork", out var afterCallWorks))
                {
                    if (afterCallWorks.ValueKind == JsonValueKind.Object && afterCallWorks.EnumerateObject().Any())
                    {
                        await SaveAfterCallWorks(connection, participantId, callId, afterCallWorks);
                    }
                }

                if (c.TryGetProperty("wrapup", out var wrapups))
                {
                    if (wrapups.ValueKind == JsonValueKind.Object && wrapups.EnumerateObject().Any())
                    {
                        await SaveWrapups(connection, participantId, callId, wrapups);
                    }
                }

                if (c.TryGetProperty("other", out var others))
                {
                    if (others.ValueKind == JsonValueKind.Object && others.EnumerateObject().Any())
                    {
                        await SaveOthers(connection, participantId, callId, others);
                    }
                }

                if (c.TryGetProperty("self", out var selfs))
                {
                    if (selfs.ValueKind == JsonValueKind.Object && selfs.EnumerateObject().Any())
                    {
                        await SaveSelfs(connection, participantId, callId, selfs);
                    }
                }


                if (c.TryGetProperty("disposition", out var dispositions))
                {
                    if (dispositions.ValueKind == JsonValueKind.Object && dispositions.EnumerateObject().Any())
                    {
                        await SaveDispositions(connection, participantId, callId, dispositions);
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveCall", "Error: " + participantId + " : " + ex.Message);
            }
        }

        private async Task SaveCallBack(SqlConnection connection, Guid participantId, JsonElement c)
        {
            try
            {

                Guid callId = Guid.Parse(c.GetProperty("id").GetString());

                string state = GetString(c, "state");
                string initialState = GetString(c, "initialState");
                string direction = GetString(c, "direction");
                int? muted = GetBoolAsInt(c, "muted");
                int? confined = GetBoolAsInt(c, "confined");
                int? held = GetBoolAsInt(c, "held");
                int? securePause = GetBoolAsInt(c, "securePause");
                string provider = GetString(c, "provider");
                DateTime? connectedTime = GetDate(c, "connectedTime");
                DateTime? disconnectedTime = GetDate(c, "disconnectedTime");
                string disconnectType = GetString(c, "disconnectType");
                string peerId = GetString(c, "peerId");
                string scriptId = GetString(c, "scriptId");
                string transferSource = GetString(c, "transferSource");

                string callsSQLQry = "INSERT INTO Calls " +
                " SELECT 'Callbacks', '" + callId + "', '" + participantId + "', '" + state + "', '" + initialState + "', '" + direction
                + "', " + ToSqlString(muted) + ", " + ToSqlString(confined) + ", " + ToSqlString(held) + ", " + ToSqlString(securePause)
                + ", '" + provider + "', " + ToSqlDateTime(connectedTime) + ", " + ToSqlDateTime(disconnectedTime) + ", '" + disconnectType
                + "', '" + peerId + "', '" + scriptId + "', '" + transferSource + "'";

                SqlCommand callsSQLCmd = new SqlCommand(callsSQLQry, connection);

                callsSQLCmd.CommandTimeout = 300;

                callsSQLCmd.ExecuteNonQuery();

                if (c.TryGetProperty("segments", out var segments))
                {
                    foreach (var segment in segments.EnumerateArray())
                    {
                        await SaveSegments(connection, participantId, callId, segment);
                    }
                }

                Guid? destUserId = null;
                string destAddress = null;
                Guid? initUserId = null;

                if (c.TryGetProperty("destination", out var dest) && dest.ValueKind == JsonValueKind.Object)
                {
                    destUserId = GetGuid(dest, "userId");
                    destAddress = GetString(dest, "address");
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveCallBack", "Error: " + ex.Message);
            }

        }

        private async Task SaveDisconnectReasons(SqlConnection connection, Guid participantId, Guid callId, JsonElement d)
        {
            try
            {
                string type = GetString(d, "type");
                int? code = GetInt(d, "code");
                string phrase = GetString(d, "phrase");

                string disconnectReasonsSQLQry = "INSERT INTO DisconnectReasons " +
                " SELECT '" + participantId + "', '" + callId + "', '" + type + "', " + code + ", '" + phrase + "'";

                SqlCommand disconnectReasonsSQLCmd = new SqlCommand(disconnectReasonsSQLQry, connection);

                disconnectReasonsSQLCmd.CommandTimeout = 300;

                disconnectReasonsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveDisconnectReasons", "Error: " + ex.Message);
            }

        }


        private async Task SaveSegments(SqlConnection connection, Guid participantId, Guid callId, JsonElement s)
        {
            try
            {
                DateTime? startTime = GetDate(s, "startTime");
                DateTime? endTime = GetDate(s, "endTime");
                string type = GetString(s, "type");
                string howEnded = GetString(s, "howEnded");
                string disconnectType = GetString(s, "disconnectType");

                string segmentsSQLQry = "INSERT INTO Segments " +
                " SELECT '" + participantId + "', '" + callId + "', " + ToSqlDateTime(startTime) + ", " + ToSqlDateTime(endTime) + ", '" + type +
                "', '" + howEnded + "', '" + disconnectType + "'";

                SqlCommand segmentsSQLCmd = new SqlCommand(segmentsSQLQry, connection);

                segmentsSQLCmd.CommandTimeout = 300;

                segmentsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveSegments", "Error: " + ex.Message);
            }

        }


        private async Task SaveAfterCallWorks(SqlConnection connection, Guid participantId, Guid callId, JsonElement a)
        {
            try
            {
                DateTime? startTime = GetDate(a, "startTime");
                DateTime? endTime = GetDate(a, "endTime");
                string state = GetString(a, "state");

                string afterCallWorkSQLQry = "INSERT INTO AfterCallWork " +
                " SELECT '" + participantId + "', '" + callId + "', " + ToSqlDateTime(startTime) + ", " + ToSqlDateTime(endTime) + ", '" + state + "'";

                SqlCommand afterCallWorkSQLCmd = new SqlCommand(afterCallWorkSQLQry, connection);

                afterCallWorkSQLCmd.CommandTimeout = 300;

                afterCallWorkSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveAfterCallWorks", "Error: " + ex.Message);
            }

        }


        private async Task SaveWrapups(SqlConnection connection, Guid participantId, Guid callId, JsonElement w)
        {
            try
            {
                string code = GetString(w, "code");
                string name = GetString(w, "name");
                string notes = GetString(w, "notes");
                int? durationSeconds = GetInt(w, "durationSeconds");
                DateTime? endTime = GetDate(w, "endTime");

                string wrapupsSQLQry = "INSERT INTO Wrapups " +
                " SELECT '" + participantId + "', '" + callId + "', '" + code + "', '" + name + "', '" + notes + "', " + durationSeconds + ", " + ToSqlDateTime(endTime);

                SqlCommand wrapupsSQLCmd = new SqlCommand(wrapupsSQLQry, connection);

                wrapupsSQLCmd.CommandTimeout = 300;

                wrapupsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveWrapups", "Error: " + ex.Message);
            }

        }

        private async Task SaveOthers(SqlConnection connection, Guid participantId, Guid callId, JsonElement o)
        {
            try
            {
                string name = GetString(o, "name");
                string nameRaw = GetString(o, "nameRaw");
                string addressNormalized = GetString(o, "addressNormalized");
                string addressRaw = GetString(o, "addressRaw");
                string addressDisplayable = GetString(o, "addressDisplayable");

                string othersSQLQry = "INSERT INTO Others " +
                " SELECT '" + participantId + "', '" + callId + "', '" + name + "', '" + nameRaw + "', '" + addressNormalized + "', '" + addressRaw + "', '" + addressDisplayable + "'";

                SqlCommand othersSQLCmd = new SqlCommand(othersSQLQry, connection);

                othersSQLCmd.CommandTimeout = 300;

                othersSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveOthers", "Error: " + ex.Message);
            }

        }

        private async Task SaveSelfs(SqlConnection connection, Guid participantId, Guid callId, JsonElement s)
        {
            try
            {
                string name = GetString(s, "name");
                string nameRaw = GetString(s, "nameRaw");
                string addressNormalized = GetString(s, "addressNormalized");
                string addressRaw = GetString(s, "addressRaw");
                string addressDisplayable = GetString(s, "addressDisplayable");

                string selfsSQLQry = "INSERT INTO Selfs " +
                " SELECT '" + participantId + "', '" + callId + "', '" + name + "', '" + nameRaw + "', '" + addressNormalized + "', '" + addressRaw + "', '" + addressDisplayable + "'";

                SqlCommand selfsSQLCmd = new SqlCommand(selfsSQLQry, connection);

                selfsSQLCmd.CommandTimeout = 300;

                selfsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveSelfs", "Error: " + ex.Message);
            }
        }

        private async Task SaveDispositions(SqlConnection connection, Guid participantId, Guid callId, JsonElement d)
        {
            try
            {

                string name = GetString(d, "name");
                string analyzer = GetString(d, "analyzer");
                DateTime? detectedSpeechStart = GetDate(d, "detectedSpeechStart");

                string selfsSQLQry = "INSERT INTO Disposition " +
                " SELECT '" + participantId + "', '" + callId + "', '" + name + "', '" + analyzer + "', " + ToSqlDateTime(detectedSpeechStart);

                SqlCommand selfsSQLCmd = new SqlCommand(selfsSQLQry, connection);

                selfsSQLCmd.CommandTimeout = 300;

                selfsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveDispositions", "Error: " + ex.Message);
            }

        }

        private static object Db(object value)
        {
            return value ?? DBNull.Value;
        }

        private static DataTable CreateTable(params (string Name, Type Type)[] columns)
        {
            var table = new DataTable();

            foreach (var c in columns)
                table.Columns.Add(c.Name, c.Type);

            return table;
        }

        private void AddCallData(
            DataTable calls,
            DataTable segments,
            DataTable disconnectReasons,
            DataTable afterCallWork,
            DataTable wrapups,
            DataTable dispositions,
            DataTable selfs,
            DataTable others,
            Guid participantId,
            JsonElement c,
            string callType)
        {
            if (!Guid.TryParse(GetString(c, "id"), out Guid callId))
                return;

            // CALL
            calls.Rows.Add(
                Db(callType),
                callId.ToString(),
                participantId.ToString(),
                Db(GetString(c, "state")),
                Db(GetString(c, "initialState")),
                Db(GetString(c, "direction")),
                Db(GetBoolAsInt(c, "muted")),
                Db(GetBoolAsInt(c, "confined")),
                Db(GetBoolAsInt(c, "held")),
                Db(GetBoolAsInt(c, "securePause")),
                Db(GetString(c, "provider")),
                Db(GetDate(c, "connectedTime")),
                Db(GetDate(c, "disconnectedTime")),
                Db(GetString(c, "disconnectType")),
                Db(GetString(c, "peerId")),
                Db(GetString(c, "scriptId")),
                Db(GetString(c, "transferSource"))
            );

            // SEGMENTS
            if (c.TryGetProperty("segments", out var ss) &&
                ss.ValueKind == JsonValueKind.Array)
            {
                foreach (var s in ss.EnumerateArray())
                {
                    segments.Rows.Add(
                        participantId.ToString(),
                        callId.ToString(),
                        Db(GetDate(s, "startTime")),
                        Db(GetDate(s, "endTime")),
                        Db(GetString(s, "type")),
                        Db(GetString(s, "howEnded")),
                        Db(GetString(s, "disconnectType"))
                    );
                }
            }

            // DISCONNECT REASONS
            if (c.TryGetProperty("disconnectReasons", out var ds) &&
                ds.ValueKind == JsonValueKind.Array)
            {
                foreach (var dr in ds.EnumerateArray())
                {
                    disconnectReasons.Rows.Add(
                        participantId.ToString(),
                        callId.ToString(),
                        Db(GetString(dr, "type")),
                        Db(GetInt(dr, "code")),
                        Db(GetString(dr, "phrase"))
                    );
                }
            }

            // AFTER CALL WORK
            if (c.TryGetProperty("afterCallWork", out var acw) &&
                acw.ValueKind == JsonValueKind.Object)
            {
                afterCallWork.Rows.Add(
                    participantId.ToString(),
                    callId.ToString(),
                    Db(GetDate(acw, "startTime")),
                    Db(GetDate(acw, "endTime")),
                    Db(GetString(acw, "state"))
                );
            }

            // WRAPUP
            if (c.TryGetProperty("wrapup", out var w) &&
                w.ValueKind == JsonValueKind.Object)
            {
                wrapups.Rows.Add(
                    participantId.ToString(),
                    callId.ToString(),
                    Db(GetString(w, "code")),
                    Db(GetString(w, "name")),
                    Db(GetString(w, "notes")),
                    Db(GetInt(w, "durationSeconds")),
                    Db(GetDate(w, "endTime"))
                );
            }

            // DISPOSITION
            if (c.TryGetProperty("disposition", out var d) &&
                d.ValueKind == JsonValueKind.Object)
            {
                dispositions.Rows.Add(
                    participantId.ToString(),
                    callId.ToString(),
                    Db(GetString(d, "name")),
                    Db(GetString(d, "analyzer")),
                    Db(GetDate(d, "detectedSpeechStart"))
                );
            }

            // SELF
            if (c.TryGetProperty("self", out var self) &&
                self.ValueKind == JsonValueKind.Object)
            {
                selfs.Rows.Add(
                    participantId.ToString(),
                    callId.ToString(),
                    Db(GetString(self, "name")),
                    Db(GetString(self, "nameRaw")),
                    Db(GetString(self, "addressNormalized")),
                    Db(GetString(self, "addressRaw")),
                    Db(GetString(self, "addressDisplayable"))
                );
            }

            // OTHER
            if (c.TryGetProperty("other", out var other) &&
                other.ValueKind == JsonValueKind.Object)
            {
                others.Rows.Add(
                    participantId.ToString(),
                    callId.ToString(),
                    Db(GetString(other, "name")),
                    Db(GetString(other, "nameRaw")),
                    Db(GetString(other, "addressNormalized")),
                    Db(GetString(other, "addressRaw")),
                    Db(GetString(other, "addressDisplayable"))
                );
            }
        }

        private async Task SaveToDatabaseConversationv3(
            SqlConnection connection,
            string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var r = doc.RootElement;

                if (!Guid.TryParse(GetString(r, "id"), out Guid conversationId))
                    return;

                // =========================================================
                // TABLES
                // =========================================================

                var conversations = CreateTable(
                    ("ConversationId", typeof(string)),
                    ("StartTime", typeof(DateTime)),
                    ("EndTime", typeof(DateTime)),
                    ("Address", typeof(string)),
                    ("RecordingState", typeof(string)),
                    ("UtilizationLabelId", typeof(string)),
                    ("SelfUri", typeof(string)),
                    ("RawJson", typeof(string)),
                    ("CreatedAt", typeof(DateTime))
                );

                var participants = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("ConversationId", typeof(string)),
                    ("StartTime", typeof(DateTime)),
                    ("EndTime", typeof(DateTime)),
                    ("ConnectedTime", typeof(DateTime)),
                    ("Name", typeof(string)),
                    ("UserId", typeof(string)),
                    ("UserUri", typeof(string)),
                    ("ExternalContactId", typeof(string)),
                    ("ExternalContactInitialDivisionId", typeof(string)),
                    ("QueueId", typeof(string)),
                    ("QueueName", typeof(string)),
                    ("Purpose", typeof(string)),
                    ("ParticipantType", typeof(string)),
                    ("Address", typeof(string)),
                    ("Ani", typeof(string)),
                    ("AniName", typeof(string)),
                    ("Dnis", typeof(string)),
                    ("StartAcwTime", typeof(DateTime)),
                    ("EndAcwTime", typeof(DateTime))
                );

                var calls = CreateTable(
                    ("Type", typeof(string)),
                    ("CallId", typeof(string)),
                    ("ParticipantId", typeof(string)),
                    ("State", typeof(string)),
                    ("InitialState", typeof(string)),
                    ("Direction", typeof(string)),
                    ("Muted", typeof(int)),
                    ("Confined", typeof(int)),
                    ("Held", typeof(int)),
                    ("SecurePause", typeof(int)),
                    ("Provider", typeof(string)),
                    ("ConnectedTime", typeof(DateTime)),
                    ("DisconnectedTime", typeof(DateTime)),
                    ("DisconnectType", typeof(string)),
                    ("PeerId", typeof(string)),
                    ("ScriptId", typeof(string)),
                    ("TransferSource", typeof(string))
                );

                var segments = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("StartTime", typeof(DateTime)),
                    ("EndTime", typeof(DateTime)),
                    ("Type", typeof(string)),
                    ("HowEnded", typeof(string)),
                    ("DisconnectType", typeof(string))
                );

                var disconnectReasons = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("Type", typeof(string)),
                    ("Code", typeof(int)),
                    ("Phrase", typeof(string))
                );

                var afterCallWork = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("StartTime", typeof(DateTime)),
                    ("EndTime", typeof(DateTime)),
                    ("State", typeof(string))
                );

                var wrapups = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("Code", typeof(string)),
                    ("Name", typeof(string)),
                    ("Notes", typeof(string)),
                    ("DurationSeconds", typeof(int)),
                    ("EndTime", typeof(DateTime))
                );

                var dispositions = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("Name", typeof(string)),
                    ("Analyzer", typeof(string)),
                    ("DetectedSpeechStart", typeof(DateTime))
                );

                var recentTransfers = CreateTable(
                    ("RecentTransferId", typeof(string)),
                    ("ConversationId", typeof(string)),
                    ("State", typeof(string)),
                    ("DateIssued", typeof(DateTime)),
                    ("TransferType", typeof(string)),
                    ("DestinationUserId", typeof(string)),
                    ("DestinationAddress", typeof(string)),
                    ("InitiatorUserId", typeof(string))
                );

                var selfs = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("Name", typeof(string)),
                    ("NameRaw", typeof(string)),
                    ("AddressNormalized", typeof(string)),
                    ("AddressRaw", typeof(string)),
                    ("AddressDisplayable", typeof(string))
                );

                var others = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("CallId", typeof(string)),
                    ("Name", typeof(string)),
                    ("NameRaw", typeof(string)),
                    ("AddressNormalized", typeof(string)),
                    ("AddressRaw", typeof(string)),
                    ("AddressDisplayable", typeof(string))
                );

                var attributes = CreateTable(
                    ("ConversationId", typeof(string)),
                    ("ParticipantId", typeof(string)),
                    ("dialerContactId", typeof(string)),
                    ("dialerContactListId", typeof(string)),
                    ("dialerCampaignId", typeof(string)),
                    ("dialerInteractionId", typeof(string)),
                    ("ConcernMIN", typeof(string)),
                    ("Category", typeof(string)),
                    ("Product", typeof(string)),
                    ("CSATCategory", typeof(string)),
                    ("CallerID", typeof(string)),
                    ("CallerNumber", typeof(string))
                );

                var participantWrapups = CreateTable(
                    ("ConversationId", typeof(string)),
                    ("ParticipantId", typeof(string)),
                    ("Code", typeof(string)),
                    ("durationSeconds", typeof(int)),
                    ("endTime", typeof(DateTime))
                );

                // =========================================================
                // CONVERSATION
                // =========================================================

                conversations.Rows.Add(
                    conversationId.ToString(),
                    Db(GetDate(r, "startTime")),
                    Db(GetDate(r, "endTime")),
                    Db(GetString(r, "address")),
                    Db(GetString(r, "recordingState")),
                    Db(GetString(r, "utilizationLabelId")),
                    Db(GetString(r, "selfUri")),
                    json,
                    DateTime.Now
                );

                // =========================================================
                // PARTICIPANTS
                // =========================================================

                if (r.TryGetProperty("participants", out var ps) &&
                    ps.ValueKind == JsonValueKind.Array)
                {
                    foreach (var p in ps.EnumerateArray())
                    {
                        if (!Guid.TryParse(GetString(p, "id"), out Guid participantId))
                            continue;

                        string participantIdString = participantId.ToString();
                        string conversationIdString = conversationId.ToString();

                        participants.Rows.Add(
                            participantIdString,
                            conversationIdString,
                            Db(GetDate(p, "startTime")),
                            Db(GetDate(p, "endTime")),
                            Db(GetDate(p, "connectedTime")),
                            Db(GetString(p, "name")),
                            Db(GetString(p, "userId")),
                            Db(GetString(p, "userUri")),
                            Db(GetString(p, "externalContactId")),
                            Db(GetString(p, "externalContactInitialDivisionId")),
                            Db(GetString(p, "queueId")),
                            Db(GetString(p, "queueName")),
                            Db(GetString(p, "purpose")),
                            Db(GetString(p, "participantType")),
                            Db(GetString(p, "address")),
                            Db(GetString(p, "ani")),
                            Db(GetString(p, "aniName")),
                            Db(GetString(p, "dnis")),
                            Db(GetDate(p, "startAcwTime")),
                            Db(GetDate(p, "endAcwTime"))
                        );

                        // =================================================
                        // ATTRIBUTES
                        // =================================================

                        if (p.TryGetProperty("attributes", out var a) &&
                            a.ValueKind == JsonValueKind.Object)
                        {
                            attributes.Rows.Add(
                                conversationIdString,
                                participantIdString,
                                Db(GetString(a, "dialerContactId")),
                                Db(GetString(a, "dialerContactListId")),
                                Db(GetString(a, "dialerCampaignId")),
                                Db(GetString(a, "dialerInteractionId")),
                                Db(GetString(a, "ConcernMIN")),
                                Db(GetString(a, "Category")),
                                Db(GetString(a, "Product")),
                                Db(GetString(a, "CSATCategory")),
                                Db(GetString(a, "CallerID")),
                                Db(GetString(a, "CallerNumber"))
                            );
                        }

                        // =================================================
                        // PARTICIPANT WRAPUP
                        // =================================================

                        if (p.TryGetProperty("wrapup", out var pw) &&
                            pw.ValueKind == JsonValueKind.Object)
                        {
                            participantWrapups.Rows.Add(
                                conversationIdString,
                                participantIdString,
                                Db(GetString(pw, "code")),
                                Db(GetInt(pw, "durationSeconds")),
                                Db(GetDate(pw, "endTime"))
                            );
                        }

                        // =================================================
                        // CALLS
                        // =================================================

                        if (p.TryGetProperty("calls", out var cs) &&
                            cs.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var c in cs.EnumerateArray())
                            {
                                AddCallData(
                                    calls,
                                    segments,
                                    disconnectReasons,
                                    afterCallWork,
                                    wrapups,
                                    dispositions,
                                    selfs,
                                    others,
                                    participantId,
                                    c,
                                    "Calls");
                            }
                        }

                        // =================================================
                        // CALLBACKS
                        // =================================================

                        if (p.TryGetProperty("callbacks", out var callbacks) &&
                            callbacks.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var c in callbacks.EnumerateArray())
                            {
                                AddCallData(
                                    calls,
                                    segments,
                                    disconnectReasons,
                                    afterCallWork,
                                    wrapups,
                                    dispositions,
                                    selfs,
                                    others,
                                    participantId,
                                    c,
                                    "Callbacks");
                            }
                        }
                    }
                }

                // =========================================================
                // RECENT TRANSFERS
                // =========================================================

                if (r.TryGetProperty("recentTransfers", out var transfers) &&
                    transfers.ValueKind == JsonValueKind.Array)
                {
                    foreach (var t in transfers.EnumerateArray())
                    {
                        if (!Guid.TryParse(GetString(t, "id"), out Guid transferId))
                            continue;

                        string destinationUserId = null;
                        string destinationAddress = null;
                        string initiatorUserId = null;

                        if (t.TryGetProperty("destination", out var dest) &&
                            dest.ValueKind == JsonValueKind.Object)
                        {
                            destinationUserId =
                                GetGuid(dest, "userId")?.ToString();

                            destinationAddress =
                                GetString(dest, "address");
                        }

                        if (t.TryGetProperty("initiator", out var init) &&
                            init.ValueKind == JsonValueKind.Object)
                        {
                            initiatorUserId =
                                GetGuid(init, "userId")?.ToString();
                        }

                        recentTransfers.Rows.Add(
                            transferId.ToString(),
                            conversationId.ToString(),
                            Db(GetString(t, "state")),
                            Db(GetDate(t, "dateIssued")),
                            Db(GetString(t, "transferType")),
                            Db(destinationUserId),
                            Db(destinationAddress),
                            Db(initiatorUserId)
                        );
                    }
                }

                // =========================================================
                // BULK INSERT
                // =========================================================

                await BulkInsert(connection, conversations, "Conversations");
                await BulkInsert(connection, participants, "Participants");
                await BulkInsert(connection, attributes, "Attributes");
                await BulkInsert(connection, participantWrapups, "Wrapups_Participant");
                await BulkInsert(connection, recentTransfers, "RecentTransfers");

                await BulkInsert(connection, calls, "Calls");
                await BulkInsert(connection, segments, "Segments");
                await BulkInsert(connection, disconnectReasons, "DisconnectReasons");
                await BulkInsert(connection, afterCallWork, "AfterCallWork");
                await BulkInsert(connection, wrapups, "Wrapups");
                await BulkInsert(connection, dispositions, "Disposition");
                await BulkInsert(connection, selfs, "Selfs");
                await BulkInsert(connection, others, "Others");

                await SqlQueryAsync(
                    "UPDATE APIAttempCount " +
                    "SET AttemptCount = AttemptCount + 1 " +
                    "WHERE APIName = 'SaveToDatabaseConversationv2'");
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    "SaveToDatabaseConversationv2",
                    "Error: " + ex.Message);

                throw;
            }
        }


        #endregion

        #region Group

        private async Task CallApiGroup(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            int pageSize = 100;
            int currentPage = 1;
            int totalHits = 0;

            const int maxAttempts = 10;

            do
            {
                string url =
                    $"{API_URL_GROUP}?pageSize={pageSize}&pageNumber={currentPage}";

                bool success = false;
                int attempt = 0;

                while (!success && attempt < maxAttempts)
                {
                    attempt++;

                    try
                    {
                        using var response = await client.GetAsync(url);

                        int statusCode = (int)response.StatusCode;

                        // ==========================================
                        // RETRY HTTP 400 - 500
                        // ==========================================
                        if (statusCode >= 400 && statusCode <= 500)
                        {
                            int delaySeconds = 3;

                            // Special handling for Genesys 429
                            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                            {
                                if (response.Headers.TryGetValues(
                                    "Retry-After",
                                    out var values))
                                {
                                    var retryAfter =
                                        System.Linq.Enumerable.FirstOrDefault(values);

                                    if (int.TryParse(
                                        retryAfter,
                                        out int parsedSeconds))
                                    {
                                        delaySeconds = parsedSeconds;
                                    }
                                }
                            }
                            else
                            {
                                // Exponential backoff for other 4xx/500 errors
                                delaySeconds = Math.Min(
                                    30,
                                    (int)Math.Pow(2, attempt)
                                );
                            }

                            if (attempt >= maxAttempts)
                            {
                                throw new HttpRequestException(
                                    $"HTTP {statusCode} after {maxAttempts} attempts. URL: {url}"
                                );
                            }

                            await Task.Delay(
                                TimeSpan.FromSeconds(delaySeconds)
                            );

                            continue;
                        }

                        // ==========================================
                        // SUCCESS
                        // ==========================================
                        response.EnsureSuccessStatusCode();

                        var result =
                            await response.Content.ReadAsStringAsync();

                        if (string.IsNullOrWhiteSpace(result))
                        {
                            success = true;
                            break;
                        }

                        // ==========================================
                        // SAVE DATA
                        // ==========================================
                        await SaveToDatabaseGroupv2(result, token);

                        // ==========================================
                        // GET PAGINATION
                        // ==========================================
                        using (JsonDocument doc =
                               JsonDocument.Parse(result))
                        {
                            if (doc.RootElement.TryGetProperty(
                                "totalHits",
                                out JsonElement totalHitsElement))
                            {
                                totalHits =
                                    totalHitsElement.GetInt32();
                            }
                            else if (doc.RootElement.TryGetProperty(
                                "total",
                                out JsonElement totalElement))
                            {
                                totalHits =
                                    totalElement.GetInt32();
                            }
                            else
                            {
                                totalHits = 0;
                            }
                        }

                        success = true;
                    }
                    catch (HttpRequestException)
                    {
                        // Network error / connection error
                        if (attempt >= maxAttempts)
                            throw;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, attempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (TaskCanceledException)
                    {
                        // Timeout
                        if (attempt >= maxAttempts)
                            throw;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, attempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                }

                // ==========================================
                // NEXT PAGE
                // ==========================================
                if (!success)
                    break;

                await Task.Delay(200);

                currentPage++;

            } while ((currentPage - 1) * pageSize < totalHits);
        }

        private async Task SaveToDatabaseGroupv2(string json, string token)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var groups = CreateTable(
                    ("GroupId", typeof(string)),
                    ("GroupName", typeof(string)),
                    ("Description", typeof(string)),
                    ("DateModified", typeof(DateTime)),
                    ("MemberCount", typeof(int)),
                    ("State", typeof(string)),
                    ("Type", typeof(string)),
                    ("RulesVisible", typeof(int)),
                    ("Visibility", typeof(string)),
                    ("RolesEnabled", typeof(int)),
                    ("IncludeOwners", typeof(int)),
                    ("CallsEnabled", typeof(int)),
                    ("SelfUri", typeof(string)));


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                if (!doc.RootElement.TryGetProperty("entities", out var entities) ||
                    entities.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'entities' array.");
                }


                // =========================================================
                // PARSE GROUPS
                // =========================================================

                foreach (var group in entities.EnumerateArray())
                {
                    string groupId = S(group, "id");
                    string groupName = S(group, "name");

                    groups.Rows.Add(
                        Db(groupId),
                        Db(groupName),
                        Db(S(group, "description")),
                        Db(D(group, "dateModified")),
                        Db(GetInt(group, "memberCount")),
                        Db(S(group, "state")),
                        Db(S(group, "type")),
                        Db(GetBoolAsInt(group, "rulesVisible")),
                        Db(S(group, "visibility")),
                        Db(GetBoolAsInt(group, "rolesEnabled")),
                        Db(GetBoolAsInt(group, "includeOwners")),
                        Db(GetBoolAsInt(group, "callsEnabled")),
                        Db(S(group, "selfUri"))
                    );
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                await BulkInsert(
                    conn,
                    groups,
                    "Groups");


                // =========================================================
                // GET GROUP MEMBERS
                // =========================================================

                foreach (var group in entities.EnumerateArray())
                {
                    string groupId = S(group, "id");
                    string groupName = S(group, "name");

                    if (string.IsNullOrWhiteSpace(groupId))
                        continue;

                    await CallApiMember(
                        token,
                        groupId,
                        groupName);
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseGroupv2),
                    $"SUCCESS | " +
                    $"Groups={groups.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseGroupv2),
                    $"ERROR | {ex.GetType().Name} | {ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }


        private async Task SaveToDatabaseGroupv1(string json, string token)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                if (root.TryGetProperty("entities", out var entities))
                {
                    foreach (var e in entities.EnumerateArray())
                    {
                        string id = GetString(e, "id");
                        string name = GetString(e, "name");
                        string description = GetString(e, "description");
                        DateTime? dateModified = GetDate(e, "dateModified");
                        int? memberCount = GetInt(e, "memberCount");
                        string state = GetString(e, "state");
                        string type = GetString(e, "type");
                        int? rulesVisible = GetBoolAsInt(e, "rulesVisible");
                        string visibility = GetString(e, "visibility");
                        int? rolesEnabled = GetBoolAsInt(e, "rolesEnabled");
                        int? includeOwners = GetBoolAsInt(e, "includeOwners");
                        int? callsEnabled = GetBoolAsInt(e, "callsEnabled");
                        string selfUri = GetString(e, "selfUri");

                        string groupsSQLQry = "INSERT INTO Groups " +
                        " SELECT '" + id + "', '" + name + "', '" + description + "', " + ToSqlDateTime(dateModified)
                        + ", " + ToSqlString(memberCount) + ", '" + state + "', '" + type + "', " + ToSqlString(rulesVisible)
                        + ", '" + visibility + "', " + ToSqlString(rolesEnabled) + ", " + ToSqlString(includeOwners) + ", " + ToSqlString(callsEnabled)
                        + ", '" + selfUri + "'";

                        SqlCommand groupsSQLCmd = new SqlCommand(groupsSQLQry, connection);

                        groupsSQLCmd.CommandTimeout = 300;

                        groupsSQLCmd.ExecuteNonQuery();

                        await CallApiMember(token, id, name);
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseGroup", "Error: " + ex.Message);
            }
        }

        private async Task CallApiMember(string token, string id, string name)
        {
            try
            {
                var client = _httpClientFactory.CreateClient();

                client.DefaultRequestHeaders.Authorization =
                    new AuthenticationHeaderValue("Bearer", token);

                var url = API_URL_MEMBER + $"{id}" + "/members";

                var response = await client.GetAsync(url);

                if (response.IsSuccessStatusCode)
                {
                    var json = await response.Content.ReadAsStringAsync();

                    await SaveToDatabaseMember(json, id, name);
                }

              
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("CallApiMember", "Error: " + ex.Message);
            }
        }

        private async Task SaveToDatabaseMember(string json, string groupid, string groupname)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                if (root.TryGetProperty("entities", out var entities))
                {
                    foreach (var e in entities.EnumerateArray())
                    {
                        string id = GetString(e, "id");
                        string name = GetString(e, "name");

                        string groupsSQLQry = "INSERT INTO GroupMembers " +
                        " SELECT '" + id + "', '" + name + "', '" + groupid + "', '" + groupname + "'";

                        SqlCommand groupsSQLCmd = new SqlCommand(groupsSQLQry, connection);

                        groupsSQLCmd.CommandTimeout = 300;

                        groupsSQLCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseMember", "Error: " + ex.Message);
            }
        }

        #endregion

        #region ConversationID

        private async Task CallApiOutboundConversationID(string token)
        {
            const int pageSize = 100;
            const int maxRetries = 10;

            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            foreach (var currentInterval in GetHourlyIntervals(INTERVAL))
            {
                await SqlQueryLogsAsync(
                    nameof(CallApiOutboundConversationID),
                    $"Processing Interval: {currentInterval}");

                int currentPage = 1;
                int totalHits = 0;

                do
                {
                    var body = new
                    {
                        interval = currentInterval,
                        paging = new
                        {
                            pageSize,
                            pageNumber = currentPage
                        },
                        segmentFilters = new[]
                        {
                    new
                    {
                        type = "and",
                        predicates = new[]
                        {
                            new
                            {
                                dimension = "direction",
                                value = "outbound"
                            }
                        }
                    }
                }
                    };

                    var json = JsonSerializer.Serialize(body);

                    HttpResponseMessage? response = null;

                    for (int retry = 0; retry < maxRetries; retry++)
                    {
                        response = await client.PostAsync(
                            API_URL_CONVERSIONS,
                            new StringContent(json, Encoding.UTF8, "application/json"));

                        if (response.IsSuccessStatusCode)
                            break;

                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            int waitSeconds = 3;

                            if (response.Headers.TryGetValues("Retry-After", out var values))
                            {
                                int.TryParse(values.FirstOrDefault(), out waitSeconds);
                            }

                            await SqlQueryLogsAsync(
                                nameof(CallApiOutboundConversationID),
                                $"429 Rate Limit. Interval={currentInterval}, Page={currentPage}. Waiting {waitSeconds}s.");

                            response.Dispose();

                            await Task.Delay(TimeSpan.FromSeconds(waitSeconds));
                            continue;
                        }

                        var error = await response.Content.ReadAsStringAsync();

                        await SqlQueryLogsAsync(
                            nameof(CallApiOutboundConversationID),
                            $"HTTP {(int)response.StatusCode}: {error}");

                        response.EnsureSuccessStatusCode();
                    }

                    if (response == null || !response.IsSuccessStatusCode)
                    {
                        throw new HttpRequestException(
                            $"Failed after {maxRetries} retries.");
                    }

                    var result = await response.Content.ReadAsStringAsync();

                    using var doc = JsonDocument.Parse(result);

                    if (!doc.RootElement.TryGetProperty("totalHits", out var totalHitsElement))
                        break;

                    totalHits = totalHitsElement.GetInt32();

                    if (totalHits == 0)
                        break;

                    await SaveToDatabaseConversationID(connection, doc);

                    currentPage++;

                    // Small delay between pages
                    await Task.Delay(200);

                } while ((currentPage - 1) * pageSize < totalHits);

                await SqlQueryLogsAsync(
                    nameof(CallApiOutboundConversationID),
                    $"Completed Interval: {currentInterval}");

                // Delay before starting the next hourly interval
                await Task.Delay(1000);
            }
        }

        private async Task CallApiInboundConversationID(string token)
        {
            const int pageSize = 100;
            const int maxRetries = 10;

            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            foreach (var currentInterval in GetHourlyIntervals(INTERVAL))
            {
                await SqlQueryLogsAsync(
                    nameof(CallApiOutboundConversationID),
                    $"Processing Interval: {currentInterval}");

                int currentPage = 1;
                int totalHits = 0;

                do
                {
                    var body = new
                    {
                        interval = currentInterval,
                        paging = new
                        {
                            pageSize,
                            pageNumber = currentPage
                        },
                        segmentFilters = new[]
                        {
                    new
                    {
                        type = "and",
                        predicates = new[]
                        {
                            new
                            {
                                dimension = "direction",
                                value = "inbound"
                            }
                        }
                    }
                }
                    };

                    var json = JsonSerializer.Serialize(body);

                    HttpResponseMessage? response = null;

                    for (int retry = 0; retry < maxRetries; retry++)
                    {
                        response = await client.PostAsync(
                            API_URL_CONVERSIONS,
                            new StringContent(json, Encoding.UTF8, "application/json"));

                        if (response.IsSuccessStatusCode)
                            break;

                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            int waitSeconds = 3;

                            if (response.Headers.TryGetValues("Retry-After", out var values))
                            {
                                int.TryParse(values.FirstOrDefault(), out waitSeconds);
                            }

                            await SqlQueryLogsAsync(
                                nameof(CallApiOutboundConversationID),
                                $"429 Rate Limit. Interval={currentInterval}, Page={currentPage}. Waiting {waitSeconds}s.");

                            response.Dispose();

                            await Task.Delay(TimeSpan.FromSeconds(waitSeconds));
                            continue;
                        }

                        var error = await response.Content.ReadAsStringAsync();

                        await SqlQueryLogsAsync(
                            nameof(CallApiOutboundConversationID),
                            $"HTTP {(int)response.StatusCode}: {error}");

                        response.EnsureSuccessStatusCode();
                    }

                    if (response == null || !response.IsSuccessStatusCode)
                    {
                        throw new HttpRequestException(
                            $"Failed after {maxRetries} retries.");
                    }

                    var result = await response.Content.ReadAsStringAsync();

                    using var doc = JsonDocument.Parse(result);

                    if (!doc.RootElement.TryGetProperty("totalHits", out var totalHitsElement))
                        break;

                    totalHits = totalHitsElement.GetInt32();

                    if (totalHits == 0)
                        break;

                    await SaveToDatabaseConversationID(connection, doc);

                    currentPage++;

                    // Small delay between pages
                    await Task.Delay(200);

                } while ((currentPage - 1) * pageSize < totalHits);

                await SqlQueryLogsAsync(
                    nameof(CallApiOutboundConversationID),
                    $"Completed Interval: {currentInterval}");

                // Delay before starting the next hourly interval
                await Task.Delay(1000);
            }
        }

        private async Task SaveToDatabaseConversationID(SqlConnection connection, JsonDocument doc)
        {
            try
            {
                var values = new StringBuilder();

                foreach (var conv in doc.RootElement.GetProperty("conversations").EnumerateArray())
                {
                    if (!conv.TryGetProperty("conversationId", out var ci))
                        continue;

                    values.Append("('");
                    values.Append(ci.GetString().Replace("'", "''"));
                    values.Append("'),");
                }

                if (values.Length == 0)
                    return;

                values.Length--;

                string sql =
                    $"INSERT INTO ConversationsID (ConversationId) VALUES {values}";

                using var cmd = new SqlCommand(sql, connection);

                cmd.CommandTimeout = 300;

                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    "SaveToDatabaseConversationID",
                    ex.ToString());
            }
        }

        #endregion

        #region User

        private async Task CallApiUsers(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int pageSize = 100;
            const int maxRetries = 10;

            int currentPage = 1;
            int totalHits = 0;

            do
            {
                string url =
                    $"{API_URL_USERS}?pageSize={pageSize}&pageNumber={currentPage}";

                HttpResponseMessage response = null;
                string result = null;

                for (int retry = 1; retry <= maxRetries; retry++)
                {
                    try
                    {
                        Console.WriteLine(
                            $"Users API Page {currentPage} - Attempt {retry}/{maxRetries}");

                        response = await client.GetAsync(url);

                        int statusCode = (int)response.StatusCode;

                        // =========================================
                        // RETRY 429 / 408 / 5xx
                        // =========================================
                        if (statusCode == 429 ||
                            statusCode == 408 ||
                            statusCode >= 500)
                        {
                            int retrySeconds = 3;

                            // Genesys Retry-After
                            if (statusCode == 429 &&
                                response.Headers.TryGetValues(
                                    "Retry-After",
                                    out IEnumerable<string> values))
                            {
                                string retryAfter =
                                    values.FirstOrDefault();

                                if (int.TryParse(
                                    retryAfter,
                                    out int seconds))
                                {
                                    retrySeconds = seconds;
                                }
                            }
                            else
                            {
                                // Exponential backoff
                                retrySeconds = Math.Min(
                                    60,
                                    3 * (int)Math.Pow(2, retry - 1));
                            }

                            Console.WriteLine(
                                $"API returned {statusCode}. " +
                                $"Retrying in {retrySeconds} seconds...");

                            response.Dispose();

                            await Task.Delay(
                                TimeSpan.FromSeconds(retrySeconds));

                            continue;
                        }

                        // =========================================
                        // RETRY OTHER 4xx
                        // =========================================
                        if (statusCode >= 400 &&
                            statusCode < 500)
                        {
                            retry++;

                            if (retry > maxRetries)
                            {
                                string error =
                                    await response.Content.ReadAsStringAsync();

                                throw new Exception(
                                    $"Users API failed after {maxRetries} retries. " +
                                    $"Status: {statusCode}. " +
                                    $"Response: {error}");
                            }

                            int retrySeconds = Math.Min(
                                60,
                                retry * 3);

                            Console.WriteLine(
                                $"API returned {statusCode}. " +
                                $"Retrying in {retrySeconds} seconds...");

                            response.Dispose();

                            await Task.Delay(
                                TimeSpan.FromSeconds(retrySeconds));

                            continue;
                        }

                        // =========================================
                        // SUCCESS
                        // =========================================
                        response.EnsureSuccessStatusCode();

                        result =
                            await response.Content.ReadAsStringAsync();

                        response.Dispose();

                        break;
                    }
                    catch (HttpRequestException ex)
                    {
                        Console.WriteLine(
                            $"Request error on page {currentPage}. " +
                            $"Attempt {retry}/{maxRetries}:");

                        Console.WriteLine(ex.ToString());

                        if (retry == maxRetries)
                            throw;

                        int retrySeconds = Math.Min(
                            60,
                            3 * (int)Math.Pow(2, retry - 1));

                        Console.WriteLine(
                            $"Retrying in {retrySeconds} seconds...");

                        await Task.Delay(
                            TimeSpan.FromSeconds(retrySeconds));
                    }
                    catch (TaskCanceledException ex)
                    {
                        Console.WriteLine(
                            $"Request timeout on page {currentPage}. " +
                            $"Attempt {retry}/{maxRetries}: " +
                            $"{ex.Message}");

                        if (retry == maxRetries)
                            throw;

                        int retrySeconds = Math.Min(
                            60,
                            3 * (int)Math.Pow(2, retry - 1));

                        await Task.Delay(
                            TimeSpan.FromSeconds(retrySeconds));
                    }
                }

                // =========================================
                // NO RESPONSE
                // =========================================
                if (string.IsNullOrWhiteSpace(result))
                {
                    Console.WriteLine(
                        $"Empty response for page {currentPage}.");

                    break;
                }

                // =========================================
                // SAVE DATA
                // =========================================
                await SaveToDatabaseUsersv2(result);

                // =========================================
                // GET TOTAL
                // =========================================
                using (JsonDocument doc =
                       JsonDocument.Parse(result))
                {
                    if (doc.RootElement.TryGetProperty(
                            "totalHits",
                            out JsonElement totalHitsElement))
                    {
                        totalHits =
                            totalHitsElement.GetInt32();
                    }
                    else if (doc.RootElement.TryGetProperty(
                            "total",
                            out JsonElement totalElement))
                    {
                        totalHits =
                            totalElement.GetInt32();
                    }
                    else
                    {
                        break;
                    }
                }

                Console.WriteLine(
                    $"Page {currentPage} completed. " +
                    $"Total users: {totalHits}");

                currentPage++;

                // Normal pacing
                await Task.Delay(200);

            }
            while ((currentPage - 1) * pageSize < totalHits);
        }






        private async Task SaveToDatabaseUsersv1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                foreach (var ent in doc.RootElement.GetProperty("entities").EnumerateArray())
                {
                    string userId = ent.TryGetProperty("id", out var ui) ? ui.GetString() : null;
                    string userFullName = ent.TryGetProperty("name", out var ufn) ? ufn.GetString() : null;
                    string userName = ent.TryGetProperty("username", out var un) ? un.GetString() : null;
                    string userState = ent.TryGetProperty("state", out var us) ? us.GetString() : null;
                    int groupRow = 0;

                    if (ent.TryGetProperty("groups", out var groups))
                    {

                        if (groups.ValueKind == JsonValueKind.Array)
                        {
                            if (groups.GetArrayLength() > 0)
                            {
                                foreach (var g in groups.EnumerateArray())
                                {
                                    groupRow = groupRow + 1;
                                    string groupId = g.TryGetProperty("id", out var gid) ? gid.GetString() : null;

                                    string entitiesSQLQry = "INSERT INTO Users_All " +
                                    " SELECT '" + userId + "', '" + userFullName + "', '" + userName + "', '" + userState + "', '" + groupId + "', " + groupRow;

                                    SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                                    entitiesSQLCmd.CommandTimeout = 300;

                                    entitiesSQLCmd.ExecuteNonQuery();
                                }
                            }
                            else
                            {
                                groupRow = groupRow + 1;
                                string groupId = "";

                                string entitiesSQLQry = "INSERT INTO Users_All " +
                                " SELECT '" + userId + "', '" + userFullName + "', '" + userName + "', '" + userState + "', '" + groupId + "', " + groupRow;

                                SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                                entitiesSQLCmd.CommandTimeout = 300;

                                entitiesSQLCmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            string entitiesSQLQry = "INSERT INTO Users_All " +
                            " SELECT '" + userId + "', '" + userFullName + "', '" + userName + "', '" + userState + "', '', " + groupRow;

                            SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                            entitiesSQLCmd.CommandTimeout = 300;

                            entitiesSQLCmd.ExecuteNonQuery();
                        }
                    }
                    else
                    {
                        string entitiesSQLQry = "INSERT INTO Users_All " +
                        " SELECT '" + userId + "', '" + userFullName + "', '" + userName + "', '" + userState + "', '', " + groupRow;

                        SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                        entitiesSQLCmd.CommandTimeout = 300;

                        entitiesSQLCmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseUsers", "Error: " + ex.Message);
            }

        }

        private async Task SaveToDatabaseUsersv2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var users = CreateTable(
                    ("UserId", typeof(string)),
                    ("UserFullName", typeof(string)),
                    ("Username", typeof(string)),
                    ("UserState", typeof(string)),
                    ("GroupId", typeof(string)),
                    ("GroupRow", typeof(int))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                if (!doc.RootElement.TryGetProperty("entities", out var entities) ||
                    entities.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'entities' array.");
                }


                // =========================================================
                // PARSE USERS
                // =========================================================

                foreach (var user in entities.EnumerateArray())
                {
                    string userId = S(user, "id");
                    string userFullName = S(user, "name");
                    string userName = S(user, "username");
                    string userState = S(user, "state");

                    int groupRow = 0;

                    // =====================================================
                    // GROUPS
                    // =====================================================

                    if (user.TryGetProperty("groups", out var groups) &&
                        groups.ValueKind == JsonValueKind.Array)
                    {
                        // -------------------------------------------------
                        // USER HAS GROUPS
                        // -------------------------------------------------

                        foreach (var group in groups.EnumerateArray())
                        {
                            groupRow++;

                            string groupId = S(group, "id");

                            users.Rows.Add(
                                Db(userId),
                                Db(userFullName),
                                Db(userName),
                                Db(userState),
                                Db(groupId),
                                groupRow
                            );
                        }

                        // -------------------------------------------------
                        // USER HAS EMPTY GROUP ARRAY
                        // -------------------------------------------------

                        if (groupRow == 0)
                        {
                            groupRow = 1;

                            users.Rows.Add(
                                Db(userId),
                                Db(userFullName),
                                Db(userName),
                                Db(userState),
                                Db(null),
                                groupRow
                            );
                        }
                    }
                    else
                    {
                        // =================================================
                        // USER HAS NO GROUP PROPERTY
                        // =================================================

                        groupRow = 1;

                        users.Rows.Add(
                            Db(userId),
                            Db(userFullName),
                            Db(userName),
                            Db(userState),
                            Db(null),
                            groupRow
                        );
                    }
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (users.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        users,
                        "Users_All");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUsersv2),
                    $"SUCCESS | " +
                    $"UsersRows={users.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUsersv2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }

        private async Task CallApiUsersByUserIdv2(string token)
        {
            var agentIds = new List<string>();

            // =========================================================
            // GET USER IDS
            // =========================================================
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (var cmd = new SqlCommand(@"
                    SELECT DISTINCT UserId
                    FROM Participants
                    WHERE UserId IS NOT NULL
                      AND UserId <> ''
                ", conn))
                {
                    cmd.CommandTimeout = 300;

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            agentIds.Add(reader.GetString(0));
                        }
                    }
                }
            }

            // =========================================================
            // CREATE HTTP CLIENT ONCE
            // =========================================================
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            // =========================================================
            // PROCESS USERS
            // =========================================================
            foreach (var userId in agentIds)
            {
                bool success = false;
                int attempt = 0;

                // Small pacing delay
                await Task.Delay(100);

                while (!success && attempt < maxAttempts)
                {
                    attempt++;

                    try
                    {
                        string url =
                            $"{API_URL_USERS_GET}{userId}?expand=groups";

                        using (var response = await client.GetAsync(url))
                        {
                            int statusCode = (int)response.StatusCode;

                            // =================================================
                            // 404 - USER DOES NOT EXIST
                            // Don't retry
                            // =================================================
                            if (response.StatusCode == HttpStatusCode.NotFound)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId2",
                                    $"404 - UserId={userId} not found. Skipping."
                                );

                                break;
                            }

                            // =================================================
                            // HTTP 400 - 500
                            // =================================================
                            if (statusCode >= 400 && statusCode <= 500)
                            {
                                int delaySeconds = 3;

                                // ---------------------------------------------
                                // 429 - Genesys Rate Limit
                                // ---------------------------------------------
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    if (response.Headers.RetryAfter?.Delta != null)
                                    {
                                        delaySeconds = Math.Max(
                                            1,
                                            (int)Math.Ceiling(
                                                response.Headers
                                                    .RetryAfter
                                                    .Delta.Value
                                                    .TotalSeconds
                                            )
                                        );
                                    }
                                    else if (
                                        response.Headers.TryGetValues(
                                            "Retry-After",
                                            out var values))
                                    {
                                        var retryAfter =
                                            System.Linq.Enumerable
                                                .FirstOrDefault(values);

                                        if (int.TryParse(
                                            retryAfter,
                                            out int parsedSeconds))
                                        {
                                            delaySeconds = parsedSeconds;
                                        }
                                    }
                                }
                                else
                                {
                                    // -----------------------------------------
                                    // Other 400-500 errors
                                    // Exponential backoff
                                    // -----------------------------------------
                                    delaySeconds = Math.Min(
                                        30,
                                        (int)Math.Pow(2, attempt)
                                    );
                                }

                                // ---------------------------------------------
                                // Last attempt
                                // ---------------------------------------------
                                if (attempt >= maxAttempts)
                                {
                                    var errorBody =
                                        await response.Content
                                            .ReadAsStringAsync();

                                    await SqlQueryLogsAsync(
                                        "CallApiUsersByUserId2",
                                        $"HTTP {statusCode} after " +
                                        $"{maxAttempts} attempts. " +
                                        $"UserId={userId}. " +
                                        $"Body: {errorBody}"
                                    );

                                    break;
                                }

                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId2",
                                    $"HTTP {statusCode} for UserId={userId}. " +
                                    $"Retrying in {delaySeconds}s. " +
                                    $"Attempt {attempt}/{maxAttempts}"
                                );

                                await Task.Delay(
                                    TimeSpan.FromSeconds(delaySeconds)
                                );

                                continue;
                            }

                            // =================================================
                            // SUCCESS
                            // =================================================
                            response.EnsureSuccessStatusCode();

                            var json =
                                await response.Content.ReadAsStringAsync();

                            if (string.IsNullOrWhiteSpace(json))
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId2",
                                    $"Empty response for UserId={userId}."
                                );

                                break;
                            }

                            // =================================================
                            // SAVE
                            // =================================================
                            await SaveToDatabaseUsersByUserIdv2(json);

                            success = true;
                        }
                    }
                    catch (HttpRequestException ex)
                    {
                        // =====================================================
                        // NETWORK ERROR
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId2",
                            $"Network error for UserId={userId}. " +
                            $"Attempt {attempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (attempt >= maxAttempts)
                            break;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, attempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (TaskCanceledException ex)
                    {
                        // =====================================================
                        // TIMEOUT
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId2",
                            $"Timeout for UserId={userId}. " +
                            $"Attempt {attempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (attempt >= maxAttempts)
                            break;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, attempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (Exception ex)
                    {
                        // =====================================================
                        // OTHER ERROR
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId2",
                            $"Error for UserId={userId}. " +
                            $"Attempt {attempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (attempt >= maxAttempts)
                            break;

                        await Task.Delay(2000);
                    }
                }
            }
        }



        private async Task CallApiUsersByUserId(string token)
        {
            var agentIds = new List<string>();

            // =========================================================
            // GET USER IDS FROM DATABASE
            // =========================================================
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (var cmd = new SqlCommand(@"
                    SELECT DISTINCT UserId
                    FROM Users_All
                    WHERE UserId IS NOT NULL
                      AND UserId <> ''
                ", conn))
                {
                    cmd.CommandTimeout = 300;

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            agentIds.Add(reader.GetString(0));
                        }
                    }
                }
            }

            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            // =========================================================
            // PROCESS EACH USER
            // =========================================================
            foreach (var userId in agentIds)
            {
                bool downloadSuccess = false;
                int currentAttempt = 0;

                // Small pacing delay between users
                await Task.Delay(100);

                while (!downloadSuccess && currentAttempt < maxAttempts)
                {
                    currentAttempt++;

                    try
                    {
                        string url =
                            $"{API_URL_USERS_GET}{userId}?expand=groups";

                        using (var response = await client.GetAsync(url))
                        {
                            int statusCode = (int)response.StatusCode;

                            // =================================================
                            // 404 - USER DOES NOT EXIST
                            // Do NOT retry
                            // =================================================
                            if (response.StatusCode == HttpStatusCode.NotFound)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId",
                                    $"404 NotFound. UserId={userId} does not exist. Skipping."
                                );

                                break;
                            }

                            // =================================================
                            // 400 - 500 RETRY
                            // Includes 429
                            // =================================================
                            if (statusCode >= 400 && statusCode <= 500)
                            {
                                int delaySeconds = 3;

                                // ---------------------------------------------
                                // 429 - Use Genesys Retry-After
                                // ---------------------------------------------
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    if (response.Headers.TryGetValues(
                                        "Retry-After",
                                        out var values))
                                    {
                                        var retryAfter =
                                            System.Linq.Enumerable
                                                .FirstOrDefault(values);

                                        if (int.TryParse(
                                            retryAfter,
                                            out int parsedSeconds))
                                        {
                                            delaySeconds = parsedSeconds;
                                        }
                                    }
                                }
                                else
                                {
                                    // -----------------------------------------
                                    // Other 400-500 errors
                                    // Exponential backoff
                                    // -----------------------------------------
                                    delaySeconds = Math.Min(
                                        30,
                                        (int)Math.Pow(2, currentAttempt)
                                    );
                                }

                                // ---------------------------------------------
                                // Max attempts reached
                                // ---------------------------------------------
                                if (currentAttempt >= maxAttempts)
                                {
                                    var errorBody =
                                        await response.Content.ReadAsStringAsync();

                                    await SqlQueryLogsAsync(
                                        "CallApiUsersByUserId",
                                        $"HTTP {statusCode} after {maxAttempts} attempts. " +
                                        $"UserId={userId}. Body: {errorBody}"
                                    );

                                    break;
                                }

                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId",
                                    $"HTTP {statusCode} for UserId={userId}. " +
                                    $"Retrying in {delaySeconds}s. " +
                                    $"Attempt {currentAttempt}/{maxAttempts}"
                                );

                                await Task.Delay(
                                    TimeSpan.FromSeconds(delaySeconds)
                                );

                                continue;
                            }

                            // =================================================
                            // SUCCESS
                            // =================================================
                            response.EnsureSuccessStatusCode();

                            var json =
                                await response.Content.ReadAsStringAsync();

                            if (string.IsNullOrWhiteSpace(json))
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiUsersByUserId",
                                    $"Empty response for UserId={userId}."
                                );

                                break;
                            }

                            // =================================================
                            // SAVE DATA
                            // =================================================
                            await SaveToDatabaseUsersByUserIdv2(json);

                            downloadSuccess = true;
                        }
                    }
                    catch (HttpRequestException ex)
                    {
                        // =====================================================
                        // NETWORK / CONNECTION ERROR
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId",
                            $"HttpRequestException for UserId={userId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: {ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                        {
                            await SqlQueryLogsAsync(
                                "CallApiUsersByUserId",
                                $"Failed permanently for UserId={userId} " +
                                $"after {maxAttempts} attempts."
                            );

                            break;
                        }

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, currentAttempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (TaskCanceledException ex)
                    {
                        // =====================================================
                        // TIMEOUT
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId",
                            $"Timeout for UserId={userId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: {ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                            break;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, currentAttempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (Exception ex)
                    {
                        // =====================================================
                        // OTHER ERRORS
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiUsersByUserId",
                            $"Exception for UserId={userId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: {ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                            break;

                        await Task.Delay(2000);
                    }
                }
            }
        }


        private async Task SaveToDatabaseUsersByUserIdv1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                var id = root.GetProperty("id").GetString();
                var name = root.GetProperty("name").GetString();
                var email = root.GetProperty("email").GetString();
                var username = root.GetProperty("username").GetString();
                var state = root.GetProperty("state").GetString();

                string entitiesSQLQry = "INSERT INTO Users " +
                    " SELECT '" + id + "', '" + name + "', '" + username + "', '" + state + "'";

                SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                entitiesSQLCmd.CommandTimeout = 300;

                entitiesSQLCmd.ExecuteNonQuery();

                int GroupRow = 0;

                if (root.TryGetProperty("groups", out var usergroups))
                {
                    foreach (var usergroup in usergroups.EnumerateArray())
                    {
                        GroupRow = GroupRow + 1;
                        await SaveUserGroup(id, name, usergroup, GroupRow);
                    }
                }

            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseUsersByUserId", "Error: " + ex.Message);
            }
        }

        private async Task SaveToDatabaseUsersByUserIdv2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLES
                // =========================================================

                var users = CreateTable(
                    ("UserId", typeof(string)),
                    ("UserFullName", typeof(string)),
                    ("Username", typeof(string)),
                    ("UserState", typeof(string))
                );

                var userGroups = CreateTable(
                    ("UserId", typeof(string)),
                    ("UserFullName", typeof(string)),
                    ("GroupId", typeof(string)),
                    ("GroupRow", typeof(int))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                var root = doc.RootElement;

                if (root.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid user object.");
                }


                // =========================================================
                // PARSE USER
                // =========================================================

                string userId = S(root, "id");

                if (string.IsNullOrWhiteSpace(userId))
                {
                    throw new InvalidOperationException(
                        "User JSON does not contain a valid 'id'.");
                }

                string userFullName = S(root, "name");
                string username = S(root, "username");
                string userState = S(root, "state");


                // =========================================================
                // USERS
                // =========================================================

                users.Rows.Add(
                    Db(userId),
                    Db(userFullName),
                    Db(username),
                    Db(userState)
                );


                // =========================================================
                // GROUPS
                // =========================================================

                int groupRow = 0;

                if (root.TryGetProperty("groups", out var groups) &&
                    groups.ValueKind == JsonValueKind.Array)
                {
                    foreach (var group in groups.EnumerateArray())
                    {
                        groupRow++;

                        string groupId = S(group, "id");

                        userGroups.Rows.Add(
                            Db(userId),
                            Db(userFullName),
                            Db(groupId),
                            groupRow
                        );
                    }
                }


                // =========================================================
                // BULK INSERT USERS
                // =========================================================

                if (users.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        users,
                        "Users");
                }


                // =========================================================
                // BULK INSERT GroupMembers_All
                // =========================================================

                if (userGroups.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        userGroups,
                        "GroupMembers_All");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUsersByUserIdv2),
                    $"SUCCESS | " +
                    $"Users={users.Rows.Count}, " +
                    $"Groups={userGroups.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUsersByUserIdv2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }

        private async Task SaveUserGroup(string userId, string username, JsonElement u, int grouprow)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                Guid groupId = Guid.Parse(u.GetProperty("id").GetString());

                string usergroupSQLQry = "INSERT INTO GroupMembers_All " +
                " SELECT '" + userId + "', '" + username + "', '" + groupId + "'," + grouprow;

                SqlCommand usergroupSQLCmd = new SqlCommand(usergroupSQLQry, connection);

                usergroupSQLCmd.CommandTimeout = 300;

                usergroupSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveUserGroup", "Error: " + ex.Message);
            }

        }

        #endregion

        #region Queue

        private async Task CallApiQueue(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            bool downloadSuccess = false;
            int currentAttempt = 0;

            while (!downloadSuccess && currentAttempt < maxAttempts)
            {
                currentAttempt++;

                try
                {
                    using var response = await client.GetAsync(API_URL_QUEUE);

                    int statusCode = (int)response.StatusCode;

                    // ==========================================
                    // RETRY HTTP 400 - 500
                    // ==========================================
                    if (statusCode >= 400 && statusCode <= 500)
                    {
                        int delaySeconds = 3;

                        // Genesys 429 - use Retry-After
                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            if (response.Headers.TryGetValues(
                                "Retry-After",
                                out var values))
                            {
                                var firstValue =
                                    System.Linq.Enumerable.FirstOrDefault(values);

                                if (int.TryParse(
                                    firstValue,
                                    out int parsedSeconds))
                                {
                                    delaySeconds = parsedSeconds;
                                }
                            }
                        }
                        else
                        {
                            // Exponential backoff
                            delaySeconds = Math.Min(
                                30,
                                (int)Math.Pow(2, currentAttempt)
                            );
                        }

                        // Last attempt - don't retry
                        if (currentAttempt >= maxAttempts)
                        {
                            throw new HttpRequestException(
                                $"HTTP {statusCode} after {maxAttempts} attempts. " +
                                $"URL: {API_URL_QUEUE}"
                            );
                        }

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );

                        continue;
                    }

                    // ==========================================
                    // SUCCESS
                    // ==========================================
                    response.EnsureSuccessStatusCode();

                    var result =
                        await response.Content.ReadAsStringAsync();

                    if (string.IsNullOrWhiteSpace(result))
                    {
                        downloadSuccess = true;
                        break;
                    }

                    // ==========================================
                    // SAVE TO DATABASE
                    // ==========================================
                    await SaveToDatabaseQueuev2(result);

                    downloadSuccess = true;
                }
                catch (HttpRequestException)
                {
                    // Network / connection error
                    if (currentAttempt >= maxAttempts)
                    {
                        throw;
                    }

                    int delaySeconds = Math.Min(
                        30,
                        (int)Math.Pow(2, currentAttempt)
                    );

                    await Task.Delay(
                        TimeSpan.FromSeconds(delaySeconds)
                    );
                }
                catch (TaskCanceledException)
                {
                    // HTTP timeout
                    if (currentAttempt >= maxAttempts)
                    {
                        throw;
                    }

                    int delaySeconds = Math.Min(
                        30,
                        (int)Math.Pow(2, currentAttempt)
                    );

                    await Task.Delay(
                        TimeSpan.FromSeconds(delaySeconds)
                    );
                }
            }
        }


        private async Task SaveToDatabaseQueuev1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                foreach (var ent in doc.RootElement.GetProperty("entities").EnumerateArray())
                {
                    string queueId = ent.TryGetProperty("id", out var ui) ? ui.GetString() : null;
                    string queueName = ent.TryGetProperty("name", out var ufn) ? ufn.GetString() : null;

                    string entitiesSQLQry = "INSERT INTO Queues_All " +
                        " SELECT '" + queueId + "', '" + queueName + "'";

                    SqlCommand entitiesSQLCmd = new SqlCommand(entitiesSQLQry, connection);

                    entitiesSQLCmd.CommandTimeout = 300;

                    entitiesSQLCmd.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseQueue", "Error: " + ex.Message);
            }

        }

        private async Task SaveToDatabaseQueuev2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var queues = CreateTable(
                    ("QueueId", typeof(string)),
                    ("QueueName", typeof(string))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                if (!doc.RootElement.TryGetProperty("entities", out var entities) ||
                    entities.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'entities' array.");
                }


                // =========================================================
                // PARSE QUEUES
                // =========================================================

                foreach (var queue in entities.EnumerateArray())
                {
                    string queueId = S(queue, "id");

                    if (string.IsNullOrWhiteSpace(queueId))
                        continue;

                    queues.Rows.Add(
                        Db(queueId),
                        Db(S(queue, "name"))
                    );
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (queues.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        queues,
                        "Queues_All");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseQueuev2),
                    $"SUCCESS | " +
                    $"Queues={queues.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseQueuev2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }

        private async Task CallApiQueuesByQueueId(string token)
        {
            var queueIds = new List<string>();

            // =========================================================
            // GET QUEUE IDS FROM DATABASE
            // =========================================================
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                using (var cmd = new SqlCommand(@"
                    SELECT DISTINCT QueueId
                    FROM Queues_All
                    WHERE QueueId IS NOT NULL
                      AND QueueId <> ''
                ", conn))
                {
                    cmd.CommandTimeout = 300;

                    using (var reader = await cmd.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            queueIds.Add(reader.GetString(0));
                        }
                    }
                }
            }

            // =========================================================
            // CREATE HTTP CLIENT ONCE
            // =========================================================
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            // =========================================================
            // PROCESS EACH QUEUE
            // =========================================================
            foreach (var queueId in queueIds)
            {
                bool downloadSuccess = false;
                int currentAttempt = 0;

                // Small pacing delay between requests
                await Task.Delay(100);

                while (!downloadSuccess && currentAttempt < maxAttempts)
                {
                    currentAttempt++;

                    try
                    {
                        string url = $"{API_URL_QUEUE_GET}{queueId}";

                        using (var response = await client.GetAsync(url))
                        {
                            int statusCode = (int)response.StatusCode;

                            // =================================================
                            // 404 - QUEUE NO LONGER EXISTS
                            // Don't retry
                            // =================================================
                            if (response.StatusCode == HttpStatusCode.NotFound)
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiQueuesByQueueId",
                                    $"404 NotFound. QueueId={queueId} " +
                                    $"no longer exists in Genesys. Skipping."
                                );

                                break;
                            }

                            // =================================================
                            // HTTP 400 - 500
                            // =================================================
                            if (statusCode >= 400 && statusCode <= 500)
                            {
                                int delaySeconds = 3;

                                // ---------------------------------------------
                                // 429 - Genesys Rate Limit
                                // ---------------------------------------------
                                if (response.StatusCode ==
                                    HttpStatusCode.TooManyRequests)
                                {
                                    if (response.Headers.RetryAfter?.Delta != null)
                                    {
                                        delaySeconds = Math.Max(
                                            1,
                                            (int)Math.Ceiling(
                                                response.Headers
                                                    .RetryAfter
                                                    .Delta.Value
                                                    .TotalSeconds
                                            )
                                        );
                                    }
                                    else if (
                                        response.Headers.TryGetValues(
                                            "Retry-After",
                                            out var values))
                                    {
                                        var retryAfter =
                                            System.Linq.Enumerable
                                                .FirstOrDefault(values);

                                        if (int.TryParse(
                                            retryAfter,
                                            out int parsedSeconds))
                                        {
                                            delaySeconds = parsedSeconds;
                                        }
                                    }
                                }
                                else
                                {
                                    // -----------------------------------------
                                    // Other 400-500 errors
                                    // Exponential backoff
                                    // -----------------------------------------
                                    delaySeconds = Math.Min(
                                        30,
                                        (int)Math.Pow(2, currentAttempt)
                                    );
                                }

                                // ---------------------------------------------
                                // Maximum attempts reached
                                // ---------------------------------------------
                                if (currentAttempt >= maxAttempts)
                                {
                                    var errorBody =
                                        await response.Content
                                            .ReadAsStringAsync();

                                    await SqlQueryLogsAsync(
                                        "CallApiQueuesByQueueId",
                                        $"HTTP {statusCode} after " +
                                        $"{maxAttempts} attempts. " +
                                        $"QueueId={queueId}. " +
                                        $"Body: {errorBody}"
                                    );

                                    break;
                                }

                                await SqlQueryLogsAsync(
                                    "CallApiQueuesByQueueId",
                                    $"HTTP {statusCode} for QueueId={queueId}. " +
                                    $"Retrying in {delaySeconds}s. " +
                                    $"Attempt {currentAttempt}/{maxAttempts}"
                                );

                                await Task.Delay(
                                    TimeSpan.FromSeconds(delaySeconds)
                                );

                                continue;
                            }

                            // =================================================
                            // SUCCESS
                            // =================================================
                            response.EnsureSuccessStatusCode();

                            var json =
                                await response.Content.ReadAsStringAsync();

                            if (string.IsNullOrWhiteSpace(json))
                            {
                                await SqlQueryLogsAsync(
                                    "CallApiQueuesByQueueId",
                                    $"Empty response for QueueId={queueId}."
                                );

                                break;
                            }

                            // =================================================
                            // SAVE DATA
                            // =================================================
                            await SaveToDatabaseQueuesByQueueIdv2(json);

                            downloadSuccess = true;
                        }
                    }
                    catch (HttpRequestException ex)
                    {
                        // =====================================================
                        // NETWORK / CONNECTION ERROR
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiQueuesByQueueId",
                            $"Network error for QueueId={queueId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                            break;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, currentAttempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (TaskCanceledException ex)
                    {
                        // =====================================================
                        // TIMEOUT
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiQueuesByQueueId",
                            $"Timeout for QueueId={queueId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                            break;

                        int delaySeconds = Math.Min(
                            30,
                            (int)Math.Pow(2, currentAttempt)
                        );

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                    catch (Exception ex)
                    {
                        // =====================================================
                        // OTHER ERROR
                        // =====================================================
                        await SqlQueryLogsAsync(
                            "CallApiQueuesByQueueId",
                            $"Exception for QueueId={queueId}. " +
                            $"Attempt {currentAttempt}/{maxAttempts}: " +
                            $"{ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                            break;

                        await Task.Delay(2000);
                    }
                }
            }
        }


        private async Task SaveToDatabaseQueuesByQueueIdv2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var queues = CreateTable(
                    ("QueueId", typeof(string)),
                    ("QueueName", typeof(string))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                var root = doc.RootElement;

                if (root.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid queue object.");
                }


                // =========================================================
                // PARSE QUEUE
                // =========================================================

                string queueId = S(root, "id");

                if (string.IsNullOrWhiteSpace(queueId))
                {
                    throw new InvalidOperationException(
                        "Queue JSON does not contain a valid 'id'.");
                }

                string queueName = S(root, "name");


                // =========================================================
                // QUEUE
                // =========================================================

                queues.Rows.Add(
                    Db(queueId),
                    Db(queueName)
                );


                // =========================================================
                // MEDIA SETTINGS
                // =========================================================

                var mediaSettings = CreateTable(
                    ("QueueId", typeof(string)),
                    ("Type", typeof(string)),
                    ("AlertingTimeoutSeconds", typeof(int)),
                    ("Percentage", typeof(decimal)),
                    ("DurationMs", typeof(int))
                );

                if (root.TryGetProperty("mediaSettings", out var settings) &&
                    settings.ValueKind == JsonValueKind.Object)
                {
                    var mediaTypes = new[]
                    {
                        ("call", "Call"),
                        ("callback", "Callback"),
                        ("chat", "Chat"),
                        ("email", "Email"),
                        ("message", "Message")
                    };

                    foreach (var (jsonName, mediaType) in mediaTypes)
                    {
                        if (!settings.TryGetProperty(jsonName, out var media) ||
                            media.ValueKind != JsonValueKind.Object ||
                            !media.EnumerateObject().Any())
                        {
                            continue;
                        }

                        int alertingTimeoutSeconds =
                            GetInt(media, "alertingTimeoutSeconds") ?? 0;

                        decimal percentage = 0;
                        int durationMs = 0;

                        if (media.TryGetProperty(
                                "serviceLevel",
                                out var serviceLevel) &&
                            serviceLevel.ValueKind == JsonValueKind.Object)
                        {
                            percentage =
                                GetDecimal(serviceLevel, "percentage") ?? 0;

                            durationMs =
                                GetInt(serviceLevel, "durationMs") ?? 0;
                        }

                        mediaSettings.Rows.Add(
                            Db(queueId),
                            Db(mediaType),
                            alertingTimeoutSeconds,
                            percentage,
                            durationMs
                        );
                    }
                }


                // =========================================================
                // BULK INSERT QUEUE
                // =========================================================

                if (queues.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        queues,
                        "Queues");
                }


                // =========================================================
                // BULK INSERT MEDIA SETTINGS
                // =========================================================

                if (mediaSettings.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        mediaSettings,
                        "MediaSettings");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseQueuesByQueueIdv2),
                    $"SUCCESS | " +
                    $"Queues={queues.Rows.Count}, " +
                    $"MediaSettings={mediaSettings.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseQueuesByQueueIdv2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }

        private async Task SaveToDatabaseQueuesByQueueIdv1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                var id = root.GetProperty("id").GetString();
                var name = root.GetProperty("name").GetString();

                string queuesSQLQry = "INSERT INTO Queues " +
                    " SELECT '" + id + "', '" + name + "'";

                SqlCommand queuesSQLCmd = new SqlCommand(queuesSQLQry, connection);

                queuesSQLCmd.CommandTimeout = 300;

                queuesSQLCmd.ExecuteNonQuery();

                if (root.TryGetProperty("mediaSettings", out var mediaSettings))
                {
                    if (mediaSettings.TryGetProperty("call", out var call))
                    {
                        if (call.ValueKind == JsonValueKind.Object && call.EnumerateObject().Any())
                        {
                            await SaveMediaSettings(id, "Call", call);
                        }
                    }

                    if (mediaSettings.TryGetProperty("callback", out var callback))
                    {
                        if (callback.ValueKind == JsonValueKind.Object && callback.EnumerateObject().Any())
                        {
                            await SaveMediaSettings(id, "Callback", callback);
                        }
                    }

                    if (mediaSettings.TryGetProperty("chat", out var chat))
                    {
                        if (chat.ValueKind == JsonValueKind.Object && chat.EnumerateObject().Any())
                        {
                            await SaveMediaSettings(id, "Chat", chat);
                        }
                    }

                    if (mediaSettings.TryGetProperty("email", out var email))
                    {
                        if (email.ValueKind == JsonValueKind.Object && email.EnumerateObject().Any())
                        {
                            await SaveMediaSettings(id, "Email", email);
                        }
                    }

                    if (mediaSettings.TryGetProperty("message", out var message))
                    {
                        if (message.ValueKind == JsonValueKind.Object && message.EnumerateObject().Any())
                        {
                            await SaveMediaSettings(id, "Message", message);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseQueuesByQueueId", "Error: " + ex.Message);
            }
        }


        private async Task SaveMediaSettings(string queueID, string Type, JsonElement a)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                int? alertingTimeoutSeconds = null;
                decimal? percentage = null;
                int? durationMs = null;

                alertingTimeoutSeconds = a.TryGetProperty("alertingTimeoutSeconds", out var ats) ? ats.GetInt32() : 0;
                if (a.TryGetProperty("serviceLevel", out var serviceLevel))
                {
                    percentage = serviceLevel.TryGetProperty("percentage", out var p) ? p.GetDecimal() : 0;
                    durationMs = serviceLevel.TryGetProperty("durationMs", out var d) ? d.GetInt32() : 0;
                }

                string mediaSettingsSQLQry = "INSERT INTO MediaSettings " +
                     " SELECT '" + queueID + "', '" + Type + "', " + alertingTimeoutSeconds + ", " + percentage + ", " + durationMs + " ";


                SqlCommand mediaSettingsSQLCmd = new SqlCommand(mediaSettingsSQLQry, connection);

                mediaSettingsSQLCmd.CommandTimeout = 300;

                mediaSettingsSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveMediaSettings", "Error: " + ex.Message);
            }

        }

        #endregion

        #region UserPresence

        //private async Task CallApiUserPresenceByUserId(string token)
        //{
        //    var agentIds = new List<string>();

        //    using (var conn = new SqlConnection(_connectionString))
        //    {
        //        conn.Open();

        //        var cmd = new SqlCommand(@"
        //            SELECT DISTINCT 
        //          UserId
        //         FROM 
        //          Users
        //         WHERE 
        //          UserId != ''
        //            UNION
        //            SELECT DISTINCT 
        //          UserId
        //         FROM 
        //          Users_All
        //         WHERE 
        //          UserId != ''
        //         UNION 
        //         SELECT DISTINCT 
        //          UserId
        //         FROM 
        //          Participants
        //         WHERE 
        //          UserId != ''
        //        ", conn);

        //        using (var reader = cmd.ExecuteReader())
        //        {
        //            while (reader.Read())
        //            {
        //                agentIds.Add(reader.GetString(0));
        //            }
        //        }
        //    }

        //    foreach (var userId in agentIds)
        //    {
        //        try
        //        {
        //            var client = _httpClientFactory.CreateClient();

        //            client.DefaultRequestHeaders.Authorization =
        //                new AuthenticationHeaderValue("Bearer", token);

        //            var body = new
        //            {
        //                interval = INTERVAL,
        //                order = "asc",
        //                orderBy = "conversationStart",
        //                userFilters = new[]
        //                {
        //                    new
        //                    {
        //                        type = "and",
        //                        predicates = new[]
        //                        {
        //                            new
        //                            {
        //                                dimension = "userId",
        //                                value = userId
        //                            }
        //                        }
        //                    }
        //                }
        //            };

        //            var json = JsonSerializer.Serialize(body);

        //            var response = await client.PostAsync(
        //                API_URL_USER_PRESENCE,
        //                new StringContent(json, Encoding.UTF8, "application/json")
        //            );

        //            var result = await response.Content.ReadAsStringAsync();

        //            await SaveToDatabaseUserPresence(result);
        //        }
        //        catch (Exception ex)
        //        {
        //            await SqlQueryLogsAsync("CallApiUserPresenceByUserId", "Error: " + ex.Message);
        //        }
        //    }
        //}

        private async Task CallApiUserPresenceByUserId(string token)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int pageSize = 100;
            const int maxAttempts = 10;

            int currentPage = 1;
            int totalHits = 0;

            do
            {
                var body = new
                {
                    interval = INTERVAL,
                    paging = new
                    {
                        pageSize = pageSize,
                        pageNumber = currentPage
                    },
                    order = "asc",
                    orderBy = "startTime"
                };

                var json = JsonSerializer.Serialize(body);

                bool downloadSuccess = false;
                int currentAttempt = 0;

                while (!downloadSuccess && currentAttempt < maxAttempts)
                {
                    currentAttempt++;

                    try
                    {
                        using var content = new StringContent(
                            json,
                            Encoding.UTF8,
                            "application/json"
                        );

                        var response = await client.PostAsync(
                            API_URL_USER_PRESENCE,
                            content
                        );

                        // =========================================================
                        // HTTP 429 - RATE LIMIT
                        // =========================================================
                        if (response.StatusCode == HttpStatusCode.TooManyRequests)
                        {
                            int backoffSeconds = 3;

                            if (response.Headers.TryGetValues(
                                "Retry-After",
                                out var values))
                            {
                                var firstValue =
                                    System.Linq.Enumerable.FirstOrDefault(values);

                                if (int.TryParse(
                                    firstValue,
                                    out int parsedSeconds))
                                {
                                    backoffSeconds = parsedSeconds;
                                }
                            }

                            await SqlQueryLogsAsync(
                                "CallApiUserPresenceByUserId",
                                $"429 Rate Limit. Page={currentPage}, " +
                                $"Attempt={currentAttempt}/{maxAttempts}. " +
                                $"Retrying in {backoffSeconds}s."
                            );

                            if (currentAttempt >= maxAttempts)
                            {
                                throw new Exception(
                                    $"Rate limit exceeded after {maxAttempts} attempts. " +
                                    $"Page={currentPage}"
                                );
                            }

                            await Task.Delay(
                                TimeSpan.FromSeconds(backoffSeconds)
                            );

                            continue;
                        }

                        // =========================================================
                        // HTTP 400 - 500
                        // Retry all 4xx/5xx responses
                        // =========================================================
                        int statusCode = (int)response.StatusCode;

                        if (statusCode >= 400 && statusCode <= 500)
                        {
                            var errorBody =
                                await response.Content.ReadAsStringAsync();

                            await SqlQueryLogsAsync(
                                "CallApiUserPresenceByUserId",
                                $"HTTP {statusCode}. Page={currentPage}, " +
                                $"Attempt={currentAttempt}/{maxAttempts}. " +
                                $"Response={errorBody}"
                            );

                            if (currentAttempt >= maxAttempts)
                            {
                                response.EnsureSuccessStatusCode();
                            }

                            // Exponential backoff:
                            // Attempt 1 -> 2 sec
                            // Attempt 2 -> 4 sec
                            // Attempt 3 -> 8 sec
                            // Attempt 4 -> 16 sec
                            int delaySeconds =
                                (int)Math.Pow(2, currentAttempt - 1) * 2;

                            await Task.Delay(
                                TimeSpan.FromSeconds(delaySeconds)
                            );

                            continue;
                        }

                        // =========================================================
                        // SUCCESS
                        // =========================================================
                        response.EnsureSuccessStatusCode();

                        var result =
                            await response.Content.ReadAsStringAsync();

                        if (string.IsNullOrWhiteSpace(result))
                        {
                            break;
                        }

                        if (result.Trim() == "{\"totalHits\":0}")
                        {
                            totalHits = 0;
                            break;
                        }

                        await SaveToDatabaseUserPresencev2(result);

                        // =========================================================
                        // GET PAGINATION INFORMATION
                        // =========================================================
                        using (JsonDocument doc = JsonDocument.Parse(result))
                        {
                            if (doc.RootElement.TryGetProperty(
                                "totalHits",
                                out JsonElement totalHitsElement))
                            {
                                totalHits = totalHitsElement.GetInt32();
                            }
                            else if (doc.RootElement.TryGetProperty(
                                "total",
                                out JsonElement totalElement))
                            {
                                totalHits = totalElement.GetInt32();
                            }
                            else
                            {
                                break;
                            }
                        }

                        downloadSuccess = true;
                    }
                    catch (Exception ex)
                    {
                        await SqlQueryLogsAsync(
                            "CallApiUserPresenceByUserId",
                            $"Exception. Page={currentPage}, " +
                            $"Attempt={currentAttempt}/{maxAttempts}. " +
                            $"Error={ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                        {
                            throw;
                        }

                        // Retry network/connection exceptions
                        int delaySeconds =
                            (int)Math.Pow(2, currentAttempt - 1) * 2;

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                }

                if (!downloadSuccess)
                {
                    break;
                }

                // Small pacing delay before next page
                await Task.Delay(200);

                currentPage++;

            }
            while ((currentPage - 1) * pageSize < totalHits);
        }


        private async Task CallApiUserPresence(string token)
        {
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            var body = new
            {
                interval = INTERVAL,
                order = "asc",
                orderBy = "conversationStart"
            };

            var json = JsonSerializer.Serialize(body);

            var response = await client.PostAsync(
                API_URL_USER_PRESENCE,
                new StringContent(json, Encoding.UTF8, "application/json")
            );

            var result = await response.Content.ReadAsStringAsync();
            await SaveToDatabaseUserPresencev2(result);
        }

        private async Task SaveToDatabaseUserPresencev1(string json)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                foreach (var userDetails in doc.RootElement.GetProperty("userDetails").EnumerateArray())
                {
                    string userId = userDetails.TryGetProperty("userId", out var ui) ? ui.GetString() : null;


                    if (userDetails.TryGetProperty("primaryPresence", out var primaryPresence))
                    {
                        if (primaryPresence.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var m in primaryPresence.EnumerateArray())
                            {
                                string systemPresence = m.TryGetProperty("systemPresence", out var sp) ? sp.GetString() : null;
                                DateTime startTime = m.TryGetProperty("startTime", out var st) ? st.GetDateTime() : DateTime.Parse("1/1/1900");
                                DateTime endTime = m.TryGetProperty("endTime", out var et) ? et.GetDateTime() : DateTime.Parse("1/1/1900");
                                string organizationPresenceId = m.TryGetProperty("organizationPresenceId", out var op) ? op.GetString() : null;


                                string startTimeString = "'" + startTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
                                string endTimeString = "'" + endTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

                                if (startTime == new DateTime(1900, 1, 1))
                                {
                                    startTimeString = "NULL";
                                }

                                if (endTime == new DateTime(1900, 1, 1))
                                {
                                    endTimeString = "NULL";
                                }

                                  

                                string UserPresenceSQLQry = "INSERT INTO UserPresence " +
                                    " SELECT '" + userId + "', '" + systemPresence + "', " + startTimeString + ", " + endTimeString + ", '" + organizationPresenceId + "'";

                                SqlCommand UserPresenceSQLCmd = new SqlCommand(UserPresenceSQLQry, connection);

                                UserPresenceSQLCmd.CommandTimeout = 300;

                                UserPresenceSQLCmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            string systemPresence = primaryPresence.TryGetProperty("systemPresence", out var sp) ? sp.GetString() : null;
                            DateTime startTime = primaryPresence.TryGetProperty("startTime", out var st) ? st.GetDateTime() : DateTime.Parse("1/1/1900");
                            DateTime endTime = primaryPresence.TryGetProperty("endTime", out var et) ? et.GetDateTime() : DateTime.Parse("1/1/1900");
                            string organizationPresenceId = primaryPresence.TryGetProperty("organizationPresenceId", out var op) ? op.GetString() : null;

                            string startTimeString = "'" + startTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
                            string endTimeString = "'" + endTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

                            if (startTime == new DateTime(1900, 1, 1))
                            {
                                startTimeString = "NULL";
                            }

                            if (endTime == new DateTime(1900, 1, 1))
                            {
                                endTimeString = "NULL";
                            }

                            string UserPresenceSQLQry = "INSERT INTO UserPresence " +
                                " SELECT '" + userId + "', '" + systemPresence + "', " + startTimeString + ", " + endTimeString + ", '" + organizationPresenceId + "'";

                            SqlCommand UserPresenceSQLCmd = new SqlCommand(UserPresenceSQLQry, connection);

                            UserPresenceSQLCmd.CommandTimeout = 300;

                            UserPresenceSQLCmd.ExecuteNonQuery();
                        }
                    }

                    if (userDetails.TryGetProperty("routingStatus", out var routingStatus))
                    {
                        if (routingStatus.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var m in routingStatus.EnumerateArray())
                            {
                                string systemPresence = m.TryGetProperty("routingStatus", out var sp) ? sp.GetString() : null;
                                DateTime startTime = m.TryGetProperty("startTime", out var st) ? st.GetDateTime() : DateTime.Parse("1/1/1900");
                                DateTime endTime = m.TryGetProperty("endTime", out var et) ? et.GetDateTime() : DateTime.Parse("1/1/1900");
                                string organizationPresenceId = m.TryGetProperty("organizationPresenceId", out var op) ? op.GetString() : null;

                                string startTimeString = "'" + startTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
                                string endTimeString = "'" + endTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

                                if (startTime == new DateTime(1900, 1, 1))
                                {
                                    startTimeString = "NULL";
                                }

                                if (endTime == new DateTime(1900, 1, 1))
                                {
                                    endTimeString = "NULL";
                                }

                                string UserPresenceSQLQry = "INSERT INTO UserPresence " +
                                    " SELECT '" + userId + "', '" + systemPresence + "', " + startTimeString + ", " + endTimeString + ", '" + organizationPresenceId + "'";

                                SqlCommand UserPresenceSQLCmd = new SqlCommand(UserPresenceSQLQry, connection);

                                UserPresenceSQLCmd.CommandTimeout = 300;

                                UserPresenceSQLCmd.ExecuteNonQuery();
                            }
                        }
                        else
                        {
                            string systemPresence = routingStatus.TryGetProperty("routingStatus", out var sp) ? sp.GetString() : null;
                            DateTime startTime = routingStatus.TryGetProperty("startTime", out var st) ? st.GetDateTime() : DateTime.Parse("1/1/1900");
                            DateTime endTime = routingStatus.TryGetProperty("endTime", out var et) ? et.GetDateTime() : DateTime.Parse("1/1/1900");
                            string organizationPresenceId = routingStatus.TryGetProperty("organizationPresenceId", out var op) ? op.GetString() : null;

                            string startTimeString = "'" + startTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
                            string endTimeString = "'" + endTime.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

                            if (startTime == new DateTime(1900, 1, 1))
                            {
                                startTimeString = "NULL";
                            }

                            if (endTime == new DateTime(1900, 1, 1))
                            {
                                endTimeString = "NULL";
                            }

                            string UserPresenceSQLQry = "INSERT INTO UserPresence " +
                                " SELECT '" + userId + "', '" + systemPresence + "', " + startTimeString + ", " + endTimeString + ", '" + organizationPresenceId + "'";

                            SqlCommand UserPresenceSQLCmd = new SqlCommand(UserPresenceSQLQry, connection);

                            UserPresenceSQLCmd.CommandTimeout = 300;

                            UserPresenceSQLCmd.ExecuteNonQuery();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabaseUserPresence", "Error: " + ex.Message);
            }

        }

        private async Task SaveToDatabaseUserPresencev2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var userPresence = CreateTable(
                    ("UserId", typeof(string)),
                    ("Presence", typeof(string)),
                    ("StartTime", typeof(DateTime)),
                    ("EndTime", typeof(DateTime)),
                    ("OrganizationPresenceId", typeof(string))
                );


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                var root = doc.RootElement;

                if (!root.TryGetProperty("userDetails", out var userDetails) ||
                    userDetails.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'userDetails' array.");
                }


                // =========================================================
                // PARSE USER DETAILS
                // =========================================================

                foreach (var user in userDetails.EnumerateArray())
                {
                    string userId = S(user, "userId");

                    if (string.IsNullOrWhiteSpace(userId))
                        continue;


                    // =====================================================
                    // PRIMARY PRESENCE
                    // =====================================================

                    if (user.TryGetProperty(
                            "primaryPresence",
                            out var primaryPresence))
                    {
                        AddPresence(
                            userPresence,
                            userId,
                            primaryPresence,
                            "systemPresence");
                    }


                    // =====================================================
                    // ROUTING STATUS
                    // =====================================================

                    if (user.TryGetProperty(
                            "routingStatus",
                            out var routingStatus))
                    {
                        AddPresence(
                            userPresence,
                            userId,
                            routingStatus,
                            "routingStatus");
                    }
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (userPresence.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        userPresence,
                        "UserPresence");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUserPresencev2),
                    $"SUCCESS | " +
                    $"UserPresence={userPresence.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseUserPresencev2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }


            // =============================================================
            // LOCAL FUNCTION
            // Handles both ARRAY and OBJECT
            // =============================================================

            void AddPresence(
                DataTable table,
                string userId,
                JsonElement presence,
                string presenceProperty)
            {
                if (presence.ValueKind == JsonValueKind.Array)
                {
                    foreach (var item in presence.EnumerateArray())
                    {
                        AddPresenceRow(
                            table,
                            userId,
                            item,
                            presenceProperty);
                    }

                    return;
                }

                if (presence.ValueKind == JsonValueKind.Object)
                {
                    AddPresenceRow(
                        table,
                        userId,
                        presence,
                        presenceProperty);
                }
            }


            // =============================================================
            // LOCAL FUNCTION
            // Adds one presence record
            // =============================================================

            void AddPresenceRow(
                DataTable table,
                string userId,
                JsonElement presence,
                string presenceProperty)
            {
                string systemPresence =
                    S(presence, presenceProperty);

                DateTime? startTime =
                    D(presence, "startTime");

                DateTime? endTime =
                    D(presence, "endTime");

                string organizationPresenceId =
                    S(presence, "organizationPresenceId");


                table.Rows.Add(
                    Db(userId),
                    Db(systemPresence),
                    Db(startTime),
                    Db(endTime),
                    Db(organizationPresenceId)
                );
            }
        }

        private async Task CallApiPresenceDefinitionByPresenceId(string token)
        {
            var presenceIds = new List<string>();

            // =========================================================
            // GET PRESENCE IDS FROM DATABASE
            // =========================================================
            using (var conn = new SqlConnection(_connectionString))
            {
                await conn.OpenAsync();

                var cmd = new SqlCommand(@"
                    SELECT DISTINCT OrganizationPresenceId
                    FROM UserPresence
                    WHERE OrganizationPresenceId IS NOT NULL
                      AND OrganizationPresenceId != ''
                ", conn);

                cmd.CommandTimeout = 300;

                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        presenceIds.Add(reader.GetString(0));
                    }
                }
            }

            // =========================================================
            // REUSE HTTP CLIENT
            // =========================================================
            var client = _httpClientFactory.CreateClient();

            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            const int maxAttempts = 10;

            // =========================================================
            // PROCESS EACH PRESENCE ID
            // =========================================================
            foreach (var presenceId in presenceIds)
            {
                bool downloadSuccess = false;
                int currentAttempt = 0;

                var url =
                    API_URL_USER_PRESENCE_DEFINITION +
                    $"{presenceId}";

                while (!downloadSuccess && currentAttempt < maxAttempts)
                {
                    currentAttempt++;

                    try
                    {
                        var response = await client.GetAsync(url);

                        int statusCode = (int)response.StatusCode;

                        // =====================================================
                        // HTTP 429 - RATE LIMIT
                        // =====================================================
                        if (response.StatusCode ==
                            HttpStatusCode.TooManyRequests)
                        {
                            int backoffSeconds = 3;

                            if (response.Headers.TryGetValues(
                                "Retry-After",
                                out var values))
                            {
                                var firstValue =
                                    System.Linq.Enumerable.FirstOrDefault(values);

                                if (int.TryParse(
                                    firstValue,
                                    out int parsedSeconds))
                                {
                                    backoffSeconds = parsedSeconds;
                                }
                            }

                            await SqlQueryLogsAsync(
                                "CallApiPresenceDefinitionByPresenceId",
                                $"429 Rate Limit. " +
                                $"PresenceId={presenceId}. " +
                                $"Attempt={currentAttempt}/{maxAttempts}. " +
                                $"Retrying in {backoffSeconds}s."
                            );

                            if (currentAttempt >= maxAttempts)
                            {
                                throw new Exception(
                                    $"Rate limit exceeded after {maxAttempts} attempts. " +
                                    $"PresenceId={presenceId}"
                                );
                            }

                            await Task.Delay(
                                TimeSpan.FromSeconds(backoffSeconds)
                            );

                            continue;
                        }

                        // =====================================================
                        // HTTP 400 - 500
                        // =====================================================
                        if (statusCode >= 400 && statusCode <= 500)
                        {
                            var errorBody =
                                await response.Content.ReadAsStringAsync();

                            await SqlQueryLogsAsync(
                                "CallApiPresenceDefinitionByPresenceId",
                                $"HTTP {statusCode}. " +
                                $"PresenceId={presenceId}. " +
                                $"Attempt={currentAttempt}/{maxAttempts}. " +
                                $"Response={errorBody}"
                            );

                            if (currentAttempt >= maxAttempts)
                            {
                                response.EnsureSuccessStatusCode();
                            }

                            // Exponential backoff
                            //
                            // Attempt 1 = 2 seconds
                            // Attempt 2 = 4 seconds
                            // Attempt 3 = 8 seconds
                            // Attempt 4 = 16 seconds
                            int delaySeconds =
                                (int)Math.Pow(2, currentAttempt - 1) * 2;

                            await Task.Delay(
                                TimeSpan.FromSeconds(delaySeconds)
                            );

                            continue;
                        }

                        // =====================================================
                        // SUCCESS
                        // =====================================================
                        response.EnsureSuccessStatusCode();

                        var json =
                            await response.Content.ReadAsStringAsync();

                        if (string.IsNullOrWhiteSpace(json))
                        {
                            await SqlQueryLogsAsync(
                                "CallApiPresenceDefinitionByPresenceId",
                                $"Empty response. PresenceId={presenceId}"
                            );

                            break;
                        }

                        // =====================================================
                        // SAVE TO DATABASE
                        // =====================================================
                        await SaveToDatabasePresenceDefinitionByPresenceIdv2(
                            json,
                            presenceId
                        );

                        downloadSuccess = true;

                        // Small pacing delay between successful requests
                        await Task.Delay(100);
                    }
                    catch (Exception ex)
                    {
                        await SqlQueryLogsAsync(
                            "CallApiPresenceDefinitionByPresenceId",
                            $"Exception. " +
                            $"PresenceId={presenceId}. " +
                            $"Attempt={currentAttempt}/{maxAttempts}. " +
                            $"Error={ex.Message}"
                        );

                        if (currentAttempt >= maxAttempts)
                        {
                            // Continue to the next PresenceId instead of
                            // terminating the entire process.
                            break;
                        }

                        // Retry network/connection exceptions
                        int delaySeconds =
                            (int)Math.Pow(2, currentAttempt - 1) * 2;

                        await Task.Delay(
                            TimeSpan.FromSeconds(delaySeconds)
                        );
                    }
                }
            }
        }


        private async Task SaveToDatabasePresenceDefinitionByPresenceIdv1(string json, string presenceId)
        {
            try
            {
                using var connection = new SqlConnection(_connectionString);

                await connection.OpenAsync();

                var doc = JsonDocument.Parse(json);

                var root = doc.RootElement;

                //var languageLabelsen = root.GetProperty("languageLabels").TryGetProperty("en", out var en) ? en.GetString() : null;
                var languageLabelsen_US = root.GetProperty("languageLabels").TryGetProperty("en_US", out var en_US) ? en_US.GetString() : null;

                string presenceDefinitionSQLQry = "INSERT INTO PresenceDefinitions " +
                    " SELECT '" + presenceId + "', '" + languageLabelsen_US + "'";

                SqlCommand presenceDefinitionSQLCmd = new SqlCommand(presenceDefinitionSQLQry, connection);

                presenceDefinitionSQLCmd.CommandTimeout = 300;

                presenceDefinitionSQLCmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync("SaveToDatabasePresenceDefinitionByPresenceId", "Error: " + ex.Message);
            }
        }

        private async Task SaveToDatabasePresenceDefinitionByPresenceIdv2(
            string json,
            string presenceId)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLE
                // =========================================================

                var presenceDefinitions = CreateTable(
                    ("PresenceId", typeof(string)),
                    ("en_US", typeof(string))
                );


                // =========================================================
                // VALIDATE PRESENCE ID
                // =========================================================

                if (string.IsNullOrWhiteSpace(presenceId))
                {
                    throw new InvalidOperationException(
                        "Presence ID cannot be empty.");
                }


                // =========================================================
                // VALIDATE JSON
                // =========================================================

                var root = doc.RootElement;

                if (root.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid presence definition object.");
                }


                // =========================================================
                // LANGUAGE LABEL
                // =========================================================

                string name = null;

                if (root.TryGetProperty(
                        "languageLabels",
                        out var languageLabels) &&
                    languageLabels.ValueKind == JsonValueKind.Object)
                {
                    name = S(languageLabels, "en_US");
                }


                // =========================================================
                // ADD ROW
                // =========================================================

                presenceDefinitions.Rows.Add(
                    Db(presenceId),
                    Db(name)
                );


                // =========================================================
                // BULK INSERT
                // =========================================================

                if (presenceDefinitions.Rows.Count > 0)
                {
                    await BulkInsert(
                        conn,
                        presenceDefinitions,
                        "PresenceDefinitions");
                }


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabasePresenceDefinitionByPresenceIdv2),
                    $"SUCCESS | " +
                    $"PresenceDefinitions={presenceDefinitions.Rows.Count}, " +
                    $"PresenceId={presenceId}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabasePresenceDefinitionByPresenceIdv2),
                    $"ERROR | " +
                    $"{ex.GetType().Name} | " +
                    $"{ex.Message} | " +
                    $"PresenceId={presenceId} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }


        #endregion

        #region Conversation

        private async Task CallApiConversations(string token)
        {
            const int pageSize = 100;
            const int maxParallel = 10;
            const int delayMs = 500;

            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

            string firstResult = string.Empty;
            bool initialPageSuccess = false;
            int initialPageAttempts = 0;

            // ---------- Get first page with Rate Limit Protection ----------
            while (!initialPageSuccess && initialPageAttempts < 5)
            {
                initialPageAttempts++;
                try
                {
                    var firstBody = new
                    {
                        interval = INTERVAL,
                        order = "asc",
                        orderBy = "conversationStart",
                        paging = new
                        {
                            pageSize,
                            pageNumber = 1
                        }
                    };

                    var firstJson = JsonSerializer.Serialize(firstBody);
                    var firstResponse = await client.PostAsync(
                        API_URL_CONVERSIONS,
                        new StringContent(firstJson, Encoding.UTF8, "application/json"));

                    // 1. Handle Initial Page Rate Limiting (HTTP 429)
                    if (firstResponse.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        int backoffSeconds = 3;
                        if (firstResponse.Headers.TryGetValues("Retry-After", out var values))
                        {
                            var firstValue = System.Linq.Enumerable.FirstOrDefault(values);
                            if (int.TryParse(firstValue, out int parsedSeconds))
                            {
                                backoffSeconds = parsedSeconds;
                            }
                        }

                        await SqlQueryLogsAsync("CallApiConversations", $"Rate limit on Page 1. Retrying in {backoffSeconds}s...");
                        await Task.Delay(TimeSpan.FromSeconds(backoffSeconds));
                        continue;
                    }

                    firstResponse.EnsureSuccessStatusCode();
                    firstResult = await firstResponse.Content.ReadAsStringAsync();
                    await SaveToDatabaseConversationsv2(firstResult);
                    initialPageSuccess = true;
                }
                catch (Exception ex)
                {
                    if (initialPageAttempts >= 5) throw;
                    await Task.Delay(2000); // 2-second buffer for transient network errors
                }
            }

            int totalHits = 0;
            using (JsonDocument doc = JsonDocument.Parse(firstResult))
            {
                if (doc.RootElement.TryGetProperty("totalHits", out JsonElement totalHitsElement))
                {
                    totalHits = totalHitsElement.GetInt32();
                }
            }

            int totalPages = (int)Math.Ceiling(totalHits / (double)pageSize);
            if (totalPages <= 1)
                return;

            var pages = Enumerable.Range(2, totalPages - 1);

            // ---------- Process remaining pages in parallel ----------
            await Parallel.ForEachAsync(
                pages,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = maxParallel
                },
                async (pageNumber, ct) =>
                {
                    bool downloadSuccess = false;
                    const int maxPageAttempts = 3;
                    int pageAttempts = 0;

                    while (!downloadSuccess && pageAttempts < maxPageAttempts)
                    {
                        pageAttempts++;

                        try
                        {
                            // Stagger delay before each request thread burst
                            await Task.Delay(delayMs, ct);

                            var body = new
                            {
                                interval = INTERVAL,
                                order = "asc",
                                orderBy = "conversationStart",
                                paging = new
                                {
                                    pageSize,
                                    pageNumber
                                }
                            };

                            var json = JsonSerializer.Serialize(body);
                            var response = await client.PostAsync(
                                API_URL_CONVERSIONS,
                                new StringContent(json, Encoding.UTF8, "application/json"),
                                ct);

                            // 2. Handle Downstream Page Parallel Rate Limiting (HTTP 429)
                            if (response.StatusCode == HttpStatusCode.TooManyRequests)
                            {
                                int backoffSeconds = 3;
                                if (response.Headers.TryGetValues("Retry-After", out var values))
                                {
                                    var firstValue = System.Linq.Enumerable.FirstOrDefault(values);
                                    if (int.TryParse(firstValue, out int parsedSeconds))
                                    {
                                        backoffSeconds = parsedSeconds;
                                    }
                                }

                                await SqlQueryLogsAsync("CallApiConversations",
                                    $"Rate Limit Encountered on Page={pageNumber}. Backing off for {backoffSeconds}s... (Attempt {pageAttempts}/{maxPageAttempts})");

                                // Sleep this specific task branch dynamically based on the penalty time requested by Genesys Cloud
                                await Task.Delay(TimeSpan.FromSeconds(backoffSeconds), ct);
                                continue; // Re-run the loop step for this page index
                            }

                            // Log and skip other hard HTTP failures (401, 500, etc.)
                            if (!response.IsSuccessStatusCode)
                            {
                                var errorBody = await response.Content.ReadAsStringAsync(ct);
                                await SqlQueryLogsAsync("CallApiConversations", $"HTTP {(int)response.StatusCode} error on Page={pageNumber}. Body: {errorBody}");
                                break;
                            }

                            var result = await response.Content.ReadAsStringAsync(ct);
                            await SaveToDatabaseConversationsv2(result);

                            downloadSuccess = true; // Complete while-loop for this page
                        }
                        catch (Exception ex)
                        {
                            await SqlQueryLogsAsync("CallApiConversations",
                                $"Exception on Page={pageNumber} (Attempt {pageAttempts}/{maxPageAttempts}): {ex.Message}");

                            if (pageAttempts < maxPageAttempts)
                            {
                                await Task.Delay(2000, ct); // 2-second buffer delay before non-429 connection retry
                            }
                        }
                    }
                });
        }

        private static IEnumerable<string> GetHourlyIntervals(string interval)
        {
            var parts = interval.Split('/');

            var start = DateTime.Parse(parts[0]).ToUniversalTime();
            var end = DateTime.Parse(parts[1]).ToUniversalTime();

            while (start < end)
            {
                var next = start.AddHours(1);

                if (next > end)
                    next = end;

                yield return $"{start:yyyy-MM-ddTHH:mm:ssZ}/{next:yyyy-MM-ddTHH:mm:ssZ}";

                start = next;
            }
        }

   

    //private async Task CallApiConversationsHourly(
    //string token,
    //CancellationToken stoppingToken)
    //    {
    //        const int pageSize = 100;
    //        const int maxParallel = 5;
    //        const int maxRetries = 10;
    //        const int timeoutSeconds = 300;

    //        var client = _httpClientFactory.CreateClient();

    //        client.Timeout = Timeout.InfiniteTimeSpan;
    //        client.DefaultRequestHeaders.Authorization =
    //            new AuthenticationHeaderValue("Bearer", token);

    //        foreach (string interval in GetHourlyIntervals(INTERVAL))
    //        {
    //            if (stoppingToken.IsCancellationRequested)
    //                return;

    //            try
    //            {
    //                await SqlQueryLogsAsync(
    //                    nameof(CallApiConversationsHourly),
    //                    $"START {interval}");

    //                // =========================================================
    //                // GET PAGE
    //                // =========================================================

    //                async Task<string?> GetPage(int page)
    //                {
    //                    string key =
    //                        $"Interval={interval}, Page={page}";

    //                    for (int attempt = 1;
    //                         attempt <= maxRetries;
    //                         attempt++)
    //                    {
    //                        if (stoppingToken.IsCancellationRequested)
    //                            return null;

    //                        try
    //                        {
    //                            var body = new
    //                            {
    //                                interval = interval,
    //                                order = "asc",
    //                                orderBy = "conversationStart",
    //                                paging = new
    //                                {
    //                                    pageSize = pageSize,
    //                                    pageNumber = page
    //                                }
    //                            };

    //                            using var request =
    //                                new HttpRequestMessage(
    //                                    HttpMethod.Post,
    //                                    API_URL_CONVERSIONS);

    //                            request.Content =
    //                                new StringContent(
    //                                    JsonSerializer.Serialize(body),
    //                                    Encoding.UTF8,
    //                                    "application/json");

    //                            // IMPORTANT:
    //                            // This token is ONLY for this HTTP request.
    //                            // It is NOT the service cancellation token.
    //                            using var timeoutCts =
    //                                new CancellationTokenSource(
    //                                    TimeSpan.FromSeconds(
    //                                        timeoutSeconds));

    //                            using var response =
    //                                await client.SendAsync(
    //                                    request,
    //                                    HttpCompletionOption.ResponseHeadersRead,
    //                                    timeoutCts.Token);

    //                            int status =
    //                                (int)response.StatusCode;

    //                            // =================================================
    //                            // SUCCESS
    //                            // =================================================

    //                            if (response.IsSuccessStatusCode)
    //                            {
    //                                string json =
    //                                    await response.Content
    //                                        .ReadAsStringAsync(
    //                                            timeoutCts.Token);

    //                                if (!string.IsNullOrWhiteSpace(json))
    //                                {
    //                                    await SqlQueryLogsAsync(
    //                                        nameof(CallApiConversationsHourly),
    //                                        $"API SUCCESS {key}, " +
    //                                        $"Attempt={attempt}");

    //                                    return json;
    //                                }

    //                                continue;
    //                            }

    //                            // =================================================
    //                            // 404 - SKIP ONLY
    //                            // =================================================

    //                            if (status == 404)
    //                            {
    //                                await SqlQueryLogsAsync(
    //                                    nameof(CallApiConversationsHourly),
    //                                    $"404 SKIP {key}");

    //                                return null;
    //                            }

    //                            // =================================================
    //                            // 400 / 401 / 403 - SKIP
    //                            // =================================================

    //                            if (status == 400 ||
    //                                status == 401 ||
    //                                status == 403)
    //                            {
    //                                string error =
    //                                    await response.Content
    //                                        .ReadAsStringAsync(
    //                                            timeoutCts.Token);

    //                                await SqlQueryLogsAsync(
    //                                    nameof(CallApiConversationsHourly),
    //                                    $"HTTP {status} SKIP {key}: {error}");

    //                                return null;
    //                            }

    //                            // =================================================
    //                            // 429
    //                            // =================================================

    //                            if (status == 429)
    //                            {
    //                                int waitSeconds = 5;

    //                                if (response.Headers.TryGetValues(
    //                                    "Retry-After",
    //                                    out var values) &&
    //                                    int.TryParse(
    //                                        values.FirstOrDefault(),
    //                                        out int retryAfter))
    //                                {
    //                                    waitSeconds =
    //                                        Math.Max(1, retryAfter);
    //                                }

    //                                await SqlQueryLogsAsync(
    //                                    nameof(CallApiConversationsHourly),
    //                                    $"429 {key}, " +
    //                                    $"Attempt={attempt}/{maxRetries}, " +
    //                                    $"Wait={waitSeconds}s");

    //                                if (attempt < maxRetries)
    //                                {
    //                                    // Don't use service token here.
    //                                    await Task.Delay(
    //                                        TimeSpan.FromSeconds(
    //                                            waitSeconds));
    //                                }

    //                                continue;
    //                            }

    //                            // =================================================
    //                            // 5XX
    //                            // =================================================

    //                            if (status >= 500)
    //                            {
    //                                await SqlQueryLogsAsync(
    //                                    nameof(CallApiConversationsHourly),
    //                                    $"HTTP {status} {key}, " +
    //                                    $"Attempt={attempt}/{maxRetries}");

    //                                if (attempt < maxRetries)
    //                                {
    //                                    await Task.Delay(
    //                                        TimeSpan.FromSeconds(
    //                                            Math.Min(
    //                                                30,
    //                                                attempt * 2)));
    //                                }

    //                                continue;
    //                            }

    //                            // =================================================
    //                            // OTHER STATUS
    //                            // =================================================

    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"HTTP {status} SKIP {key}");

    //                            return null;
    //                        }

    //                        // =====================================================
    //                        // REQUEST TIMEOUT
    //                        // =====================================================

    //                        catch (OperationCanceledException)
    //                        {
    //                            // This is the HTTP timeout.
    //                            // Do NOT throw it to Worker.
    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"HTTP TIMEOUT {key}, " +
    //                                $"Attempt={attempt}/{maxRetries}");

    //                            if (attempt < maxRetries)
    //                            {
    //                                await Task.Delay(
    //                                    TimeSpan.FromSeconds(
    //                                        Math.Min(
    //                                            30,
    //                                            attempt * 2)));
    //                            }
    //                        }

    //                        // =====================================================
    //                        // SOCKET / NETWORK ERROR
    //                        // =====================================================

    //                        catch (HttpRequestException ex)
    //                        {
    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"HTTP ERROR {key}, " +
    //                                $"Attempt={attempt}/{maxRetries}: " +
    //                                $"{ex.Message}");

    //                            if (attempt < maxRetries)
    //                            {
    //                                await Task.Delay(
    //                                    TimeSpan.FromSeconds(
    //                                        Math.Min(
    //                                            30,
    //                                            attempt * 2)));
    //                            }
    //                        }

    //                        // =====================================================
    //                        // SOCKET 995 / IO ERROR
    //                        // =====================================================

    //                        catch (IOException ex)
    //                        {
    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"IO ERROR {key}, " +
    //                                $"Attempt={attempt}/{maxRetries}: " +
    //                                $"{ex.Message}");

    //                            if (attempt < maxRetries)
    //                            {
    //                                await Task.Delay(
    //                                    TimeSpan.FromSeconds(
    //                                        Math.Min(
    //                                            30,
    //                                            attempt * 2)));
    //                            }
    //                        }

    //                        // =====================================================
    //                        // ANY OTHER ERROR
    //                        // =====================================================

    //                        catch (Exception ex)
    //                        {
    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"API ERROR {key}, " +
    //                                $"Attempt={attempt}/{maxRetries}: {ex}");

    //                            if (attempt < maxRetries)
    //                            {
    //                                await Task.Delay(
    //                                    TimeSpan.FromSeconds(5));
    //                            }
    //                        }
    //                    }

    //                    await SqlQueryLogsAsync(
    //                        nameof(CallApiConversationsHourly),
    //                        $"API FAILED {key}");

    //                    return null;
    //                }

    //                // =========================================================
    //                // SAVE DATABASE
    //                // =========================================================

    //                async Task<bool> SavePage(
    //                    string json,
    //                    int page)
    //                {
    //                    string key =
    //                        $"Interval={interval}, Page={page}";

    //                    for (int attempt = 1;
    //                         attempt <= maxRetries;
    //                         attempt++)
    //                    {
    //                        try
    //                        {
                              
    //                            await SaveToDatabaseConversations(
    //                                json);

    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"DB SUCCESS {key}");

    //                            return true;
    //                        }
    //                        catch (Exception ex)
    //                        {
    //                            await SqlQueryLogsAsync(
    //                                nameof(CallApiConversationsHourly),
    //                                $"DB ERROR {key}, " +
    //                                $"Attempt={attempt}/{maxRetries}: " +
    //                                $"{ex.Message}");

    //                            if (attempt < maxRetries)
    //                            {
    //                                await Task.Delay(
    //                                    TimeSpan.FromSeconds(
    //                                        Math.Min(
    //                                            30,
    //                                            attempt * 2)));
    //                            }
    //                        }
    //                    }

    //                    return false;
    //                }

    //                // =========================================================
    //                // PAGE 1
    //                // =========================================================

    //                string? first =
    //                    await GetPage(1);

    //                if (string.IsNullOrWhiteSpace(first))
    //                {
    //                    await SqlQueryLogsAsync(
    //                        nameof(CallApiConversationsHourly),
    //                        $"PAGE 1 FAILED - SKIP {interval}");

    //                    continue;
    //                }

    //                if (!await SavePage(first, 1))
    //                {
    //                    await SqlQueryLogsAsync(
    //                        nameof(CallApiConversationsHourly),
    //                        $"PAGE 1 SAVE FAILED - SKIP {interval}");

    //                    continue;
    //                }

    //                // =========================================================
    //                // TOTAL HITS
    //                // =========================================================

    //                int totalHits;

    //                try
    //                {
    //                    using var doc =
    //                        JsonDocument.Parse(first);

    //                    if (!doc.RootElement.TryGetProperty(
    //                        "totalHits",
    //                        out var hits))
    //                    {
    //                        await SqlQueryLogsAsync(
    //                            nameof(CallApiConversationsHourly),
    //                            $"totalHits missing - SKIP {interval}");

    //                        continue;
    //                    }

    //                    totalHits =
    //                        hits.GetInt32();
    //                }
    //                catch (Exception ex)
    //                {
    //                    await SqlQueryLogsAsync(
    //                        nameof(CallApiConversationsHourly),
    //                        $"JSON ERROR {interval}: {ex.Message}");

    //                    continue;
    //                }

    //                if (totalHits <= pageSize)
    //                {
    //                    await SqlQueryLogsAsync(
    //                        nameof(CallApiConversationsHourly),
    //                        $"COMPLETE {interval}");

    //                    continue;
    //                }

    //                int totalPages =
    //                    (int)Math.Ceiling(
    //                        totalHits / (double)pageSize);

    //                await SqlQueryLogsAsync(
    //                    nameof(CallApiConversationsHourly),
    //                    $"PAGES {interval}: " +
    //                    $"Hits={totalHits}, Pages={totalPages}");

    //                // =========================================================
    //                // CONTROLLED PARALLEL PROCESSING
    //                // =========================================================

    //                using var semaphore =
    //                    new SemaphoreSlim(maxParallel);

    //                var tasks =
    //                    new List<Task>();

    //                for (int page = 2;
    //                     page <= totalPages;
    //                     page++)
    //                {
    //                    int currentPage = page;

    //                    await semaphore.WaitAsync();

    //                    tasks.Add(
    //                        Task.Run(async () =>
    //                        {
    //                            try
    //                            {
    //                                string? json =
    //                                    await GetPage(currentPage);

    //                                if (string.IsNullOrWhiteSpace(json))
    //                                {
    //                                    await SqlQueryLogsAsync(
    //                                        nameof(
    //                                            CallApiConversationsHourly),
    //                                        $"PAGE FAILED {interval}, " +
    //                                        $"Page={currentPage}");

    //                                    return;
    //                                }

    //                                if (!await SavePage(
    //                                    json,
    //                                    currentPage))
    //                                {
    //                                    await SqlQueryLogsAsync(
    //                                        nameof(
    //                                            CallApiConversationsHourly),
    //                                        $"PAGE SAVE FAILED {interval}, " +
    //                                        $"Page={currentPage}");

    //                                    return;
    //                                }

    //                                await SqlQueryLogsAsync(
    //                                    nameof(
    //                                        CallApiConversationsHourly),
    //                                    $"PAGE COMPLETE {interval}, " +
    //                                    $"Page={currentPage}/{totalPages}");
    //                            }
    //                            catch (Exception ex)
    //                            {
    //                                // NEVER allow an individual page
    //                                // to terminate the service.
    //                                await SqlQueryLogsAsync(
    //                                    nameof(
    //                                        CallApiConversationsHourly),
    //                                    $"PAGE EXCEPTION {interval}, " +
    //                                    $"Page={currentPage}: {ex}");
    //                            }
    //                            finally
    //                            {
    //                                semaphore.Release();
    //                            }
    //                        }));
    //                }

    //                await Task.WhenAll(tasks);

    //                await SqlQueryLogsAsync(
    //                    nameof(CallApiConversationsHourly),
    //                    $"INTERVAL COMPLETE {interval}");
    //            }
    //            catch (Exception ex)
    //            {
    //                // NEVER allow an interval failure to stop service.
    //                await SqlQueryLogsAsync(
    //                    nameof(CallApiConversationsHourly),
    //                    $"INTERVAL ERROR {interval}: {ex}");
    //            }
    //        }

    //        await SqlQueryLogsAsync(
    //            nameof(CallApiConversationsHourly),
    //            "ALL INTERVALS COMPLETED.");
    //    }


    private async Task CallApiConversationsHourly(string token)
        {
            const int pageSize = 100;
            const int maxParallel = 5;
            const int maxRetries = 5;
            const int timeoutSeconds = 180;

            using var client = _httpClientFactory.CreateClient();
            client.Timeout = Timeout.InfiniteTimeSpan;
            client.DefaultRequestHeaders.Authorization =
                new AuthenticationHeaderValue("Bearer", token);

            foreach (var interval in GetHourlyIntervals(INTERVAL))
            {
                try
                {
                    async Task<string> GetPage(int page)
                    {
                        for (int attempt = 1; attempt <= maxRetries; attempt++)
                        {
                            try
                            {
                                var body = new
                                {
                                    interval,
                                    order = "asc",
                                    orderBy = "conversationStart",
                                    paging = new { pageSize, pageNumber = page }
                                };

                                using var req = new HttpRequestMessage(
                                    HttpMethod.Post, API_URL_CONVERSIONS)
                                {
                                    Content = new StringContent(
                                        JsonSerializer.Serialize(body),
                                        Encoding.UTF8,
                                        "application/json")
                                };

                                using var timeout = new CancellationTokenSource(
                                    TimeSpan.FromSeconds(timeoutSeconds));

                                using var res = await client.SendAsync(
                                    req,
                                    HttpCompletionOption.ResponseContentRead,
                                    timeout.Token);

                                int status = (int)res.StatusCode;

                                if (res.IsSuccessStatusCode)
                                    return await res.Content.ReadAsStringAsync();

                                if (status == 404)
                                {
                                    await SqlQueryLogsAsync(
                                        nameof(CallApiConversationsHourly),
                                        $"404 SKIP Interval={interval} Page={page}");
                                    return null;
                                }

                                if (status == 429)
                                {
                                    int wait = 5;

                                    if (res.Headers.TryGetValues(
                                        "Retry-After", out var values))
                                        int.TryParse(values.FirstOrDefault(), out wait);

                                    await Task.Delay(
                                        TimeSpan.FromSeconds(Math.Max(1, wait)));

                                    continue;
                                }

                                if (status >= 400 && status < 500)
                                    return null;

                                if (status >= 500 && attempt < maxRetries)
                                {
                                    await Task.Delay(
                                        TimeSpan.FromSeconds(attempt * 2));
                                    continue;
                                }
                            }
                            catch (OperationCanceledException)
                            {
                                if (attempt < maxRetries)
                                    await Task.Delay(
                                        TimeSpan.FromSeconds(attempt * 2));
                            }
                            catch (HttpRequestException)
                            {
                                if (attempt < maxRetries)
                                    await Task.Delay(
                                        TimeSpan.FromSeconds(attempt * 2));
                            }
                        }

                        return null;
                    }

                    // Page 1
                    var first = await GetPage(1);

                    if (string.IsNullOrWhiteSpace(first))
                        continue;

                    await SaveToDatabaseConversationsv2(first);

                    using var doc = JsonDocument.Parse(first);

                    if (!doc.RootElement.TryGetProperty(
                        "totalHits", out var hits))
                        continue;

                    int totalHits = hits.GetInt32();

                    if (totalHits <= pageSize)
                        continue;

                    int totalPages =
                        (int)Math.Ceiling(totalHits / (double)pageSize);

                    // Remaining pages
                    await Parallel.ForEachAsync(
                        Enumerable.Range(2, totalPages - 1),
                        new ParallelOptions
                        {
                            MaxDegreeOfParallelism = maxParallel
                        },
                        async (page, _) =>
                        {
                            try
                            {
                                var result = await GetPage(page);

                                if (!string.IsNullOrWhiteSpace(result))
                                    await SaveToDatabaseConversationsv2(result);
                            }
                            catch (Exception ex)
                            {
                                await SqlQueryLogsAsync(
                                    nameof(CallApiConversationsHourly),
                                    $"Page {page} ERROR: {ex.Message}");
                            }
                        });

                    await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    await SqlQueryLogsAsync(
                        nameof(CallApiConversationsHourly),
                        $"Interval {interval} ERROR: {ex.Message}");
                }
            }
        }




        //private async Task SaveToDatabaseConversations(string json)
        //{
        //    try
        //    {
        //        json = json.Replace("'", "");

        //        using var connection = new SqlConnection(_connectionString);
        //        await connection.OpenAsync();

        //        var doc = JsonDocument.Parse(json);

        //        foreach (var conv in doc.RootElement.GetProperty("conversations").EnumerateArray())
        //        {
        //            string conversationId = conv.TryGetProperty("conversationId", out var ci) ? ci.GetString() : null;
        //            DateTime conversationstart = conv.TryGetProperty("conversationStart", out var cs) ? cs.GetDateTime() : DateTime.Parse("1/1/1900");
        //            DateTime conversationend = conv.TryGetProperty("conversationEnd", out var ce) ? ce.GetDateTime() : DateTime.Parse("1/1/1900");
        //            string ConversationOriginatingDirection = conv.TryGetProperty("originatingDirection", out var od) ? od.GetString() : null;

        //            string conversationstartString = "'" + conversationstart.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
        //            string conversationendString = "'" + conversationend.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //            if (conversationstart == new DateTime(1900, 1, 1))
        //            {
        //                conversationstartString = "NULL";
        //            }

        //            if (conversationend == new DateTime(1900, 1, 1))
        //            {
        //                conversationendString = "NULL";
        //            }

        //            string conversationsSQLQry = "INSERT INTO Conversations_Old " +
        //                " SELECT '" + conversationId + "', " + conversationstartString + ", " + conversationendString + ", '" + ConversationOriginatingDirection + "'";

        //            SqlCommand conversationsSQLCmd = new SqlCommand(conversationsSQLQry, connection);

        //            conversationsSQLCmd.CommandTimeout = 300;

        //            foreach (var participant in conv.GetProperty("participants").EnumerateArray())
        //            {
        //                string participantId = participant.TryGetProperty("participantId", out var pai) ? pai.GetString() : null;
        //                string participantName = participant.TryGetProperty("participantName", out var pn) ? pn.GetString() : null;
        //                string participantpurpose = participant.TryGetProperty("purpose", out var pu) ? pu.GetString() : null;
        //                string participantuserId = participant.TryGetProperty("userId", out var pud) ? pud.GetString() : null;

        //                string participantsSQLQry = "INSERT INTO Participants_Old " +
        //                    " SELECT '" + participantId + "', '" + conversationId + "', '" + participantName + "', '" + participantpurpose + "', '" + participantuserId + "'";

        //                SqlCommand participantsSQLCmd = new SqlCommand(participantsSQLQry, connection);

        //                participantsSQLCmd.CommandTimeout = 300;

        //                participantsSQLCmd.ExecuteNonQuery();

        //                foreach (var session in participant.GetProperty("sessions").EnumerateArray())
        //                {
        //                    string sessionId = session.TryGetProperty("sessionId", out var si) ? si.GetString() : null;
        //                    string sessionani = session.TryGetProperty("ani", out var sa) ? sa.GetString() : null;
        //                    string sessiondnis = session.TryGetProperty("dnis", out var sd) ? sd.GetString() : null;
        //                    string sessiondirection = session.TryGetProperty("direction", out var di) ? di.GetString() : null;
        //                    string sessionmediaType = session.TryGetProperty("mediaType", out var mt) ? mt.GetString() : null;
        //                    string sessionagentId = session.TryGetProperty("selectedAgentId", out var sai) ? sai.GetString() : null;

        //                    string edgeid = session.TryGetProperty("edgeId", out var ei) ? ei.GetString() : null;
        //                    string protocolcallid = session.TryGetProperty("protocolCallId", out var pci) ? pci.GetString() : null;
        //                    string provider = session.TryGetProperty("provider", out var pro) ? pro.GetString() : null;
        //                    string remotenamedisplayable = session.TryGetProperty("remoteNameDisplayable", out var rdp) ? rdp.GetString() : null;
        //                    string sessiondnis2 = session.TryGetProperty("sessionDnis", out var sds) ? sds.GetString() : null;
        //                    string callbackusername = session.TryGetProperty("callbackUserName", out var cbu) ? cbu.GetString() : null;
        //                    string outboundcampaignid = session.TryGetProperty("outboundCampaignId", out var ocai) ? ocai.GetString() : null;
        //                    string outboundcontactid = session.TryGetProperty("outboundContactId", out var ocoi) ? ocoi.GetString() : null;
        //                    string outboundcontactlistid = session.TryGetProperty("outboundContactListId", out var ocoli) ? ocoli.GetString() : null;
        //                    string scriptid = session.TryGetProperty("scriptId", out var sci) ? sci.GetString() : null;
        //                    DateTime detectedspeechend = session.TryGetProperty("detectedSpeechEnd", out var dsh) ? dsh.GetDateTime() : DateTime.Parse("1/1/1900");
        //                    DateTime detectedspeechstart = session.TryGetProperty("detectedSpeechStart", out var dss) ? dss.GetDateTime() : DateTime.Parse("1/1/1900");
        //                    string dispositionanalyzer = session.TryGetProperty("dispositionAnalyzer", out var dpa) ? dpa.GetString() : null;
        //                    string dispositionname = session.TryGetProperty("dispositionName", out var dpn) ? dpn.GetString() : null;
        //                    string peerid = session.TryGetProperty("peerId", out var pi) ? pi.GetString() : null;
        //                    string remote = session.TryGetProperty("remote", out var rem) ? rem.GetString() : null;
        //                    string usedrouting = session.TryGetProperty("usedRouting", out var uro) ? uro.GetString() : null;

        //                    DateTime? result = detectedspeechend == new DateTime(1900, 1, 1) ? (DateTime?)null : detectedspeechend;

        //                    string detectedspeechendString = "'" + detectedspeechend.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
        //                    string detectedspeechstartString = "'" + detectedspeechstart.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //                    if (detectedspeechend == new DateTime(1900, 1, 1))
        //                    {
        //                        detectedspeechendString = "NULL";
        //                    }

        //                    if (detectedspeechstart == new DateTime(1900, 1, 1))
        //                    {
        //                        detectedspeechstartString = "NULL";
        //                    }

        //                    string sessionsSQLQry = "INSERT INTO Sessions_Old " +
        //                        " SELECT '" + sessionId + "', '" + participantId + "', '" + sessionani + "', '', '" + sessiondirection + "', '" + sessionmediaType + "', '"
        //                        + sessionagentId + "', '" + edgeid + "', '" + protocolcallid + "', '"
        //                        + provider + "', '" + remotenamedisplayable + "', '', '"
        //                        + callbackusername + "', '" + outboundcampaignid + "', '" + outboundcontactid + "', '"
        //                        + outboundcontactlistid + "', '" + scriptid + "', " + detectedspeechendString + ", "
        //                        + detectedspeechstartString + ", '" + dispositionanalyzer + "', '" + dispositionname + "', '"
        //                        + peerid + "', '" + remote + "', '" + usedrouting + "'";

        //                    SqlCommand sessionsSQLCmd = new SqlCommand(sessionsSQLQry, connection);

        //                    sessionsSQLCmd.CommandTimeout = 300;

        //                    sessionsSQLCmd.ExecuteNonQuery();

        //                    if (session.TryGetProperty("flow", out var flows))
        //                    {
        //                        if (flows.ValueKind == JsonValueKind.Array)
        //                        {
        //                            foreach (var m in flows.EnumerateArray())
        //                            {
        //                                string flowId = m.TryGetProperty("flowId", out var fid) ? fid.GetString() : null;
        //                                string flowName = m.TryGetProperty("flowName", out var fn) ? fn.GetString() : null;
        //                                string flowType = m.TryGetProperty("flowType", out var ft) ? ft.GetString() : null;
        //                                string flowtransferTargetName = m.TryGetProperty("transferTargetName", out var ftn) ? ftn.GetString() : null;
        //                                string flowtransferType = m.TryGetProperty("transferType", out var tt) ? tt.GetString() : null;

        //                                string flowsSQLQry = "INSERT INTO Flows_Old " +
        //                                "SELECT '" + flowId + "','" + sessionId + "','" + flowName + "','" + flowType
        //                                + "','" + flowtransferTargetName + "','" + flowtransferType + "'";

        //                                SqlCommand flowsSQLCmd = new SqlCommand(flowsSQLQry, connection);

        //                                flowsSQLCmd.CommandTimeout = 300;

        //                                flowsSQLCmd.ExecuteNonQuery();
        //                            }
        //                        }
        //                        else
        //                        {
        //                            string flowId = flows.TryGetProperty("flowId", out var fid) ? fid.GetString() : null;
        //                            string flowName = flows.TryGetProperty("flowName", out var fn) ? fn.GetString() : null;
        //                            string flowType = flows.TryGetProperty("flowType", out var ft) ? ft.GetString() : null;
        //                            string flowtransferTargetName = flows.TryGetProperty("transferTargetName", out var ftn) ? ftn.GetString() : null;
        //                            string flowtransferType = flows.TryGetProperty("transferType", out var ftt) ? ftt.GetString() : null;

        //                            string flowsSQLQry = "INSERT INTO Flows_Old " +
        //                                "SELECT '" + flowId + "','" + sessionId + "','" + flowName + "','" + flowType
        //                                + "','" + flowtransferTargetName + "','" + flowtransferType + "'";

        //                            SqlCommand flowsSQLCmd = new SqlCommand(flowsSQLQry, connection);

        //                            flowsSQLCmd.CommandTimeout = 300;

        //                            flowsSQLCmd.ExecuteNonQuery();
        //                        }
        //                    }

        //                    if (session.TryGetProperty("metrics", out var metrics))
        //                    {
        //                        if (metrics.ValueKind == JsonValueKind.Array)
        //                        {
        //                            foreach (var m in metrics.EnumerateArray())
        //                            {
        //                                DateTime metricemitDate = m.TryGetProperty("emitDate", out var ed) ? ed.GetDateTime() : DateTime.Parse("1/1/1900");
        //                                string metricname = m.TryGetProperty("name", out var n) ? n.GetString() : null;
        //                                int metricvalue = m.TryGetProperty("value", out var v) ? v.GetInt32() : 0;

        //                                string metricemitDateString = "'" + metricemitDate.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //                                if (metricemitDate == new DateTime(1900, 1, 1))
        //                                {
        //                                    metricemitDateString = "NULL";
        //                                }

        //                                string metricsSQLQry = "INSERT INTO Metrics_Old " +
        //                                "SELECT '" + sessionId + "'," + metricemitDateString + ",'" + metricname + "'," + metricvalue.ToString() + "";

        //                                SqlCommand metricsSQLCmd = new SqlCommand(metricsSQLQry, connection);

        //                                metricsSQLCmd.CommandTimeout = 300;

        //                                metricsSQLCmd.ExecuteNonQuery();
        //                            }
        //                        }
        //                        else
        //                        {
        //                            DateTime metricemitDate = metrics.TryGetProperty("emitDate", out var ed) ? ed.GetDateTime() : DateTime.Parse("1/1/1900");
        //                            string metricname = metrics.TryGetProperty("name", out var n) ? n.GetString() : null;
        //                            int metricvalue = metrics.TryGetProperty("value", out var v) ? v.GetInt32() : 0;

        //                            string metricemitDateString = "'" + metricemitDate.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //                            if (metricemitDate == new DateTime(1900, 1, 1))
        //                            {
        //                                metricemitDateString = "NULL";
        //                            }

        //                            string metricsSQLQry = "INSERT INTO Metrics_Old " +
        //                                "SELECT '" + sessionId + "'," + metricemitDateString + ",'" + metricname + "'," + metricvalue.ToString() + "";

        //                            SqlCommand metricsSQLCmd = new SqlCommand(metricsSQLQry, connection);

        //                            metricsSQLCmd.CommandTimeout = 300;

        //                            metricsSQLCmd.ExecuteNonQuery();
        //                        }
        //                    }

        //                    if (session.TryGetProperty("segments", out var segments))
        //                    {
        //                        if (segments.ValueKind == JsonValueKind.Array)
        //                        {
        //                            foreach (var m in segments.EnumerateArray())
        //                            {
        //                                DateTime segmentStart = m.TryGetProperty("segmentStart", out var ss) ? ss.GetDateTime() : DateTime.Parse("1/1/1900");
        //                                DateTime segmentEnd = m.TryGetProperty("segmentEnd", out var se) ? se.GetDateTime() : DateTime.Parse("1/1/1900");
        //                                string segmentType = m.TryGetProperty("segmentType", out var st) ? st.GetString() : null;
        //                                string segmentqueueId = m.TryGetProperty("queueId", out var qi) ? qi.GetString() : null;
        //                                string segmentdisconnectType = m.TryGetProperty("disconnectType", out var dt) ? dt.GetString() : null;

        //                                string segmentStartString = "'" + segmentStart.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
        //                                string segmentEndString = "'" + segmentEnd.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //                                if (segmentStart == new DateTime(1900, 1, 1))
        //                                {
        //                                    segmentStartString = "NULL";
        //                                }

        //                                if (segmentEnd == new DateTime(1900, 1, 1))
        //                                {
        //                                    segmentEndString = "NULL";
        //                                }

        //                                string segmentsSQLQry = "INSERT INTO Segments_Old " +
        //                                "SELECT '" + sessionId + "'," + segmentStartString + "," + segmentEndString + ",'" + segmentType + "','" + segmentqueueId + "','" + segmentdisconnectType + "'";

        //                                SqlCommand segmentsSQLCmd = new SqlCommand(segmentsSQLQry, connection);

        //                                segmentsSQLCmd.CommandTimeout = 300;

        //                                segmentsSQLCmd.ExecuteNonQuery();
        //                            }
        //                        }
        //                        else
        //                        {
        //                            DateTime segmentStart = segments.TryGetProperty("segmentStart", out var ss) ? ss.GetDateTime() : DateTime.Parse("1/1/1900");
        //                            DateTime segmentEnd = segments.TryGetProperty("segmentEnd", out var se) ? se.GetDateTime() : DateTime.Parse("1/1/1900");
        //                            string segmentType = segments.TryGetProperty("segmentType", out var st) ? st.GetString() : null;
        //                            string segmentqueueId = segments.TryGetProperty("queueId", out var qi) ? qi.GetString() : null;
        //                            string segmentdisconnectType = segments.TryGetProperty("disconnectType", out var dt) ? dt.GetString() : null;

        //                            string segmentStartString = "'" + segmentStart.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";
        //                            string segmentEndString = "'" + segmentEnd.ToString("MM/dd/yyyy HH:mm:ss:fff") + "'";

        //                            if (segmentStart == new DateTime(1900, 1, 1))
        //                            {
        //                                segmentStartString = "NULL";
        //                            }

        //                            if (segmentEnd == new DateTime(1900, 1, 1))
        //                            {
        //                                segmentEndString = "NULL";
        //                            }

        //                            string segmentsSQLQry = "INSERT INTO Segments_Old " +
        //                               "SELECT '" + sessionId + "'," + segmentStartString + "," + segmentEndString + ",'" + segmentType + "','" + segmentqueueId + "','" + segmentdisconnectType + "'";

        //                            SqlCommand segmentsSQLCmd = new SqlCommand(segmentsSQLQry, connection);

        //                            segmentsSQLCmd.CommandTimeout = 300;

        //                            segmentsSQLCmd.ExecuteNonQuery();
        //                        }
        //                    }
        //                }
        //            }

        //            await conversationsSQLCmd.ExecuteNonQueryAsync();
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        await SqlQueryLogsAsync("SaveToDatabaseConversations", "Error: " + ex.Message + json);
        //    }
        //}

        //private async Task SaveToDatabaseConversationsv1(string json)
        //{
        //    var sw = Stopwatch.StartNew();

        //    try
        //    {
        //        using var doc = JsonDocument.Parse(json);
        //        using var conn = new SqlConnection(_connectionString);

        //        await conn.OpenAsync();

        //        var conversations = new DataTable();
        //        conversations.Columns.Add("ConversationId", typeof(string));
        //        conversations.Columns.Add("ConversationStart", typeof(DateTime));
        //        conversations.Columns.Add("ConversationEnd", typeof(DateTime));
        //        conversations.Columns.Add("ConversationOriginatingDirection", typeof(string));

        //        var participants = new DataTable();
        //        participants.Columns.Add("ParticipantId", typeof(string));
        //        participants.Columns.Add("ConversationId", typeof(string));
        //        participants.Columns.Add("ParticipantName", typeof(string));
        //        participants.Columns.Add("ParticipantPurpose", typeof(string));
        //        participants.Columns.Add("ParticipantUserId", typeof(string));

        //        var sessions = new DataTable();
        //        string[] sessionCols =
        //        {
        //    "SessionId","ParticipantId","SessionANI","SessionDNIS","SessionDirection","SessionMediaType",
        //    "SessionAgentId","SessionsEdgeId","SessionsProtocolCallId","SessionsProvider",
        //    "SessionsRemoteNameDisplayable","SessionsDNIS2","SessionsCallbackUserName",
        //    "SessionsOutboundCampaignId","SessionsOutboundContactId","SessionsOutboundContactListId",
        //    "SessionsScriptId","SessionsDetectedSpeechEnd","SessionsDetectedSpeechStart",
        //    "SessionsDispositionAnalyzer","SessionsDispositionName","SessionsPeerId","SessionsRemote","SessionsUsedRouting"
        //};

        //        foreach (var col in sessionCols)
        //            sessions.Columns.Add(
        //                col,
        //                col.Contains("Speech") ? typeof(DateTime) : typeof(string));

        //        var flows = new DataTable();
        //        foreach (var col in new[]
        //        {
        //    "FlowId","SessionId","FlowName","FlowType",
        //    "FlowTransferTargetName","FlowTransferType"
        //})
        //            flows.Columns.Add(col, typeof(string));

        //        var metrics = new DataTable();
        //        metrics.Columns.Add("SessionId", typeof(string));
        //        metrics.Columns.Add("MetricEmitDate", typeof(DateTime));
        //        metrics.Columns.Add("MetricName", typeof(string));
        //        metrics.Columns.Add("MetricValue", typeof(int));

        //        var segments = new DataTable();
        //        segments.Columns.Add("SessionId", typeof(string));
        //        segments.Columns.Add("SegmentStart", typeof(DateTime));
        //        segments.Columns.Add("SegmentEnd", typeof(DateTime));
        //        segments.Columns.Add("SegmentType", typeof(string));
        //        segments.Columns.Add("SegmentQueueId", typeof(string));
        //        segments.Columns.Add("DisconnectType", typeof(string));

        //        object V(string value) =>
        //            string.IsNullOrEmpty(value)
        //                ? DBNull.Value
        //                : value;

        //        foreach (var c in doc.RootElement
        //            .GetProperty("conversations")
        //            .EnumerateArray())
        //        {
        //            string cid = S(c, "conversationId");

        //            conversations.Rows.Add(
        //                V(cid),
        //                D(c, "conversationStart") ?? (object)DBNull.Value,
        //                D(c, "conversationEnd") ?? (object)DBNull.Value,
        //                V(S(c, "originatingDirection")));

        //            foreach (var p in A(c, "participants"))
        //            {
        //                string pid = S(p, "participantId");

        //                participants.Rows.Add(
        //                    V(pid),
        //                    V(cid),
        //                    V(S(p, "participantName")),
        //                    V(S(p, "purpose")),
        //                    V(S(p, "userId")));

        //                foreach (var s in A(p, "sessions"))
        //                {
        //                    string sid = S(s, "sessionId");

        //                    sessions.Rows.Add(
        //                        V(sid),
        //                        V(pid),
        //                        V(S(s, "ani")),
        //                        V(S(s, "dnis")),
        //                        V(S(s, "direction")),
        //                        V(S(s, "mediaType")),
        //                        V(S(s, "selectedAgentId")),
        //                        V(S(s, "edgeId")),
        //                        V(S(s, "protocolCallId")),
        //                        V(S(s, "provider")),
        //                        V(S(s, "remoteNameDisplayable")),
        //                        V(S(s, "sessionDnis")),
        //                        V(S(s, "callbackUserName")),
        //                        V(S(s, "outboundCampaignId")),
        //                        V(S(s, "outboundContactId")),
        //                        V(S(s, "outboundContactListId")),
        //                        V(S(s, "scriptId")),
        //                        D(s, "detectedSpeechEnd") ?? (object)DBNull.Value,
        //                        D(s, "detectedSpeechStart") ?? (object)DBNull.Value,
        //                        V(S(s, "dispositionAnalyzer")),
        //                        V(S(s, "dispositionName")),
        //                        V(S(s, "peerId")),
        //                        V(S(s, "remote")),
        //                        V(S(s, "usedRouting")));

        //                    foreach (var f in A(s, "flow"))
        //                    {
        //                        flows.Rows.Add(
        //                            V(S(f, "flowId")),
        //                            V(sid),
        //                            V(S(f, "flowName")),
        //                            V(S(f, "flowType")),
        //                            V(S(f, "transferTargetName")),
        //                            V(S(f, "transferType")));
        //                    }

        //                    foreach (var m in A(s, "metrics"))
        //                    {
        //                        int value = 0;

        //                        if (m.TryGetProperty("value", out var v))
        //                        {
        //                            if (v.ValueKind == JsonValueKind.Number)
        //                                v.TryGetInt32(out value);
        //                        }

        //                        metrics.Rows.Add(
        //                            V(sid),
        //                            D(m, "emitDate") ?? (object)DBNull.Value,
        //                            V(S(m, "name")),
        //                            value);
        //                    }

        //                    foreach (var g in A(s, "segments"))
        //                    {
        //                        segments.Rows.Add(
        //                            V(sid),
        //                            D(g, "segmentStart") ?? (object)DBNull.Value,
        //                            D(g, "segmentEnd") ?? (object)DBNull.Value,
        //                            V(S(g, "segmentType")),
        //                            V(S(g, "queueId")),
        //                            V(S(g, "disconnectType")));
        //                    }
        //                }
        //            }
        //        }

        //        await BulkInsert(conn, conversations, "Conversations_Old");
        //        await BulkInsert(conn, participants, "Participants_Old");
        //        await BulkInsert(conn, sessions, "Sessions_Old");
        //        await BulkInsert(conn, flows, "Flows_Old");
        //        await BulkInsert(conn, metrics, "Metrics_Old");
        //        await BulkInsert(conn, segments, "Segments_Old");

        //        await SqlQueryLogsAsync(
        //            nameof(SaveToDatabaseConversationsv1),
        //            $"SUCCESS | C={conversations.Rows.Count} " +
        //            $"P={participants.Rows.Count} " +
        //            $"S={sessions.Rows.Count} " +
        //            $"F={flows.Rows.Count} " +
        //            $"M={metrics.Rows.Count} " +
        //            $"G={segments.Rows.Count} | " +
        //            $"{sw.Elapsed.TotalSeconds:F1}s");
        //    }
        //    catch (Exception ex)
        //    {
        //        await SqlQueryLogsAsync(
        //            nameof(SaveToDatabaseConversationsv1),
        //            $"ERROR | {ex.GetType().Name} | {ex.Message}");

        //        throw;
        //    }
        //}

        private async Task SaveToDatabaseConversationsv2(string json)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                using var doc = JsonDocument.Parse(json);
                using var conn = new SqlConnection(_connectionString);

                await conn.OpenAsync();

                // =========================================================
                // TABLES
                // =========================================================

                var conversations = CreateTable(
                    ("ConversationId", typeof(string)),
                    ("ConversationStart", typeof(DateTime)),
                    ("ConversationEnd", typeof(DateTime)),
                    ("ConversationOriginatingDirection", typeof(string)));

                var participants = CreateTable(
                    ("ParticipantId", typeof(string)),
                    ("ConversationId", typeof(string)),
                    ("ParticipantName", typeof(string)),
                    ("ParticipantPurpose", typeof(string)),
                    ("ParticipantUserId", typeof(string)));

                var sessions = CreateTable(
                    ("SessionId", typeof(string)),
                    ("ParticipantId", typeof(string)),
                    ("SessionANI", typeof(string)),
                    ("SessionDNIS", typeof(string)),
                    ("SessionDirection", typeof(string)),
                    ("SessionMediaType", typeof(string)),
                    ("SessionAgentId", typeof(string)),
                    ("SessionsEdgeId", typeof(string)),
                    ("SessionsProtocolCallId", typeof(string)),
                    ("SessionsProvider", typeof(string)),
                    ("SessionsRemoteNameDisplayable", typeof(string)),
                    ("SessionsDNIS2", typeof(string)),
                    ("SessionsCallbackUserName", typeof(string)),
                    ("SessionsOutboundCampaignId", typeof(string)),
                    ("SessionsOutboundContactId", typeof(string)),
                    ("SessionsOutboundContactListId", typeof(string)),
                    ("SessionsScriptId", typeof(string)),
                    ("SessionsDetectedSpeechEnd", typeof(DateTime)),
                    ("SessionsDetectedSpeechStart", typeof(DateTime)),
                    ("SessionsDispositionAnalyzer", typeof(string)),
                    ("SessionsDispositionName", typeof(string)),
                    ("SessionsPeerId", typeof(string)),
                    ("SessionsRemote", typeof(string)),
                    ("SessionsUsedRouting", typeof(string)));

                var flows = CreateTable(
                    ("FlowId", typeof(string)),
                    ("SessionId", typeof(string)),
                    ("FlowName", typeof(string)),
                    ("FlowType", typeof(string)),
                    ("FlowTransferTargetName", typeof(string)),
                    ("FlowTransferType", typeof(string)));

                var metrics = CreateTable(
                    ("SessionId", typeof(string)),
                    ("MetricEmitDate", typeof(DateTime)),
                    ("MetricName", typeof(string)),
                    ("MetricValue", typeof(int)));

                var segments = CreateTable(
                    ("SessionId", typeof(string)),
                    ("SegmentStart", typeof(DateTime)),
                    ("SegmentEnd", typeof(DateTime)),
                    ("SegmentType", typeof(string)),
                    ("SegmentQueueId", typeof(string)),
                    ("DisconnectType", typeof(string)));


                // =========================================================
                // PARSE JSON
                // =========================================================

                if (!doc.RootElement.TryGetProperty("conversations", out var conversationArray) ||
                    conversationArray.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "JSON does not contain a valid 'conversations' array.");
                }

                foreach (var conversation in conversationArray.EnumerateArray())
                {
                    string conversationId = S(conversation, "conversationId");

                    conversations.Rows.Add(
                        Db(conversationId),
                        Db(D(conversation, "conversationStart")),
                        Db(D(conversation, "conversationEnd")),
                        Db(S(conversation, "originatingDirection"))
                    );


                    // =====================================================
                    // PARTICIPANTS
                    // =====================================================

                    foreach (var participant in A(conversation, "participants"))
                    {
                        string participantId = S(participant, "participantId");

                        participants.Rows.Add(
                            Db(participantId),
                            Db(conversationId),
                            Db(S(participant, "participantName")),
                            Db(S(participant, "purpose")),
                            Db(S(participant, "userId"))
                        );


                        // =================================================
                        // SESSIONS
                        // =================================================

                        foreach (var session in A(participant, "sessions"))
                        {
                            string sessionId = S(session, "sessionId");

                            sessions.Rows.Add(
                                Db(sessionId),
                                Db(participantId),
                                Db(S(session, "ani")),
                                Db(S(session, "dnis")),
                                Db(S(session, "direction")),
                                Db(S(session, "mediaType")),
                                Db(S(session, "selectedAgentId")),
                                Db(S(session, "edgeId")),
                                Db(S(session, "protocolCallId")),
                                Db(S(session, "provider")),
                                Db(S(session, "remoteNameDisplayable")),
                                Db(S(session, "sessionDnis")),
                                Db(S(session, "callbackUserName")),
                                Db(S(session, "outboundCampaignId")),
                                Db(S(session, "outboundContactId")),
                                Db(S(session, "outboundContactListId")),
                                Db(S(session, "scriptId")),
                                Db(D(session, "detectedSpeechEnd")),
                                Db(D(session, "detectedSpeechStart")),
                                Db(S(session, "dispositionAnalyzer")),
                                Db(S(session, "dispositionName")),
                                Db(S(session, "peerId")),
                                Db(S(session, "remote")),
                                Db(S(session, "usedRouting"))
                            );


                            // =============================================
                            // FLOWS
                            // =============================================

                            foreach (var flow in A(session, "flow"))
                            {
                                flows.Rows.Add(
                                    Db(S(flow, "flowId")),
                                    Db(sessionId),
                                    Db(S(flow, "flowName")),
                                    Db(S(flow, "flowType")),
                                    Db(S(flow, "transferTargetName")),
                                    Db(S(flow, "transferType"))
                                );
                            }


                            // =============================================
                            // METRICS
                            // =============================================

                            foreach (var metric in A(session, "metrics"))
                            {
                                metrics.Rows.Add(
                                    Db(sessionId),
                                    Db(D(metric, "emitDate")),
                                    Db(S(metric, "name")),
                                    Db(GetInt(metric, "value"))
                                );
                            }


                            // =============================================
                            // SEGMENTS
                            // =============================================

                            foreach (var segment in A(session, "segments"))
                            {
                                segments.Rows.Add(
                                    Db(sessionId),
                                    Db(D(segment, "segmentStart")),
                                    Db(D(segment, "segmentEnd")),
                                    Db(S(segment, "segmentType")),
                                    Db(S(segment, "queueId")),
                                    Db(S(segment, "disconnectType"))
                                );
                            }
                        }
                    }
                }


                // =========================================================
                // BULK INSERT
                // =========================================================

                await BulkInsert(conn, conversations, "Conversations_Old");

                await BulkInsert(conn, participants, "Participants_Old");

                await BulkInsert(conn, sessions, "Sessions_Old");

                await BulkInsert(conn, flows, "Flows_Old");

                await BulkInsert(conn, metrics, "Metrics_Old");

                await BulkInsert(conn, segments, "Segments_Old");


                // =========================================================
                // LOG
                // =========================================================

                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseConversationsv2),
                    $"SUCCESS | " +
                    $"Conversations={conversations.Rows.Count}, " +
                    $"Participants={participants.Rows.Count}, " +
                    $"Sessions={sessions.Rows.Count}, " +
                    $"Flows={flows.Rows.Count}, " +
                    $"Metrics={metrics.Rows.Count}, " +
                    $"Segments={segments.Rows.Count}, " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );
            }
            catch (Exception ex)
            {
                await SqlQueryLogsAsync(
                    nameof(SaveToDatabaseConversationsv2),
                    $"ERROR | {ex.GetType().Name} | {ex.Message} | " +
                    $"Time={sw.Elapsed.TotalSeconds:F1}s"
                );

                throw;
            }
        }


        // String helper
        private string S(JsonElement element, string property)
        {
            if (!element.TryGetProperty(property, out var value))
                return null;

            if (value.ValueKind == JsonValueKind.Null)
                return null;

            return value.ValueKind == JsonValueKind.String
                ? value.GetString()
                : value.ToString();
        }


        // DateTime helper
        private DateTime? D(JsonElement element, string property)
        {
            if (!element.TryGetProperty(property, out var value))
                return null;

            if (value.ValueKind != JsonValueKind.String)
                return null;

            if (DateTime.TryParse(value.GetString(), out var date))
                return date;

            return null;
        }


        // Array helper
        private IEnumerable<JsonElement> A(JsonElement element, string property)
        {
            if (!element.TryGetProperty(property, out var value))
                return Enumerable.Empty<JsonElement>();

            if (value.ValueKind == JsonValueKind.Array)
                return value.EnumerateArray();

            // Handles Genesys responses where an object appears instead of an array
            if (value.ValueKind == JsonValueKind.Object)
                return new[] { value };

            return Enumerable.Empty<JsonElement>();
        }


        // Bulk insert helper
        private async Task BulkInsert(
            SqlConnection connection,
            DataTable table,
            string destination)
        {
            if (table.Rows.Count == 0)
                return;

            using var bulk = new SqlBulkCopy(connection)
            {
                DestinationTableName = destination,
                BatchSize = 500,
                BulkCopyTimeout = 300
            };

            foreach (DataColumn column in table.Columns)
            {
                bulk.ColumnMappings.Add(
                    column.ColumnName,
                    column.ColumnName);
            }

            await bulk.WriteToServerAsync(table);
        }

        #endregion

    }
}
