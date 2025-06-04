/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System.Web;
using Newtonsoft.Json;
using QuantConnect.Util;
using Newtonsoft.Json.Linq;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace QuantConnect.Lean.DataSource.Polygon
{
    /// <summary>
    /// Provides a common API to download data from Polygon.io's REST API
    /// </summary>
    public class PolygonRestApiClient : IDisposable
    {
        private readonly static string RestApiBaseUrl = Config.Get("polygon-api-url", "https://api.polygon.io");

        private readonly HttpClient _httpClient;

        private readonly CancellationTokenSource _cancellationTokenSource = new();

        /// <summary>
        /// The maximum number of retry attempts for downloading data or executing a request.
        /// </summary>
        private const int MaxRetries = 10;

        private readonly string _apiKey;

        // Made virtual for testing purposes
        protected virtual string ApiResponseLimit { get; } = Config.Get("polygon-aggregate-response-limit", "5000");

        // Made virtual for testing purposes
        protected virtual RateGate RateLimiter { get; } = new(300, TimeSpan.FromSeconds(1));

        /// <summary>
        /// Initializes a new instance of the <see cref="PolygonRestApiClient"/> class
        /// </summary>
        /// <param name="apiKey">The Polygon API key</param>
        public PolygonRestApiClient(string apiKey)
        {
            _apiKey = apiKey;
            _httpClient = new HttpClient()
            {
                BaseAddress = new Uri(RestApiBaseUrl),
                Timeout = TimeSpan.FromMinutes(5) // 5 minutes
            };

            // Set default Authorization header for all API requests
            _httpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {_apiKey}");
        }

        /// <summary>
        /// Downloads data and tries to parse the JSON response data into the specified type
        /// </summary>
        public virtual IEnumerable<T> DownloadAndParseData<T>(string resource, Dictionary<string, string> parameters = null)
            where T : BaseResponse
        {
            var requestUri = BuildRequestUri(resource, parameters);

            while (requestUri != null)
            {
                Log.Debug($"PolygonRestApi.DownloadAndParseData(): Downloading {requestUri}");

                var responseContent = DownloadWithRetries(requestUri);
                if (string.IsNullOrEmpty(responseContent))
                {
                    throw new Exception($"{nameof(PolygonRestApiClient)}.{nameof(DownloadAndParseData)}: Failed to download data for {requestUri} after {MaxRetries} attempts.");
                }

                var result = ParseResponse<T>(responseContent);

                if (result == null)
                {
                    yield break;
                }

                yield return result;

                requestUri = result.NextUrl;
            }
        }

        private string BuildRequestUri(string resource, Dictionary<string, string> parameters)
        {
            if (string.IsNullOrEmpty(resource))
            {
                return null;
            }

            var uriBuilder = new UriBuilder(RestApiBaseUrl + "/" + resource.TrimStart('/'));
            var query = HttpUtility.ParseQueryString(uriBuilder.Query);

            // Add default limit if not specified
            if (parameters == null || !parameters.ContainsKey("limit"))
            {
                query["limit"] = ApiResponseLimit;
            }

            // Add custom parameters
            if (parameters != null)
            {
                foreach (var param in parameters)
                {
                    query[param.Key] = param.Value;
                }
            }

            uriBuilder.Query = query.ToString();
            return uriBuilder.ToString();
        }

        private string DownloadWithRetries(string requestUri)
        {
            HttpResponseMessage response = null;
            for (var attempt = 0; attempt < MaxRetries; attempt++)
            {
                if (RateLimiter != null)
                {
                    if (RateLimiter.IsRateLimited)
                    {
                        Log.Trace("PolygonRestApi.DownloadAndParseData(): Rest API calls are limited; waiting to proceed.");
                    }
                    RateLimiter.WaitToProceed();
                }

                try
                {
                    response = _httpClient.GetAsync(requestUri, _cancellationTokenSource.Token).Result;
                    var content = response.Content.ReadAsStringAsync().Result;

                    if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        var waitTime = TimeSpan.FromSeconds(10 * attempt);

                        var baseResponse = JsonConvert.DeserializeObject<BaseResponse>(content);
                        Log.Trace($"PolygonRestApi.DownloadAndParseData(): Attempt {attempt + 1} was throttled due to too many requests. Waiting {waitTime.TotalSeconds} seconds before retrying... (Last error: {baseResponse?.Error ?? "Unknown error"})");

                        if (_cancellationTokenSource.Token.WaitHandle.WaitOne(waitTime))
                        {
                            // shutting down
                            return null;
                        }
                        continue;
                    }

                    if (response.IsSuccessStatusCode && !string.IsNullOrEmpty(content))
                    {
                        return content;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonRestApi.DownloadWithRetries(): Attempt {attempt + 1} failed with exception: {ex.Message}");
                }
                finally
                {
                    response?.Dispose();
                }
            }

            Log.Trace($"Failed after {MaxRetries} attempts for {requestUri}. Last response: {response?.StatusCode}");
            return null;
        }

        private T? ParseResponse<T>(string responseContent) where T : BaseResponse
        {
            var result = JObject.Parse(responseContent).ToObject<T>();

            if (result == null)
            {
                throw new ArgumentException($"{nameof(PolygonRestApiClient)}.{nameof(ParseResponse)}: Unable to parse response. Response: {responseContent}");
            }

            return result;
        }

        public void Dispose()
        {
            RateLimiter?.DisposeSafely();
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.DisposeSafely();
            _httpClient?.Dispose();
        }
    }
}