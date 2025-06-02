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

using System.Net.Http.Headers;
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
        public readonly static string RestApiBaseUrl = Config.Get("polygon-api-url", "https://api.polygon.io");

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
            _httpClient = new HttpClient
            {
                BaseAddress = new Uri(RestApiBaseUrl),
                Timeout = TimeSpan.FromMinutes(5)
            };
        }

        /// <summary>
        /// Downloads data and tries to parse the JSON response data into the specified type
        /// </summary>
        public virtual async IAsyncEnumerable<T> DownloadAndParseData<T>(HttpRequestMessage? request)
            where T : BaseResponse
        {
            if (request != null && !request.RequestUri.Query.Contains("limit="))
            {
                var uriBuilder = new UriBuilder(request.RequestUri);
                var query = System.Web.HttpUtility.ParseQueryString(uriBuilder.Query);
                query["limit"] = ApiResponseLimit;
                uriBuilder.Query = query.ToString();
                request.RequestUri = uriBuilder.Uri;
            }

            while (request != null)
            {
                Log.Debug($"PolygonRestApi.DownloadAndParseData(): Downloading {request.RequestUri}");

                var responseContent = await DownloadWithRetries(request);
                if (string.IsNullOrEmpty(responseContent))
                {
                    throw new Exception($"{nameof(PolygonRestApiClient)}.{nameof(DownloadAndParseData)}: Failed to download data for {request.RequestUri} after {MaxRetries} attempts.");
                }

                var result = ParseResponse<T>(responseContent);

                if (result == null)
                {
                    yield break;
                }

                yield return result;

                request = result.NextUrl != null ? new HttpRequestMessage(HttpMethod.Get, result.NextUrl) : null;
            }
        }

        private async Task<string?> DownloadWithRetries(HttpRequestMessage request)
        {
            var response = default(HttpResponseMessage);
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

                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);

                try
                {
                    response = await _httpClient.SendAsync(request, _cancellationTokenSource.Token);

                    var content = await response.Content.ReadAsStringAsync();
                    var baseResponse = JsonConvert.DeserializeObject<BaseResponse>(content);

                    if (response.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                    {
                        var waitTime = TimeSpan.FromSeconds(10 * attempt);

                        Log.Trace($"PolygonRestApi.DownloadAndParseData(): Attempt {attempt + 1} was throttled due to too many requests. Waiting {waitTime.TotalSeconds} seconds before retrying... (Last error: {baseResponse?.Error ?? "Unknown error"})");

                        if (_cancellationTokenSource.Token.WaitHandle.WaitOne(waitTime))
                        {
                            // shutting down
                            return null;
                        }
                        continue;
                    }

                    if (response != null && response.IsSuccessStatusCode && !string.IsNullOrEmpty(content))
                    {
                        return content;
                    }
                }
                catch (Exception ex)
                {
                    Log.Error($"PolygonRestApi.DownloadWithRetries(): Error during request attempt {attempt + 1} - {ex.Message}");
                }
            }

            var responseContent = response != null ? await response.Content.ReadAsStringAsync() : string.Empty;
            Log.Trace($"Failed after {MaxRetries} attempts for {request.RequestUri}. Content: {responseContent}");
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