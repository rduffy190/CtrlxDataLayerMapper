/*
 * SPDX-License-Identifier: MIT
 */

using Microsoft.IdentityModel.JsonWebTokens;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Samples.Datalayer.Mapper
{
    /// <summary>Request body sent by the Solutions app for every save/load phase.</summary>
    internal sealed record AppDataHttpRequest(string ConfigurationPath, string Id, string Phase);

    [JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
    [JsonSerializable(typeof(AppDataHttpRequest))]
    internal partial class AppDataHttpRequestSerializerContext : JsonSerializerContext { }

    /// <summary>
    /// Registers the app in the ctrlX save/load workflow (app.solutions).
    ///
    /// A light-weight HttpListener serves the two commands declared in
    /// *.package-manifest.json. On "load" the mapping configuration is re-read from
    /// the active configuration and re-applied without restarting the app.
    /// </summary>
    internal sealed class AppDataService : IDisposable
    {
        private const int HttpPort = 5556;
        private const string AppId = "sdk-net-datalayer-mapper";

        private static readonly string HttpApiRouteLoad = $"http://localhost:{HttpPort}/{AppId}/api/v1/load";
        private static readonly string HttpApiRouteSave = $"http://localhost:{HttpPort}/{AppId}/api/v1/save";

        private readonly Func<bool> _onLoad;
        private readonly Func<bool> _onSave;
        private HttpListener? _httpListener;

        /// <param name="onLoad">Re-reads the configuration and re-applies it. Returns success.</param>
        /// <param name="onSave">Persists the current configuration into the appdata directory.</param>
        public AppDataService(Func<bool> onLoad, Func<bool> onSave)
        {
            _onLoad = onLoad ?? throw new ArgumentNullException(nameof(onLoad));
            _onSave = onSave ?? throw new ArgumentNullException(nameof(onSave));
        }

        public bool Start()
        {
            try
            {
                if (!HttpListener.IsSupported)
                {
                    Console.WriteLine("HTTP listening not supported!");
                    return false;
                }

                _httpListener = new HttpListener();
                _httpListener.Prefixes.Add(HttpApiRouteLoad + "/");
                _httpListener.Prefixes.Add(HttpApiRouteSave + "/");
                _httpListener.Start();

                if (!_httpListener.IsListening)
                {
                    Console.WriteLine("Listening to HTTP failed!");
                    return false;
                }

                Console.WriteLine($"Listening to HTTP: {string.Join(", ", _httpListener.Prefixes)}");

                Task.Factory.StartNew(Listen, TaskCreationOptions.LongRunning);
                return true;
            }
            catch (HttpListenerException exc)
            {
                Console.WriteLine($"Listening to HTTP failed! {exc.Message}");
                return false;
            }
        }

        private void Listen()
        {
            var listener = _httpListener;

            while (listener is { IsListening: true })
            {
                HttpListenerContext context;

                try
                {
                    // Blocks until the next request arrives.
                    context = listener.GetContext();
                }
                catch (Exception exc) when (exc is HttpListenerException || exc is ObjectDisposedException)
                {
                    // Listener stopped.
                    return;
                }

                var request = context.Request;
                var response = context.Response;

                try
                {
                    Console.WriteLine($"Request: {request.Url}");

                    var jwt = GetToken(request);
                    if (jwt is null || !IsAuthorized(jwt))
                    {
                        response.StatusCode = (int)HttpStatusCode.Unauthorized;
                        continue;
                    }

                    using var reader = new StreamReader(request.InputStream, request.ContentEncoding);
                    var appDataHttpRequest = JsonSerializer.Deserialize(
                        reader.ReadToEnd(),
                        AppDataHttpRequestSerializerContext.Default.AppDataHttpRequest);

                    var phase = appDataHttpRequest?.Phase ?? string.Empty;
                    var route = request.Url?.ToString() ?? string.Empty;

                    Console.WriteLine($"Payload: {appDataHttpRequest}");

                    response.StatusCode = route.Equals(HttpApiRouteLoad, StringComparison.Ordinal)
                        ? HandleLoad(phase, appDataHttpRequest?.Id)
                        : route.Equals(HttpApiRouteSave, StringComparison.Ordinal)
                            ? HandleSave(phase, appDataHttpRequest?.Id)
                            : (int)HttpStatusCode.NotFound;
                }
                catch (Exception exc)
                {
                    // We must ALWAYS answer, whatever happens.
                    Console.WriteLine($"Failed to handle app data request! {exc.Message}");
                    response.StatusCode = (int)HttpStatusCode.InternalServerError;
                }
                finally
                {
                    response.Close();
                }
            }
        }

        private int HandleLoad(string phase, string? id) => phase switch
        {
            // Provide resources according to the data of the active configuration.
            "load" => _onLoad()
                ? (int)HttpStatusCode.Accepted
                : (int)HttpStatusCode.InternalServerError,

            // query / prepare / validate / activate / abort and any future phase:
            // 204 keeps the workflow going and stays upwards compatible.
            _ => (int)HttpStatusCode.NoContent,
        };

        private int HandleSave(string phase, string? id) => phase switch
        {
            "save" => _onSave()
                ? (int)HttpStatusCode.Accepted
                : (int)HttpStatusCode.InternalServerError,

            _ => (int)HttpStatusCode.NoContent,
        };

        private static JsonWebToken? GetToken(HttpListenerRequest request)
        {
            var authorization = request.Headers["Authorization"];
            if (string.IsNullOrEmpty(authorization))
            {
                return null;
            }

            var token = authorization.Split(" ").Last();

            try
            {
                return new JsonWebToken(token);
            }
            catch (ArgumentException)
            {
                return null;
            }
        }

        private static bool IsAuthorized(JsonWebToken jwt)
        {
            try
            {
                var scopes = jwt.GetPayloadValue<string[]>("scope");
                return scopes.Any(scope => scope.Equals("rexroth-device.all.rwx", StringComparison.Ordinal));
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public void Dispose()
        {
            _httpListener?.Stop();
            _httpListener?.Close();
            _httpListener = null;
        }
    }
}
