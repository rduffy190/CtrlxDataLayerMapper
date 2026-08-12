/*
 * SPDX-License-Identifier: MIT
 */

using Microsoft.IdentityModel.JsonWebTokens;
using System;
using System.IO;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Samples.Datalayer.Mapper
{
    /// <summary>
    /// What the app data service calls when the Solutions app asks it to load or
    /// save. An interface rather than callbacks, so the implementation is a named
    /// class with visible state.
    /// </summary>
    internal interface IAppDataHandler
    {
        bool Load();

        bool Save();
    }

    /// <summary>Request body sent by the Solutions app for every save/load phase.</summary>
    internal sealed class AppDataHttpRequest
    {
        public string ConfigurationPath { get; set; } = string.Empty;

        public string Id { get; set; } = string.Empty;

        public string Phase { get; set; } = string.Empty;

        public override string ToString()
        {
            return "id=" + Id + ", phase=" + Phase + ", path=" + ConfigurationPath;
        }
    }

    [JsonSourceGenerationOptions(PropertyNameCaseInsensitive = true)]
    [JsonSerializable(typeof(AppDataHttpRequest))]
    internal partial class AppDataHttpRequestSerializerContext : JsonSerializerContext
    {
    }

    /// <summary>
    /// Registers the app in the ctrlX save/load workflow (app.solutions).
    ///
    /// A light-weight HttpListener serves the two commands declared in
    /// *.package-manifest.json.
    /// </summary>
    internal sealed class AppDataService : IDisposable
    {
        private const int HttpPort = 5556;
        private const string AppId = "sdk-net-datalayer-mapper";
        private const string RequiredScope = "rexroth-device.all.rwx";

        private static readonly string HttpApiRouteLoad =
            "http://localhost:" + HttpPort + "/" + AppId + "/api/v1/load";

        private static readonly string HttpApiRouteSave =
            "http://localhost:" + HttpPort + "/" + AppId + "/api/v1/save";

        private readonly IAppDataHandler _handler;
        private HttpListener? _httpListener;

        public AppDataService(IAppDataHandler handler)
        {
            if (handler == null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            _handler = handler;
        }

        public bool Start()
        {
            if (!HttpListener.IsSupported)
            {
                Console.WriteLine("HTTP listening not supported!");
                return false;
            }

            try
            {
                HttpListener listener = new HttpListener();
                listener.Prefixes.Add(HttpApiRouteLoad + "/");
                listener.Prefixes.Add(HttpApiRouteSave + "/");
                listener.Start();

                if (!listener.IsListening)
                {
                    Console.WriteLine("Listening to HTTP failed!");
                    return false;
                }

                _httpListener = listener;

                Console.WriteLine("Listening to HTTP: " + HttpApiRouteLoad + ", " + HttpApiRouteSave);

                Task.Factory.StartNew(Listen, TaskCreationOptions.LongRunning);
                return true;
            }
            catch (HttpListenerException exc)
            {
                Console.WriteLine("Listening to HTTP failed! " + exc.Message);
                return false;
            }
        }

        private void Listen()
        {
            HttpListener? listener = _httpListener;

            while (listener != null && listener.IsListening)
            {
                HttpListenerContext context;

                try
                {
                    // Blocks until the next request arrives.
                    context = listener.GetContext();
                }
                catch (HttpListenerException)
                {
                    return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                HandleRequest(context);
            }
        }

        private void HandleRequest(HttpListenerContext context)
        {
            HttpListenerRequest request = context.Request;
            HttpListenerResponse response = context.Response;

            try
            {
                Console.WriteLine("Request: " + request.Url);

                JsonWebToken? jwt = GetToken(request);

                if (jwt == null || !IsAuthorized(jwt))
                {
                    response.StatusCode = (int)HttpStatusCode.Unauthorized;
                    return;
                }

                AppDataHttpRequest? payload = ReadPayload(request);
                string phase = string.Empty;

                if (payload != null)
                {
                    phase = payload.Phase;
                    Console.WriteLine("Payload: " + payload);
                }

                string route = string.Empty;

                if (request.Url != null)
                {
                    route = request.Url.ToString();
                }

                if (string.Equals(route, HttpApiRouteLoad, StringComparison.Ordinal))
                {
                    response.StatusCode = HandleLoad(phase);
                }
                else if (string.Equals(route, HttpApiRouteSave, StringComparison.Ordinal))
                {
                    response.StatusCode = HandleSave(phase);
                }
                else
                {
                    response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            catch (Exception exc)
            {
                // We must ALWAYS answer, whatever happens.
                Console.WriteLine("Failed to handle app data request! " + exc.Message);
                response.StatusCode = (int)HttpStatusCode.InternalServerError;
            }
            finally
            {
                response.Close();
            }
        }

        private static AppDataHttpRequest? ReadPayload(HttpListenerRequest request)
        {
            using StreamReader reader = new StreamReader(request.InputStream, request.ContentEncoding);

            string body = reader.ReadToEnd();

            return JsonSerializer.Deserialize(
                body,
                AppDataHttpRequestSerializerContext.Default.AppDataHttpRequest);
        }

        private int HandleLoad(string phase)
        {
            // Provide resources according to the data of the active configuration.
            if (string.Equals(phase, "load", StringComparison.Ordinal))
            {
                if (_handler.Load())
                {
                    return (int)HttpStatusCode.Accepted;
                }

                return (int)HttpStatusCode.InternalServerError;
            }

            // query / prepare / validate / activate / abort and any future phase:
            // 204 keeps the workflow going and stays upwards compatible.
            return (int)HttpStatusCode.NoContent;
        }

        private int HandleSave(string phase)
        {
            if (string.Equals(phase, "save", StringComparison.Ordinal))
            {
                if (_handler.Save())
                {
                    return (int)HttpStatusCode.Accepted;
                }

                return (int)HttpStatusCode.InternalServerError;
            }

            return (int)HttpStatusCode.NoContent;
        }

        private static JsonWebToken? GetToken(HttpListenerRequest request)
        {
            string? authorization = request.Headers["Authorization"];

            if (string.IsNullOrEmpty(authorization))
            {
                return null;
            }

            string[] parts = authorization.Split(' ');
            string token = parts[parts.Length - 1];

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
                string[] scopes = jwt.GetPayloadValue<string[]>("scope");

                for (int i = 0; i < scopes.Length; i++)
                {
                    if (string.Equals(scopes[i], RequiredScope, StringComparison.Ordinal))
                    {
                        return true;
                    }
                }

                return false;
            }
            catch (ArgumentException)
            {
                return false;
            }
        }

        public void Dispose()
        {
            HttpListener? listener = _httpListener;

            if (listener == null)
            {
                return;
            }

            _httpListener = null;
            listener.Stop();
            listener.Close();
        }
    }
}
